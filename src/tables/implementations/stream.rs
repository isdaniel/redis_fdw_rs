use std::borrow::Cow;

use crate::{
    query::{
        limit::LimitOffsetInfo,
        pushdown_types::{ComparisonOperator, PushableCondition},
        scan_ops::extract_scan_conditions,
    },
    tables::{
        interface::RedisTableOperations,
        types::{DataSet, LoadDataResult, RowVec},
    },
};

/// Redis Stream table type supporting large data sets with streaming and pagination
///
/// Redis Streams are append-only log data structures that support:
/// - Time-ordered entry IDs (timestamp-sequence format)
/// - Field-value pairs per entry (like hash fields)
/// - Range queries by time or ID
/// - Consumer groups for distributed processing
/// - Efficient pagination with COUNT and ID cursors
#[derive(Debug, Clone, Default)]
pub struct RedisStreamTable {
    pub dataset: DataSet,
    /// Pre-split stream entries for zero-allocation row access
    pub entries: Vec<Vec<String>>,
    /// Last processed stream ID for pagination
    pub last_id: Option<String>,
    /// Batch size for streaming operations to handle large data sets
    pub batch_size: usize,
    /// Column names from the foreign table definition (for mapping fields to positions)
    pub column_names: Vec<String>,
    /// Raw attribute index of the stream ID column (accounts for TTL position)
    pub pushdown_column_index: usize,
}

impl RedisStreamTable {
    pub fn new(batch_size: usize) -> Self {
        Self {
            dataset: DataSet::Empty,
            entries: Vec::new(),
            last_id: None,
            batch_size,
            column_names: Vec::new(),
            pushdown_column_index: 0,
        }
    }

    fn next_start_id(&self) -> String {
        match &self.last_id {
            Some(id) => {
                let parts: Vec<&str> = id.splitn(2, '-').collect();
                if parts.len() == 2 {
                    if let Ok(seq) = parts[1].parse::<u64>() {
                        format!("{}-{}", parts[0], seq.saturating_add(1))
                    } else {
                        id.clone()
                    }
                } else {
                    id.clone()
                }
            }
            None => "-".to_string(),
        }
    }

    fn load_with_xrange(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        start_id: &str,
        end_id: &str,
        count: Option<usize>,
    ) -> Result<LoadDataResult, redis::RedisError> {
        // Use XRANGE to get stream entries
        let entries: Vec<(String, Vec<(String, String)>)> = match count {
            Some(c) => redis::cmd("XRANGE")
                .arg(key_prefix)
                .arg(start_id)
                .arg(end_id)
                .arg("COUNT")
                .arg(c)
                .query(conn)?,
            None => redis::cmd("XRANGE")
                .arg(key_prefix)
                .arg(start_id)
                .arg(end_id)
                .query(conn)?,
        };

        if entries.is_empty() {
            return Ok(LoadDataResult::Empty);
        }

        // Store last stream ID for pagination before processing entries
        let last_id = entries.last().map(|(id, _)| id.clone());

        // Store as pre-split structured entries for zero-allocation row access
        let mut structured_entries = Vec::with_capacity(entries.len());
        let mut flat_data = Vec::with_capacity(entries.len());
        for (stream_id, fields) in entries {
            let mut row = vec![stream_id.clone()];
            for (field, value) in fields {
                row.push(field);
                row.push(value);
            }
            flat_data.push(stream_id);
            structured_entries.push(row);
        }

        // Store last stream ID for pagination
        if let Some(id) = last_id {
            self.last_id = Some(id);
        }

        self.entries = structured_entries;
        // Store flat_data for DataSet compatibility
        self.dataset = DataSet::Filtered(flat_data);
        Ok(LoadDataResult::FullyLoaded)
    }

    fn load_with_stream_optimization(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        scan_conditions: &crate::query::scan_ops::ScanConditions,
        limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        let mut start_id = "-".to_string();
        let mut end_id = "+".to_string();
        let mut start_set = false;
        let mut end_set = false;
        // Start with no COUNT cap. When a stream_id range is pushed down (start_set
        // / end_set) the FDW calls this path once and marks scan_complete = true,
        // so an over-eager batch_size limit would silently truncate any range that
        // contains more than `batch_size` rows. We add the batch_size cap below
        // ONLY for the full-scan + client-side-filter fallback (no id bounds).
        let mut count: Option<usize> = None;
        let mut non_id_conditions: Vec<&PushableCondition> = Vec::new();
        let id_col_idx = self.pushdown_column_index;

        // Simplification vs. the plan: we do NOT attempt to "tighten" duplicate
        // bounds via string comparison — `format!("({}", v)` sorts before digits
        // lexicographically, so the plan's `bound > start_id` check is buggy.
        // PostgreSQL normally pushes at most one condition per operator class for
        // a single column, so "last writer wins" is safe in practice. If a future
        // planner shape sends multiple, we just take the latest — still correct,
        // just not maximally narrow.
        for condition in &scan_conditions.exact_conditions {
            if condition.column_index != id_col_idx {
                non_id_conditions.push(condition);
                continue;
            }
            // extract_scan_conditions only places Equal into exact_conditions.
            if condition.operator == ComparisonOperator::Equal {
                if let Some(v) = parse_stream_id_bound(&condition.value) {
                    start_id = v.clone();
                    end_id = v;
                    start_set = true;
                    end_set = true;
                    count = Some(1);
                } else {
                    // Malformed id: route to client-side filter so we still
                    // honour the WHERE. Without this, the FDW falls back to
                    // a full scan AND drops the predicate — returning every
                    // stream row to PostgreSQL. (No pgrx::warning! here:
                    // adding the macro to this code path triggers a rust-lld
                    // undefined-symbol error against pgrx-pg-sys under
                    // pgrx 0.18.1 + rustc 1.95.)
                    non_id_conditions.push(condition);
                }
            } else {
                non_id_conditions.push(condition);
            }
        }

        for condition in &scan_conditions.pattern_conditions {
            // LIKE on stream_id has no Redis pushdown — must run as client-side
            // post-filter. (Other column LIKEs route through non_id_conditions
            // for the field-value chunk filter.)
            non_id_conditions.push(condition);
        }

        // Unoptimizable operators (NotEqual, NotIn, In). Without surfacing them
        // here they'd be silently dropped — should_use_direct_load returns true
        // for any pushable condition, marks scan_complete=true after one call,
        // and PostgreSQL's recheck wouldn't see these because we claim no
        // lossy filtering. Apply them client-side via non_id_conditions.
        // (Note: id-column ones must still come through; the client-side
        // filter at the bottom checks column_index == id_col_idx and compares
        // against entry[0] instead of the field-value chunks.)
        for condition in &scan_conditions.other_conditions {
            non_id_conditions.push(condition);
        }

        // Range conditions (>=, >, <=, <) on the stream_id column → bounded XRANGE
        for condition in &scan_conditions.range_conditions {
            if condition.column_index != id_col_idx {
                non_id_conditions.push(condition);
                continue;
            }
            let mut bound_set = false;
            match condition.operator {
                ComparisonOperator::GreaterThanOrEqual => {
                    if let Some(v) = parse_stream_id_bound(&condition.value) {
                        start_id = v;
                        start_set = true;
                        bound_set = true;
                    }
                }
                ComparisonOperator::GreaterThan => {
                    if let Some(v) = parse_stream_id_bound(&condition.value) {
                        start_id = format!("({}", v);
                        start_set = true;
                        bound_set = true;
                    }
                }
                ComparisonOperator::LessThanOrEqual => {
                    if let Some(v) = parse_stream_id_bound(&condition.value) {
                        end_id = v;
                        end_set = true;
                        bound_set = true;
                    }
                }
                ComparisonOperator::LessThan => {
                    if let Some(v) = parse_stream_id_bound(&condition.value) {
                        end_id = format!("({}", v);
                        end_set = true;
                        bound_set = true;
                    }
                }
                _ => {}
            }
            if !bound_set {
                // Malformed id range — push to client-side filter so we
                // still apply the WHERE on the full scan result.
                non_id_conditions.push(condition);
            }
        }

        // If we have no stream_id bounds at all, cap the scan at batch_size
        // (covers both no-condition full scans and full-scan-with-field-filter
        // paths). When bounds ARE set, leave `count` as None so the entire
        // bounded range is fetched — `should_use_direct_load` sets
        // scan_complete=true after this call, so any cap would truncate.
        //
        // Known limitation (tracked separately): for an unbounded-LIMIT range
        // query on a million-row stream, the single XRANGE call still returns
        // the whole range. A cursor-paginated load_batch path would mitigate
        // OOM for that case; deliberately out of scope here because it
        // requires removing this branch from should_use_direct_load.
        if !start_set && !end_set {
            count = Some(self.batch_size);
        } else if let Some(limit) = limit_offset.limit {
            if non_id_conditions.is_empty() {
                // Bounded range + LIMIT and no client-side filtering: ask
                // Redis for only `offset + limit` rows from the range. With a
                // client-side filter we can't safely cap here because PG-side
                // LIMIT will discard rows we still need for filtering.
                // OFFSET-without-LIMIT intentionally leaves count=None — we
                // can't synthesize a batch_size cap there or we'd truncate.
                let offset = limit_offset.offset.unwrap_or(0);
                count = Some(offset.saturating_add(limit));
            }
        }

        let result = self.load_with_xrange(conn, key_prefix, &start_id, &end_id, count)?;

        // Apply client-side filtering for non-ID column conditions
        if !non_id_conditions.is_empty() && !self.entries.is_empty() {
            let like_matchers: Vec<Option<crate::query::scan_ops::PatternMatcher>> =
                non_id_conditions
                    .iter()
                    .map(|c| {
                        if c.operator == ComparisonOperator::Like {
                            Some(crate::query::scan_ops::PatternMatcher::from_like_pattern(
                                &c.value,
                            ))
                        } else {
                            None
                        }
                    })
                    .collect();

            let mut filtered_entries = Vec::with_capacity(self.entries.len());
            let mut filtered_flat = Vec::with_capacity(self.entries.len());

            for entry in &self.entries {
                let matches = non_id_conditions.iter().enumerate().all(|(i, cond)| {
                    let matcher = like_matchers[i].as_ref();
                    // id-column conditions compare against entry[0] (the
                    // stream_id), not the field-value chunks.
                    if cond.column_index == id_col_idx {
                        return eval_condition(&entry[0], cond, matcher);
                    }
                    entry[1..].chunks(2).any(|chunk| {
                        if chunk.len() != 2 || chunk[0] != cond.column_name {
                            return false;
                        }
                        eval_condition(&chunk[1], cond, matcher)
                    })
                });

                if matches {
                    filtered_flat.push(entry[0].clone());
                    filtered_entries.push(entry.clone());
                }
            }

            self.entries = filtered_entries;
            if self.entries.is_empty() {
                self.dataset = DataSet::Empty;
                return Ok(LoadDataResult::Empty);
            } else {
                self.dataset = DataSet::Filtered(filtered_flat);
            }
        }

        // Apply LIMIT/OFFSET after filtering, mirroring load_data's
        // no-condition fallback. Without this, PG receives all bounded-range
        // rows and discards client-side — wasteful on large ranges.
        if limit_offset.has_constraints() {
            if let DataSet::Filtered(data) = std::mem::take(&mut self.dataset) {
                self.dataset = DataSet::Filtered(limit_offset.apply_to_vec(data));
            }
            let paginated_entries = limit_offset.apply_to_vec(std::mem::take(&mut self.entries));
            self.entries = paginated_entries;
        }

        Ok(result)
    }

    /// Add a new entry to the stream
    pub fn add_entry(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        id: &str, // Use "*" for auto-generated ID
        fields: &[(String, String)],
    ) -> Result<String, redis::RedisError> {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(key_prefix).arg(id);

        // Add field-value pairs
        for (field, value) in fields {
            cmd.arg(field).arg(value);
        }

        let stream_id: String = cmd.query(conn)?;
        Ok(stream_id)
    }
}

/// Parse a stream id WHERE bound for use as XRANGE start/end.
///
/// Returns `Some(value)` if the value is a valid Redis stream id —
/// `ms` or `ms-seq` where each part fits in u64. Returns `None` on garbage
/// or overflow so the caller can route to a client-side filter instead of
/// letting Redis abort the query with `ERR Invalid stream ID`.
fn parse_stream_id_bound(value: &str) -> Option<String> {
    let mut parts = value.splitn(2, '-');
    let _ms = parts.next()?.parse::<u64>().ok()?;
    if let Some(seq) = parts.next() {
        let _seq = seq.parse::<u64>().ok()?;
    }
    Some(value.to_string())
}

/// Evaluate one PushableCondition against a single column value. Handles every
/// operator the FDW currently declares as pushable (Equal/NotEqual/Like/In/
/// NotIn + the four range operators).
///
/// Range comparisons try, in order:
/// 1. Stream-id tuple `(ms, seq)` parse — correct for `ms-seq` values
///    (lexicographic compare would mis-order `1710000000000-10` vs `…-2`).
/// 2. f64 parse — for numeric field columns.
/// 3. Lexicographic — last-resort fallback for opaque strings.
///
/// Mirrors row_matches_condition in state_manager.rs so client-side filtering
/// on Stream is consistent with the join post-filter.
fn eval_condition(
    val: &str,
    cond: &PushableCondition,
    matcher: Option<&crate::query::scan_ops::PatternMatcher>,
) -> bool {
    fn parse_sid(s: &str) -> Option<(u64, u64)> {
        let mut parts = s.splitn(2, '-');
        let ms = parts.next()?.parse::<u64>().ok()?;
        let seq = match parts.next() {
            Some(seq_str) => seq_str.parse::<u64>().ok()?,
            None => 0,
        };
        Some((ms, seq))
    }

    match cond.operator {
        ComparisonOperator::Equal => val == cond.value,
        ComparisonOperator::NotEqual => val != cond.value,
        ComparisonOperator::Like => matcher.is_some_and(|m| m.matches(val)),
        ComparisonOperator::In => cond.value.split(',').any(|x| x == val),
        ComparisonOperator::NotIn => !cond.value.split(',').any(|x| x == val),
        ComparisonOperator::GreaterThan
        | ComparisonOperator::GreaterThanOrEqual
        | ComparisonOperator::LessThan
        | ComparisonOperator::LessThanOrEqual => {
            if let (Some(a), Some(b)) = (parse_sid(val), parse_sid(&cond.value)) {
                return match cond.operator {
                    ComparisonOperator::GreaterThan => a > b,
                    ComparisonOperator::GreaterThanOrEqual => a >= b,
                    ComparisonOperator::LessThan => a < b,
                    ComparisonOperator::LessThanOrEqual => a <= b,
                    _ => unreachable!(),
                };
            }
            let (l, r) = (val.parse::<f64>(), cond.value.parse::<f64>());
            match (l, r) {
                (Ok(a), Ok(b)) => match cond.operator {
                    ComparisonOperator::GreaterThan => a > b,
                    ComparisonOperator::GreaterThanOrEqual => a >= b,
                    ComparisonOperator::LessThan => a < b,
                    ComparisonOperator::LessThanOrEqual => a <= b,
                    _ => unreachable!(),
                },
                _ => match cond.operator {
                    ComparisonOperator::GreaterThan => val > cond.value.as_str(),
                    ComparisonOperator::GreaterThanOrEqual => val >= cond.value.as_str(),
                    ComparisonOperator::LessThan => val < cond.value.as_str(),
                    ComparisonOperator::LessThanOrEqual => val <= cond.value.as_str(),
                    _ => unreachable!(),
                },
            }
        }
    }
}

impl RedisTableOperations for RedisStreamTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
        limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        if let Some(conditions) = conditions {
            let scan_conditions = extract_scan_conditions(conditions);

            // For streams, we can optimize range queries and pattern matching
            // Route to the optimization path when there's anything to do:
            // either Redis-pushable conditions (id range / Equal / Like) OR
            // unoptimizable ones we still need to filter client-side. Without
            // the second clause, WHERE field IN (...) on a non-id column
            // would trigger should_use_direct_load but then skip the filter,
            // returning unfiltered rows.
            if scan_conditions.has_optimizable_conditions()
                || !scan_conditions.other_conditions.is_empty()
            {
                return self.load_with_stream_optimization(
                    conn,
                    key_prefix,
                    &scan_conditions,
                    limit_offset,
                );
            }
        }

        // Fallback: Load recent entries with LIMIT/OFFSET optimization
        let effective_count = if limit_offset.has_constraints() {
            let offset = limit_offset.offset.unwrap_or(0);
            let limit = limit_offset.limit.unwrap_or(self.batch_size);
            Some(offset + limit) // Load enough to apply offset and limit
        } else {
            Some(self.batch_size)
        };

        match self.load_with_xrange(conn, key_prefix, "-", "+", effective_count) {
            Ok(result) => {
                // Apply LIMIT/OFFSET to loaded stream data if constraints are present
                if limit_offset.has_constraints() {
                    if let DataSet::Filtered(data) = std::mem::take(&mut self.dataset) {
                        self.dataset = DataSet::Filtered(limit_offset.apply_to_vec(data));
                    }
                    // Also paginate structured entries to keep them in sync
                    let paginated_entries =
                        limit_offset.apply_to_vec(std::mem::take(&mut self.entries));
                    self.entries = paginated_entries;
                }
                Ok(result)
            }
            Err(e) => Err(e),
        }
    }

    fn get_dataset(&self) -> &DataSet {
        &self.dataset
    }

    fn data_len(&self) -> usize {
        if !self.entries.is_empty() {
            self.entries.len()
        } else {
            self.dataset.len()
        }
    }

    #[inline]
    fn get_row(&self, index: usize) -> Option<RowVec<'_>> {
        if !self.entries.is_empty() {
            self.entries.get(index).map(|row| {
                // row = [stream_id, field1, val1, field2, val2, ...]
                if self.column_names.len() > 1 {
                    let mut result = RowVec::with_capacity(self.column_names.len());
                    result.push(Cow::Borrowed(row[0].as_str()));

                    for col_name in &self.column_names[1..] {
                        let val = row[1..]
                            .chunks(2)
                            .find(|chunk| chunk.len() == 2 && chunk[0] == *col_name)
                            .map(|chunk| Cow::Borrowed(chunk[1].as_str()))
                            .unwrap_or(Cow::Borrowed("NULL"));
                        result.push(val);
                    }
                    result
                } else {
                    row.iter().map(|s| Cow::Borrowed(s.as_str())).collect()
                }
            })
        } else {
            match &self.dataset {
                DataSet::Filtered(entries) => entries.get(index).map(|entry| {
                    entry
                        .split('\t')
                        .map(|s| Cow::Owned(s.to_string()))
                        .collect()
                }),
                DataSet::Complete(container) => container.get_row(index),
                DataSet::Empty => None,
            }
        }
    }

    fn insert(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        if data.is_empty() {
            return Ok(());
        }

        // For streams, we expect data format: [id?, field1, value1, field2, value2, ...]
        // If first element looks like a stream ID, use it; otherwise auto-generate
        let (id, field_start) = if !data.is_empty() && (data[0] == "*" || data[0].contains('-')) {
            (data[0].as_str(), 1)
        } else {
            ("*", 0) // Auto-generate ID
        };

        // Convert remaining data to field-value pairs
        let mut fields = Vec::new();
        for chunk in data[field_start..].chunks(2) {
            if chunk.len() == 2 {
                fields.push((chunk[0].clone(), chunk[1].clone()));
            }
        }

        if !fields.is_empty() {
            self.add_entry(conn, key_prefix, id, &fields)?;
        }

        Ok(())
    }

    fn delete(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        if data.is_empty() {
            return Ok(());
        }

        // For streams, data should contain stream IDs to delete
        let ids: Vec<&String> = data.iter().collect();
        let _deleted_count: usize = redis::cmd("XDEL").arg(key_prefix).arg(&ids).query(conn)?;

        Ok(())
    }

    fn update(
        &mut self,
        _conn: &mut dyn redis::ConnectionLike,
        _key_prefix: &str,
        _old_data: &[String],
        _new_data: &[String],
    ) -> Result<(), redis::RedisError> {
        Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "UPDATE is not supported for Redis Stream (append-only data structure)",
        )))
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        // Stream supports id equality, id range (translated to XRANGE start end),
        // and post-fetch field filtering. In/NotIn are also surfaced so
        // load_with_stream_optimization's client-side filter sees them; without
        // declaring them here PostgreSQL would not push them down at all.
        matches!(
            operator,
            ComparisonOperator::Equal
                | ComparisonOperator::NotEqual
                | ComparisonOperator::Like
                | ComparisonOperator::In
                | ComparisonOperator::NotIn
                | ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual
        )
    }

    fn load_batch(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        _cursor: u64,
        batch_size: usize,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<(u64, usize), redis::RedisError> {
        // Determine start/end IDs from ID-column conditions only
        let id_col_idx = self.pushdown_column_index;
        let (start_id, end_id) = if let Some(conds) = conditions {
            let mut start = None;
            let mut end = None;
            for c in conds {
                if c.operator == ComparisonOperator::Equal && c.column_index == id_col_idx {
                    start = Some(c.value.clone());
                    end = Some(c.value.clone());
                    break;
                }
            }
            (
                start.unwrap_or_else(|| self.next_start_id()),
                end.unwrap_or_else(|| "+".to_string()),
            )
        } else {
            (self.next_start_id(), "+".to_string())
        };

        let entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
            .arg(key_prefix)
            .arg(&start_id)
            .arg(&end_id)
            .arg("COUNT")
            .arg(batch_size)
            .query(conn)?;

        let row_count = entries.len();
        let new_cursor = if row_count < batch_size { 0 } else { 1 };

        if entries.is_empty() {
            self.dataset = DataSet::Empty;
            self.entries = Vec::new();
            return Ok((0, 0));
        }

        self.last_id = entries.last().map(|(id, _)| id.clone());

        // Collect non-ID conditions for client-side filtering
        let non_id_conds: Vec<&PushableCondition> = conditions
            .map(|conds| {
                conds
                    .iter()
                    .filter(|c| c.column_index != id_col_idx)
                    .collect()
            })
            .unwrap_or_default();

        // Pre-calculate PatternMatchers for LIKE conditions (O(1) lookup by index)
        let like_matchers: Vec<Option<crate::query::scan_ops::PatternMatcher>> = non_id_conds
            .iter()
            .map(|c| {
                if c.operator == ComparisonOperator::Like {
                    Some(crate::query::scan_ops::PatternMatcher::from_like_pattern(
                        &c.value,
                    ))
                } else {
                    None
                }
            })
            .collect();

        let mut structured_entries = Vec::with_capacity(entries.len());
        let mut flat_data = Vec::with_capacity(entries.len());
        for (stream_id, fields) in entries {
            // Apply client-side filtering for non-ID conditions
            if !non_id_conds.is_empty() {
                let matches = non_id_conds.iter().enumerate().all(|(i, c)| {
                    fields.iter().any(|(f, v)| {
                        if f != &c.column_name {
                            return false;
                        }
                        eval_condition(v, c, like_matchers[i].as_ref())
                    })
                });
                if !matches {
                    continue;
                }
            }

            let mut row = vec![stream_id.clone()];
            for (field, value) in fields {
                row.push(field);
                row.push(value);
            }
            flat_data.push(stream_id);
            structured_entries.push(row);
        }

        let filtered_count = structured_entries.len();
        self.entries = structured_entries;
        self.dataset = DataSet::Filtered(flat_data);
        Ok((new_cursor, filtered_count))
    }

    fn configure(
        &mut self,
        column_names: &[String],
        pushdown_column_index: usize,
        _score_column_index: Option<usize>,
    ) {
        self.column_names = column_names.to_vec();
        self.pushdown_column_index = pushdown_column_index;
    }

    fn load_multi_key_data(
        &mut self,
        _conn: &mut dyn redis::ConnectionLike,
        _keys: &[String],
    ) -> Result<Vec<String>, redis::RedisError> {
        Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Multi-key mode is not supported for Redis Stream",
        )))
    }

    fn clear(&mut self) {
        self.dataset = DataSet::default();
        self.entries = Vec::new();
        self.last_id = None;
    }

    fn redis_type_name(&self) -> &'static str {
        "stream"
    }

    fn set_filtered_data(&mut self, data: Vec<String>) {
        // Called by parameterized_lookup with a single row's column values
        // (e.g. [stream_id, val1, val2, ...]). get_row's DataSet::Filtered
        // branch reconstructs columns via split('\t'), so we join here.
        // Without this, only the first column survives — everything else
        // comes back as NULL to PostgreSQL on JOIN.
        //
        // Must clear self.entries: get_row prefers `entries` when non-empty,
        // so stale data from a previous scan would shadow the new row.
        self.entries.clear();
        if data.is_empty() {
            self.dataset = DataSet::Empty;
        } else {
            self.dataset = DataSet::Filtered(vec![data.join("\t")]);
        }
    }

    fn multi_key_columns_per_row(&self) -> usize {
        4
    }

    fn batch_parameterized_lookup(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        params: &[String],
    ) -> Result<Vec<Option<Vec<String>>>, redis::RedisError> {
        if params.is_empty() {
            return Ok(Vec::new());
        }

        // Convert a single XRANGE id-id result into the row shape configured
        // for this table (stream_id, then per-column lookup over the field-
        // value chunks). Returns None when the entry doesn't exist.
        let entry_to_row = |entry: Option<(String, Vec<(String, String)>)>| -> Option<Vec<String>> {
            let (real_id, fields) = entry?;
            if self.column_names.len() > 1 {
                let mut row = Vec::with_capacity(self.column_names.len());
                row.push(real_id);
                for col_name in &self.column_names[1..] {
                    let v = fields
                        .iter()
                        .find(|(f, _)| f == col_name)
                        .map(|(_, v)| v.clone())
                        .unwrap_or_else(|| "NULL".to_string());
                    row.push(v);
                }
                Some(row)
            } else {
                Some(vec![real_id])
            }
        };

        // Fast path: single param — direct XRANGE id id.
        if params.len() == 1 {
            let p = &params[0];
            // Validate the id first; passing 'garbage' to XRANGE raises
            // "ERR Invalid stream ID" which would abort the whole join.
            if parse_stream_id_bound(p).is_none() {
                return Ok(vec![None]);
            }
            let entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
                .arg(key_prefix)
                .arg(p)
                .arg(p)
                .query(conn)?;
            return Ok(vec![entry_to_row(entries.into_iter().next())]);
        }

        // Multi-param: try pipelined XRANGE id id; fall back per-key on cluster.
        // Filter out malformed ids up-front and back-fill None for those slots
        // so a single bad row in the outer relation cannot abort the join.
        let mut valid_indices: Vec<usize> = Vec::with_capacity(params.len());
        let mut valid_params: Vec<&String> = Vec::with_capacity(params.len());
        for (i, p) in params.iter().enumerate() {
            if parse_stream_id_bound(p).is_some() {
                valid_indices.push(i);
                valid_params.push(p);
            }
        }

        let mut results: Vec<Option<Vec<String>>> = vec![None; params.len()];
        if valid_params.is_empty() {
            return Ok(results);
        }

        type StreamEntries = Vec<(String, Vec<(String, String)>)>;
        let pipe_result: Result<Vec<StreamEntries>, redis::RedisError> = {
            let mut pipe = redis::pipe();
            for p in &valid_params {
                pipe.cmd("XRANGE").arg(key_prefix).arg(*p).arg(*p);
            }
            pipe.query(conn)
        };

        let per_param_entries: Vec<StreamEntries> = match pipe_result {
            Ok(v) => v,
            Err(_e) => {
                // Pipeline fails on ClusterConnection — fall through to per-key
                // XRANGE. No pgrx::log! here; see the comment at the top of
                // load_with_stream_optimization for the link-error rationale.
                let mut out = Vec::with_capacity(valid_params.len());
                for p in &valid_params {
                    let r: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
                        .arg(key_prefix)
                        .arg(*p)
                        .arg(*p)
                        .query(conn)?;
                    out.push(r);
                }
                out
            }
        };

        for (idx, entries) in valid_indices.into_iter().zip(per_param_entries) {
            results[idx] = entry_to_row(entries.into_iter().next());
        }

        Ok(results)
    }
}
