use std::borrow::Cow;

use crate::{
    query::{
        limit::LimitOffsetInfo,
        pushdown_types::{ComparisonOperator, PushableCondition},
        scan_ops::extract_scan_conditions,
    },
    tables::{
        interface::RedisTableOperations,
        types::{DataSet, LoadDataResult},
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
}

impl RedisStreamTable {
    pub fn new(batch_size: usize) -> Self {
        Self {
            dataset: DataSet::Empty,
            entries: Vec::new(),
            last_id: None,
            batch_size,
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    fn load_with_stream_optimization(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        scan_conditions: &crate::query::scan_ops::ScanConditions,
    ) -> Result<LoadDataResult, redis::RedisError> {
        // Extract time-based conditions for ID range queries
        let mut start_id = "-".to_string(); // Start from beginning
        let mut end_id = "+".to_string(); // Go to end
        let mut count = Some(self.batch_size);

        // Check for time-based or ID-based conditions
        for condition in &scan_conditions.exact_conditions {
            match condition.operator {
                ComparisonOperator::Equal => {
                    // Exact ID match - use as both start and end
                    start_id = condition.value.clone();
                    end_id = condition.value.clone();
                    count = Some(1); // Only need one entry
                }
                ComparisonOperator::NotEqual => {
                    // For streams, not equal is less useful but we can handle it
                    // by loading all data except this specific ID
                    continue;
                }
                _ => {} // Other operators not directly applicable to stream IDs
            }
        }

        self.load_with_xrange(conn, key_prefix, &start_id, &end_id, count)
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
            if scan_conditions.has_optimizable_conditions() {
                return self.load_with_stream_optimization(conn, key_prefix, &scan_conditions);
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
    fn get_row(&self, index: usize) -> Option<Vec<Cow<'_, str>>> {
        // Use structured entries for zero-allocation access
        if !self.entries.is_empty() {
            self.entries
                .get(index)
                .map(|row| row.iter().map(|s| Cow::Borrowed(s.as_str())).collect())
        } else {
            match &self.dataset {
                DataSet::Filtered(entries) => {
                    // Legacy path: parse tab-separated entry back to fields
                    entries.get(index).map(|entry| {
                        entry
                            .split('\t')
                            .map(|s| Cow::Owned(s.to_string()))
                            .collect()
                    })
                }
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
        // Redis Streams support range queries based on stream IDs (timestamps)
        matches!(
            operator,
            ComparisonOperator::Equal | ComparisonOperator::NotEqual | ComparisonOperator::Like
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
        let (start_id, end_id) = if let Some(conds) = conditions {
            let mut start = None;
            let mut end = None;
            for c in conds {
                if c.operator == ComparisonOperator::Equal && c.column_name == "id" {
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
            .map(|conds| conds.iter().filter(|c| c.column_name != "id").collect())
            .unwrap_or_default();

        // Pre-calculate PatternMatchers for LIKE conditions (index-based)
        let like_matchers: Vec<(usize, crate::query::scan_ops::PatternMatcher)> = non_id_conds
            .iter()
            .enumerate()
            .filter(|(_, c)| c.operator == ComparisonOperator::Like)
            .map(|(i, c)| {
                (
                    i,
                    crate::query::scan_ops::PatternMatcher::from_like_pattern(&c.value),
                )
            })
            .collect();

        let mut structured_entries = Vec::with_capacity(entries.len());
        let mut flat_data = Vec::with_capacity(entries.len());
        for (stream_id, fields) in entries {
            // Apply client-side filtering for non-ID conditions
            if !non_id_conds.is_empty() {
                let matches = non_id_conds.iter().enumerate().all(|(i, c)| {
                    fields.iter().any(|(f, v)| {
                        let target = if c.column_name == "field" { f } else { v };
                        match c.operator {
                            ComparisonOperator::Equal => target == &c.value,
                            ComparisonOperator::NotEqual => target != &c.value,
                            ComparisonOperator::Like => like_matchers
                                .iter()
                                .find(|(idx, _)| *idx == i)
                                .is_some_and(|(_, m)| m.matches(target)),
                            _ => true,
                        }
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
}
