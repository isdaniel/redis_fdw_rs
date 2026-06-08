//! Pure-Rust EXPLAIN report. No pg_sys dependencies — unit-testable.

#[derive(Debug, PartialEq, Eq)]
pub enum Prop {
    Text {
        label: &'static str,
        value: String,
    },
    Int {
        label: &'static str,
        unit: Option<&'static str>,
        value: i64,
    },
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ExplainReport {
    pub props: Vec<Prop>,
}

impl ExplainReport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn text(&mut self, label: &'static str, value: impl Into<String>) {
        self.props.push(Prop::Text {
            label,
            value: value.into(),
        });
    }

    pub fn int(&mut self, label: &'static str, unit: Option<&'static str>, value: i64) {
        self.props.push(Prop::Int { label, unit, value });
    }

    /// Add the labels common to every scan EXPLAIN output: server, key, type,
    /// multi-key mode, batch size. Accepts primitive args (not `&RedisFdwState`)
    /// so it stays trivially unit-testable.
    pub fn add_scan_core_fields(
        &mut self,
        host_port: &str,
        key_prefix: &str,
        type_name: &str,
        is_multi_key: bool,
        batch_size: usize,
    ) {
        self.text("Redis Server", host_port.to_string());
        self.text("Redis Key", key_prefix.to_string());
        self.text("Table Type", type_name.to_string());
        self.text(
            "Multi-Key Mode",
            if is_multi_key { "true" } else { "false" },
        );
        self.int("Batch Size", Some("rows"), batch_size as i64);
    }

    /// Render the pushdown summary. Emits `Pushdown: none` when no conditions
    /// were pushed, else a comma-separated list of `column op 'value'` clauses.
    pub fn add_pushdown_summary(
        &mut self,
        analysis: Option<&crate::query::pushdown_types::PushdownAnalysis>,
    ) {
        let desc = match analysis {
            Some(a) if a.has_optimizations() => a
                .pushable_conditions
                .iter()
                .map(|c| format!("{} {} '{}'", c.column_name, c.operator, c.value))
                .collect::<Vec<_>>()
                .join(", "),
            _ => "none".to_string(),
        };
        self.text("Pushdown", desc);
    }

    /// Render a join descriptor for FDW-to-FDW pushdown scans.
    pub fn add_join_descriptor(
        &mut self,
        host_port: &str,
        outer_type: &str,
        outer_key: &str,
        inner_type: &str,
        inner_key: &str,
    ) {
        let desc = format!(
            "{}({}) x {}({})",
            outer_type, outer_key, inner_type, inner_key
        );
        self.text("Redis Join", desc);
        self.text("Redis Server", host_port.to_string());
    }

    pub fn add_rows_fetched(&mut self, row_count: u32) {
        self.int("Rows Fetched", Some("rows"), row_count as i64);
    }

    /// Surface *why* a pushdown was skipped, when applicable. `None` means
    /// either pushdown ran or there was nothing to push — nothing is emitted.
    pub fn add_pushdown_skip_reason(&mut self, reason: Option<&str>) {
        if let Some(r) = reason {
            self.text("Pushdown Skipped", r.to_string());
        }
    }

    /// Emit batched parameterized join info: batch size (rows) and execution mode
    /// (`pipeline` | `fallback` | `n/a`).
    pub fn add_batch_join_info(&mut self, batch_size: usize, mode: &str) {
        self.int("Join Batch Size", Some("rows"), batch_size as i64);
        self.text("Join Batch Mode", mode.to_string());
    }

    /// Emit the Pushdown In Join label when Redis-side WHERE conditions filter
    /// the lookup result (post-fetch). `None` emits nothing.
    pub fn add_pushdown_in_join(&mut self, summary: Option<&str>) {
        if let Some(s) = summary {
            self.text("Pushdown In Join", s.to_string());
        }
    }

    /// Render which Redis commands this scan will issue. Helps users understand
    /// the actual access pattern at a glance.
    pub fn add_redis_ops(&mut self, ops: &[&'static str]) {
        let desc = if ops.is_empty() {
            "none".to_string()
        } else {
            ops.join(", ")
        };
        self.text("Redis Ops", desc);
    }

    /// Build a scan report from raw fields. Kept separate from `for_scan` so
    /// unit tests don't have to construct a full `RedisFdwState`.
    #[allow(clippy::too_many_arguments)]
    pub fn from_scan_inputs(
        host_port: &str,
        key_prefix: &str,
        type_name: &'static str,
        is_multi_key: bool,
        batch_size: usize,
        analysis: Option<&crate::query::pushdown_types::PushdownAnalysis>,
        skip_reason: Option<&str>,
        ops: &[&'static str],
        analyze: bool,
        row_count: u32,
    ) -> Self {
        let mut r = Self::new();
        r.add_scan_core_fields(host_port, key_prefix, type_name, is_multi_key, batch_size);
        r.add_pushdown_summary(analysis);
        r.add_pushdown_skip_reason(skip_reason);
        r.add_redis_ops(ops);
        if analyze {
            r.add_rows_fetched(row_count);
        }
        r
    }

    /// Build a modify report from raw fields.
    pub fn from_modify_inputs(host_port: &str, key_prefix: &str, type_name: &'static str) -> Self {
        let mut r = Self::new();
        r.text("Redis Server", host_port.to_string());
        r.text("Redis Key", key_prefix.to_string());
        r.text("Table Type", type_name.to_string());
        r
    }

    /// Build a join report from raw fields.
    pub fn from_join_inputs(
        host_port: &str,
        outer_type: &str,
        outer_key: &str,
        inner_type: &str,
        inner_key: &str,
    ) -> Self {
        let mut r = Self::new();
        r.add_join_descriptor(host_port, outer_type, outer_key, inner_type, inner_key);
        r
    }

    /// Public scan entrypoint: extract everything needed from `state` then delegate.
    pub fn for_scan(state: &crate::core::state_manager::RedisFdwState, analyze: bool) -> Self {
        if state.is_join_scan {
            // Prefer the descriptor we have; fall back to a generic label.
            if let Some(js) = state.join_state.as_ref() {
                return Self::from_join_inputs(
                    &state.host_port,
                    js.outer_table_type.redis_type_name(),
                    &js.outer_key_prefix,
                    js.inner_table_type.redis_type_name(),
                    &js.inner_key_prefix,
                );
            }
            let mut r = Self::new();
            r.text("Redis Join", "FDW-to-FDW pushdown".to_string());
            r.text("Redis Server", state.host_port.as_str().to_string());
            return r;
        }

        let ops = redis_ops_for(state);
        let skip = pushdown_skip_reason(state);
        let mut report = Self::from_scan_inputs(
            &state.host_port,
            &state.table_key_prefix,
            state.table_type.redis_type_name(),
            state.is_multi_key,
            state.batch_size,
            state.pushdown_analysis.as_ref(),
            skip,
            &ops,
            analyze,
            state.row_count,
        );

        // PR-2: surface batched parameterized join info.
        if state.is_parameterized {
            let mode = match state.join_batch_mode {
                crate::core::state_manager::BatchMode::Pipeline => "pipeline",
                crate::core::state_manager::BatchMode::Fallback => "fallback",
                crate::core::state_manager::BatchMode::NotApplicable => "n/a",
            };
            report.add_batch_join_info(state.join_batch_size, mode);

            if let Some(a) = state.pushdown_analysis.as_ref() {
                if a.has_optimizations() {
                    let summary = a
                        .pushable_conditions
                        .iter()
                        .map(|c| {
                            format!(
                                "{} {} '{}' (filtered after lookup)",
                                c.column_name, c.operator, c.value
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    report.add_pushdown_in_join(Some(&summary));
                }
            }
        }

        report
    }

    pub fn for_modify(state: &crate::core::state_manager::RedisFdwState) -> Self {
        Self::from_modify_inputs(
            &state.host_port,
            &state.table_key_prefix,
            state.table_type.redis_type_name(),
        )
    }
}

/// Compute which Redis commands this scan will issue. Static today;
/// PR-2 extends this to reflect batched/parameterized ops.
fn redis_ops_for(state: &crate::core::state_manager::RedisFdwState) -> Vec<&'static str> {
    use crate::tables::types::RedisTableType;
    if state.is_parameterized {
        // Reflect what batch_parameterized_lookup actually issues: the
        // fast-path uses the single-key command (HGET/GET/SISMEMBER/ZSCORE)
        // for params.len() == 1, falling back to the multi-key/pipelined
        // command for batches >1.
        return match &state.table_type {
            RedisTableType::Hash(_) => vec!["HGET", "HMGET"],
            RedisTableType::Set(_) => vec!["SISMEMBER"],
            RedisTableType::ZSet(_) => vec!["ZSCORE"],
            RedisTableType::String(_) => vec!["GET", "MGET"],
            _ => vec![],
        };
    }
    match &state.table_type {
        RedisTableType::String(_) if state.is_multi_key => vec!["SCAN", "MGET"],
        RedisTableType::String(_) => vec!["GET"],
        RedisTableType::Hash(_) if state.is_multi_key => vec!["SCAN", "HGETALL"],
        RedisTableType::Hash(_) => vec!["HGETALL"],
        RedisTableType::List(_) if state.is_multi_key => vec!["SCAN", "LRANGE"],
        RedisTableType::List(_) => vec!["LRANGE"],
        RedisTableType::Set(_) if state.is_multi_key => vec!["SCAN", "SMEMBERS"],
        RedisTableType::Set(_) => vec!["SMEMBERS"],
        RedisTableType::ZSet(_) if state.is_multi_key => vec!["SCAN", "ZRANGE"],
        RedisTableType::ZSet(_) => vec!["ZRANGE"],
        RedisTableType::Stream(_) => vec!["XRANGE"],
        RedisTableType::None => vec![],
    }
}

/// Determine whether pushdown was skipped and why. Returns `None` when no
/// skip happened (the common case).
fn pushdown_skip_reason(
    _state: &crate::core::state_manager::RedisFdwState,
) -> Option<&'static str> {
    // PR-2 will populate this when WHERE-through-join paths set a skip reason.
    // For PR-1 we always return None — the label is reserved.
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_report_is_empty() {
        let r = ExplainReport::new();
        assert!(r.props.is_empty());
    }

    #[test]
    fn text_pushes_text_prop() {
        let mut r = ExplainReport::new();
        r.text("Redis Server", "127.0.0.1:6379");
        assert_eq!(
            r.props,
            vec![Prop::Text {
                label: "Redis Server",
                value: "127.0.0.1:6379".to_string()
            }]
        );
    }

    #[test]
    fn int_pushes_int_prop() {
        let mut r = ExplainReport::new();
        r.int("Batch Size", Some("rows"), 256);
        assert_eq!(
            r.props,
            vec![Prop::Int {
                label: "Batch Size",
                unit: Some("rows"),
                value: 256
            }]
        );
    }

    #[test]
    fn scan_core_includes_server_key_type_multikey_batch() {
        let mut r = ExplainReport::new();
        r.add_scan_core_fields("127.0.0.1:6379", "user:42", "hash", false, 5000);

        assert!(matches!(
            r.props.first(),
            Some(Prop::Text { label: "Redis Server", value }) if value == "127.0.0.1:6379"
        ));
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Redis Key", value } if value == "user:42"
        )));
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Table Type", value } if value == "hash"
        )));
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Multi-Key Mode", value } if value == "false"
        )));
        assert!(r.props.iter().any(|p| matches!(
            p,
            Prop::Int {
                label: "Batch Size",
                unit: Some("rows"),
                value: 5000
            }
        )));
    }

    #[test]
    fn add_pushdown_with_no_analysis_emits_none() {
        let mut r = ExplainReport::new();
        r.add_pushdown_summary(None);
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Pushdown", value } if value == "none"
        )));
    }

    #[test]
    fn add_pushdown_with_conditions_lists_them() {
        use crate::query::pushdown_types::{
            ComparisonOperator, PushableCondition, PushdownAnalysis,
        };
        let analysis = PushdownAnalysis {
            can_optimize: true,
            pushable_conditions: vec![
                PushableCondition {
                    column_name: "field".to_string(),
                    column_index: 0,
                    operator: ComparisonOperator::Equal,
                    value: "x".to_string(),
                },
                PushableCondition {
                    column_name: "score".to_string(),
                    column_index: 1,
                    operator: ComparisonOperator::GreaterThanOrEqual,
                    value: "10".to_string(),
                },
            ],
            limit_offset: None,
        };

        let mut r = ExplainReport::new();
        r.add_pushdown_summary(Some(&analysis));

        let pushdown = r.props.iter().find_map(|p| match p {
            Prop::Text {
                label: "Pushdown",
                value,
            } => Some(value.clone()),
            _ => None,
        });
        let pushdown = pushdown.expect("Pushdown property missing");
        assert!(pushdown.contains("field"));
        assert!(pushdown.contains("score"));
        assert!(pushdown.contains("'x'"));
        assert!(pushdown.contains("'10'"));
    }

    #[test]
    fn add_join_emits_join_label_and_server() {
        let mut r = ExplainReport::new();
        r.add_join_descriptor("127.0.0.1:6379", "hash", "users", "set", "active");

        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Redis Join", value } if value == "hash(users) x set(active)"
        )));
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Redis Server", value } if value == "127.0.0.1:6379"
        )));
    }

    #[test]
    fn add_rows_fetched_emits_int_with_unit() {
        let mut r = ExplainReport::new();
        r.add_rows_fetched(123);
        assert!(r.props.iter().any(|p| matches!(
            p,
            Prop::Int {
                label: "Rows Fetched",
                unit: Some("rows"),
                value: 123
            }
        )));
    }

    #[test]
    fn add_pushdown_skip_reason_emits_text_only_when_some() {
        let mut r = ExplainReport::new();
        r.add_pushdown_skip_reason(None);
        assert!(!r.props.iter().any(|p| matches!(
            p,
            Prop::Text {
                label: "Pushdown Skipped",
                ..
            }
        )));

        let mut r2 = ExplainReport::new();
        r2.add_pushdown_skip_reason(Some("baserestrictinfo present on inner rel"));
        assert!(r2.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Pushdown Skipped", value }
                if value == "baserestrictinfo present on inner rel"
        )));
    }

    #[test]
    fn add_redis_ops_joins_with_commas() {
        let mut r = ExplainReport::new();
        r.add_redis_ops(&["HMGET", "ZRANGEBYSCORE"]);
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Redis Ops", value }
                if value == "HMGET, ZRANGEBYSCORE"
        )));
    }

    #[test]
    fn add_redis_ops_empty_emits_none() {
        let mut r = ExplainReport::new();
        r.add_redis_ops(&[]);
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Redis Ops", value } if value == "none"
        )));
    }

    #[test]
    fn add_batch_join_info_emits_label() {
        let mut r = ExplainReport::new();
        r.add_batch_join_info(256, "pipeline");
        assert!(r.props.iter().any(|p| matches!(
            p,
            Prop::Int {
                label: "Join Batch Size",
                unit: Some("rows"),
                value: 256
            }
        )));
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Join Batch Mode", value } if value == "pipeline"
        )));
    }

    #[test]
    fn add_pushdown_in_join_emits_when_present() {
        let mut r = ExplainReport::new();
        r.add_pushdown_in_join(Some("score >= 2.0 (filtered after lookup)"));
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Pushdown In Join", value }
                if value == "score >= 2.0 (filtered after lookup)"
        )));

        let mut empty = ExplainReport::new();
        empty.add_pushdown_in_join(None);
        assert!(!empty.props.iter().any(|p| matches!(
            p,
            Prop::Text {
                label: "Pushdown In Join",
                ..
            }
        )));
    }

    #[test]
    fn for_modify_emits_server_key_type_only() {
        let r = ExplainReport::from_modify_inputs("127.0.0.1:6379", "users:42", "hash");
        assert_eq!(r.props.len(), 3);
        assert!(matches!(
            &r.props[0],
            Prop::Text {
                label: "Redis Server",
                ..
            }
        ));
        assert!(matches!(
            &r.props[1],
            Prop::Text {
                label: "Redis Key",
                ..
            }
        ));
        assert!(matches!(
            &r.props[2],
            Prop::Text {
                label: "Table Type",
                ..
            }
        ));
    }
}
