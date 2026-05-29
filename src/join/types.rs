use crate::core::pool_manager::PooledConnection;
use crate::tables::types::RedisTableType;
use smallvec::SmallVec;

pub type JoinRow = SmallVec<[String; 3]>;

#[derive(Debug, Clone, PartialEq)]
pub enum RedisJoinType {
    Inner,
    Left,
}

/// Compact representation of a join result row.
/// Instead of cloning all strings, we store indices into the source data vectors.
#[derive(Debug, Clone)]
pub enum JoinResultRow {
    /// Both outer and inner matched (INNER JOIN or matched LEFT JOIN row)
    Matched { outer_idx: usize, inner_idx: usize },
    /// Outer row with no inner match (LEFT JOIN unmatched row)
    OuterOnly { outer_idx: usize },
}

pub struct RedisJoinState {
    pub connection: Option<PooledConnection>,
    pub outer_table_type: RedisTableType,
    pub inner_table_type: RedisTableType,
    pub outer_key_prefix: String,
    pub inner_key_prefix: String,
    pub join_type: RedisJoinType,
    pub join_column_outer: usize,
    pub join_column_inner: usize,
    /// Source data from outer relation (retained for index-based iteration)
    pub outer_data: Vec<JoinRow>,
    /// Source data from inner relation (retained for index-based iteration)
    pub inner_data: Vec<JoinRow>,
    /// Compact join result: indices into outer_data/inner_data
    pub result_indices: Vec<JoinResultRow>,
    pub current_row: usize,
    pub result_columns: usize,
}

impl RedisJoinState {
    pub fn new(
        outer_table_type: RedisTableType,
        inner_table_type: RedisTableType,
        outer_key_prefix: String,
        inner_key_prefix: String,
        join_type: RedisJoinType,
        join_column_outer: usize,
        join_column_inner: usize,
    ) -> Self {
        Self {
            connection: None,
            outer_table_type,
            inner_table_type,
            outer_key_prefix,
            inner_key_prefix,
            join_type,
            join_column_outer,
            join_column_inner,
            outer_data: Vec::new(),
            inner_data: Vec::new(),
            result_indices: Vec::new(),
            current_row: 0,
            result_columns: 0,
        }
    }
}
