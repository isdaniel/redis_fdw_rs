use crate::core::pool_manager::PooledConnection;
use crate::tables::types::RedisTableType;

#[derive(Debug, Clone, PartialEq)]
pub enum RedisJoinType {
    Inner,
    Left,
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
    pub result_data: Vec<Vec<String>>,
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
            result_data: Vec::new(),
            current_row: 0,
            result_columns: 0,
        }
    }
}
