#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::{query::ComparisonOperator, tables::{DataSet, RedisStreamTable, RedisTableOperations}};
    use super::*;
    use redis::Commands;

    #[test]
    fn test_stream_table_creation() {
        let table = RedisStreamTable::new();
        assert_eq!(table.batch_size, 1000);
        assert!(table.last_id.is_none());
        assert!(matches!(table.dataset, DataSet::Empty));
    }

    #[test]
    fn test_stream_table_with_batch_size() {
        let table = RedisStreamTable::with_batch_size(500);
        assert_eq!(table.batch_size, 500);
    }

    #[test]
    fn test_supports_pushdown() {
        let table = RedisStreamTable::new();
        assert!(table.supports_pushdown(&ComparisonOperator::Equal));
        assert!(table.supports_pushdown(&ComparisonOperator::NotEqual));
        assert!(table.supports_pushdown(&ComparisonOperator::Like));
        assert!(!table.supports_pushdown(&ComparisonOperator::In));
        assert!(!table.supports_pushdown(&ComparisonOperator::NotIn));
    }
}
