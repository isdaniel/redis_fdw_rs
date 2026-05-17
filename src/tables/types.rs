/// Redis FDW data types and enums
/// This module contains the core data type definitions used throughout the Redis FDW
use crate::{
    query::{
        limit::LimitOffsetInfo,
        pushdown_types::{ComparisonOperator, PushableCondition},
    },
    tables::{
        implementations::{
            RedisHashTable, RedisListTable, RedisSetTable, RedisStreamTable, RedisStringTable,
            RedisZSetTable,
        },
        interface::RedisTableOperations,
        macros::{table_dispatch, table_dispatch_mut_result},
    },
};
use std::borrow::Cow;

/// Enum representing different Redis table types with their implementations
#[derive(Debug, Clone)]
pub enum RedisTableType {
    String(RedisStringTable),
    Hash(RedisHashTable),
    List(RedisListTable),
    Set(RedisSetTable),
    ZSet(RedisZSetTable),
    Stream(RedisStreamTable),
    None,
}

impl RedisTableType {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "string" => RedisTableType::String(RedisStringTable::new()),
            "hash" => RedisTableType::Hash(RedisHashTable::new()),
            "list" => RedisTableType::List(RedisListTable::new()),
            "set" => RedisTableType::Set(RedisSetTable::new()),
            "zset" => RedisTableType::ZSet(RedisZSetTable::new()),
            "stream" => RedisTableType::Stream(RedisStreamTable::new(1000)),
            _ => RedisTableType::None,
        }
    }

    #[allow(dead_code)]
    pub fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
        limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        table_dispatch_mut_result!(self, load_data(conn, key_prefix, conditions, limit_offset))
    }

    pub fn load_batch(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        cursor: u64,
        batch_size: usize,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<(u64, usize), redis::RedisError> {
        table_dispatch_mut_result!(self, load_batch(conn, key_prefix, cursor, batch_size, conditions) -> Result<(u64, usize), redis::RedisError>, Ok((0, 0)))
    }

    pub fn data_len(&self) -> usize {
        table_dispatch!(self, data_len() -> 0)
    }

    pub fn clear_data(&mut self) {
        match self {
            RedisTableType::String(t) => t.dataset = DataSet::default(),
            RedisTableType::Hash(t) => t.dataset = DataSet::default(),
            RedisTableType::List(t) => t.dataset = DataSet::default(),
            RedisTableType::Set(t) => t.dataset = DataSet::default(),
            RedisTableType::ZSet(t) => t.dataset = DataSet::default(),
            RedisTableType::Stream(t) => t.dataset = DataSet::default(),
            RedisTableType::None => {}
        }
    }

    /// Get a row at the specified index
    #[inline]
    pub fn get_row(&self, index: usize) -> Option<Vec<Cow<'_, str>>> {
        table_dispatch!(self, get_row(index) -> None)
    }

    pub fn insert(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        table_dispatch_mut_result!(self, insert(conn, key_prefix, data) -> Result<(), redis::RedisError>, Ok(()))
    }

    pub fn delete(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        table_dispatch_mut_result!(self, delete(conn, key_prefix, data) -> Result<(), redis::RedisError>, Ok(()))
    }

    pub fn update(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        old_data: &[String],
        new_data: &[String],
    ) -> Result<(), redis::RedisError> {
        table_dispatch_mut_result!(self, update(conn, key_prefix, old_data, new_data) -> Result<(), redis::RedisError>, Ok(()))
    }

    /// Check if this table type supports a specific pushdown operator
    pub fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        table_dispatch!(self, supports_pushdown(operator) -> false)
    }
}

/// Result type for data loading operations
#[derive(Debug)]
#[allow(dead_code)]
pub enum LoadDataResult {
    /// Data was loaded into internal storage (possibly with pushdown applied)
    FullyLoaded,
    /// No data found or operation resulted in empty set
    Empty,
}

/// Represents the different states of data in a Redis table
#[derive(Debug, Clone, Default)]
pub enum DataSet {
    /// No data has been loaded yet
    #[default]
    Empty,
    /// Data loaded with pushdown optimization applied
    Filtered(Vec<String>),
    /// All data loaded without filtering
    Complete(DataContainer),
}

/// Container for complete data sets with type-specific storage
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum DataContainer {
    /// Single string value (Redis String type)
    String(Option<String>),
    /// Key-value pairs (Redis Hash type)
    Hash(Vec<(String, String)>),
    /// Ordered list of values (Redis List type)
    List(Vec<String>),
    /// Sorted set with scores (Redis ZSet type)
    ZSet(Vec<(String, f64)>),
}

impl DataSet {
    /// Get the number of rows/items in this dataset
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            DataSet::Empty => 0,
            DataSet::Filtered(data) => {
                // Filtered data length depends on the data structure
                // This will be properly handled by the specific table type's get_row implementation
                data.len()
            }
            DataSet::Complete(container) => container.len(),
        }
    }

    /// Get a row at the specified index
    /// Note: For filtered data, this is a generic implementation
    /// Table types should override get_row to handle their specific data format
    #[inline]
    pub fn get_row(&self, index: usize) -> Option<Vec<Cow<'_, str>>> {
        match self {
            DataSet::Empty => None,
            DataSet::Filtered(data) => {
                // Generic implementation - each element is a row
                data.get(index)
                    .map(|item| vec![Cow::Borrowed(item.as_str())])
            }
            DataSet::Complete(container) => container.get_row(index),
        }
    }
}

impl DataContainer {
    /// Get the number of rows in this container
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            DataContainer::String(opt) => {
                if opt.is_some() {
                    1
                } else {
                    0
                }
            }
            DataContainer::Hash(pairs) => pairs.len(),
            DataContainer::List(items) => items.len(),
            DataContainer::ZSet(items) => items.len(),
        }
    }

    /// Get a row at the specified index - returns borrowed strings to avoid cloning
    #[inline]
    pub fn get_row(&self, index: usize) -> Option<Vec<Cow<'_, str>>> {
        match self {
            DataContainer::String(opt) => {
                if index == 0 && opt.is_some() {
                    opt.as_ref().map(|s| vec![Cow::Borrowed(s.as_str())])
                } else {
                    None
                }
            }
            DataContainer::Hash(pairs) => pairs
                .get(index)
                .map(|(k, v)| vec![Cow::Borrowed(k.as_str()), Cow::Borrowed(v.as_str())]),
            DataContainer::List(items) => items
                .get(index)
                .map(|item| vec![Cow::Borrowed(item.as_str())]),
            DataContainer::ZSet(items) => items.get(index).map(|(member, score)| {
                vec![
                    Cow::Borrowed(member.as_str()),
                    Cow::Owned(score.to_string()),
                ]
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_table_type_from_str() {
        assert!(matches!(
            RedisTableType::from_str("string"),
            RedisTableType::String(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("hash"),
            RedisTableType::Hash(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("list"),
            RedisTableType::List(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("set"),
            RedisTableType::Set(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("zset"),
            RedisTableType::ZSet(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("stream"),
            RedisTableType::Stream(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("unknown"),
            RedisTableType::None
        ));
        assert!(matches!(
            RedisTableType::from_str("STRING"),
            RedisTableType::String(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("Hash"),
            RedisTableType::Hash(_)
        ));
    }

    #[test]
    fn test_dataset_empty() {
        let ds = DataSet::Empty;
        assert_eq!(ds.len(), 0);
        assert!(ds.get_row(0).is_none());
    }

    #[test]
    fn test_dataset_filtered() {
        let ds = DataSet::Filtered(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        assert_eq!(ds.len(), 3);
        let row = ds.get_row(0).unwrap();
        assert_eq!(row, vec![Cow::Borrowed("a")]);
        let row2 = ds.get_row(2).unwrap();
        assert_eq!(row2, vec![Cow::Borrowed("c")]);
        assert!(ds.get_row(3).is_none());
    }

    #[test]
    fn test_dataset_complete_string() {
        let ds = DataSet::Complete(DataContainer::String(Some("hello".to_string())));
        assert_eq!(ds.len(), 1);
        let row = ds.get_row(0).unwrap();
        assert_eq!(row, vec![Cow::Borrowed("hello")]);
        assert!(ds.get_row(1).is_none());
    }

    #[test]
    fn test_dataset_complete_string_none() {
        let ds = DataSet::Complete(DataContainer::String(None));
        assert_eq!(ds.len(), 0);
        assert!(ds.get_row(0).is_none());
    }

    #[test]
    fn test_data_container_hash() {
        let container = DataContainer::Hash(vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ]);
        assert_eq!(container.len(), 2);
        let row = container.get_row(0).unwrap();
        assert_eq!(row, vec![Cow::Borrowed("key1"), Cow::Borrowed("val1")]);
        let row2 = container.get_row(1).unwrap();
        assert_eq!(row2, vec![Cow::Borrowed("key2"), Cow::Borrowed("val2")]);
        assert!(container.get_row(2).is_none());
    }

    #[test]
    fn test_data_container_list() {
        let container = DataContainer::List(vec![
            "item1".to_string(),
            "item2".to_string(),
            "item3".to_string(),
        ]);
        assert_eq!(container.len(), 3);
        let row = container.get_row(1).unwrap();
        assert_eq!(row, vec![Cow::Borrowed("item2")]);
    }

    #[test]
    fn test_data_container_zset() {
        let container = DataContainer::ZSet(vec![
            ("member1".to_string(), 1.5),
            ("member2".to_string(), 2.7),
        ]);
        assert_eq!(container.len(), 2);
        let row = container.get_row(0).unwrap();
        assert_eq!(row[0], Cow::Borrowed("member1"));
        assert_eq!(row[1], Cow::<str>::Owned("1.5".to_string()));
    }

    #[test]
    fn test_redis_table_type_data_len_none() {
        let table = RedisTableType::None;
        assert_eq!(table.data_len(), 0);
    }

    #[test]
    fn test_redis_table_type_get_row_none() {
        let table = RedisTableType::None;
        assert!(table.get_row(0).is_none());
    }

    #[test]
    fn test_load_data_result_variants() {
        let loaded = LoadDataResult::FullyLoaded;
        assert!(matches!(loaded, LoadDataResult::FullyLoaded));
        let empty = LoadDataResult::Empty;
        assert!(matches!(empty, LoadDataResult::Empty));
    }
}
