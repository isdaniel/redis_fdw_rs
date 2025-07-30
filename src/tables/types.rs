/// Redis FDW data types and enums
/// This module contains the core data type definitions used throughout the Redis FDW
use crate::{
    query::pushdown_types::{ComparisonOperator, PushableCondition},
    tables::{
        implementations::{
            RedisHashTable, RedisListTable, RedisSetTable, RedisStringTable, RedisZSetTable,
        },
        interface::RedisTableOperations,
    },
};

/// Enum representing different Redis table types with their implementations
#[derive(Debug, Clone)]
pub enum RedisTableType {
    String(RedisStringTable),
    Hash(RedisHashTable),
    List(RedisListTable),
    Set(RedisSetTable),
    ZSet(RedisZSetTable),
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
            _ => RedisTableType::None,
        }
    }

    pub fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<LoadDataResult, redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::Hash(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::List(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::Set(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::ZSet(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::None => Ok(LoadDataResult::Empty),
        }
    }

    pub fn data_len(&self) -> usize {
        match self {
            RedisTableType::String(table) => table.data_len(),
            RedisTableType::Hash(table) => table.data_len(),
            RedisTableType::List(table) => table.data_len(),
            RedisTableType::Set(table) => table.data_len(),
            RedisTableType::ZSet(table) => table.data_len(),
            RedisTableType::None => 0,
        }
    }

    pub fn get_row(&self, index: usize) -> Option<Vec<String>> {
        match self {
            RedisTableType::String(table) => table.get_row(index),
            RedisTableType::Hash(table) => table.get_row(index),
            RedisTableType::List(table) => table.get_row(index),
            RedisTableType::Set(table) => table.get_row(index),
            RedisTableType::ZSet(table) => table.get_row(index),
            RedisTableType::None => None,
        }
    }

    pub fn insert(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.insert(conn, key_prefix, data),
            RedisTableType::Hash(table) => table.insert(conn, key_prefix, data),
            RedisTableType::List(table) => table.insert(conn, key_prefix, data),
            RedisTableType::Set(table) => table.insert(conn, key_prefix, data),
            RedisTableType::ZSet(table) => table.insert(conn, key_prefix, data),
            RedisTableType::None => Ok(()),
        }
    }

    pub fn delete(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.delete(conn, key_prefix, data),
            RedisTableType::Hash(table) => table.delete(conn, key_prefix, data),
            RedisTableType::List(table) => table.delete(conn, key_prefix, data),
            RedisTableType::Set(table) => table.delete(conn, key_prefix, data),
            RedisTableType::ZSet(table) => table.delete(conn, key_prefix, data),
            RedisTableType::None => Ok(()),
        }
    }

    /// Check if this table type supports a specific pushdown operator
    pub fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        match self {
            RedisTableType::String(table) => table.supports_pushdown(operator),
            RedisTableType::Hash(table) => table.supports_pushdown(operator),
            RedisTableType::List(table) => table.supports_pushdown(operator),
            RedisTableType::Set(table) => table.supports_pushdown(operator),
            RedisTableType::ZSet(table) => table.supports_pushdown(operator),
            RedisTableType::None => false,
        }
    }
}

/// Result type for data loading operations
#[derive(Debug)]
pub enum LoadDataResult {
    /// Data was loaded and optimized with pushdown conditions
    PushdownApplied(Vec<String>),
    /// Data was loaded into internal storage without optimization
    LoadedToInternal,
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
pub enum DataContainer {
    /// Single string value (Redis String type)
    String(Option<String>),
    /// Key-value pairs (Redis Hash type)  
    Hash(Vec<(String, String)>),
    /// Ordered list of values (Redis List type)
    List(Vec<String>),
    /// Unordered set of values (Redis Set type)
    Set(Vec<String>),
    /// Sorted set with scores (Redis ZSet type)
    ZSet(Vec<(String, f64)>),
}

impl DataSet {
    /// Get the number of rows/items in this dataset
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
    pub fn get_row(&self, index: usize) -> Option<Vec<String>> {
        match self {
            DataSet::Empty => None,
            DataSet::Filtered(data) => {
                // Generic implementation - each element is a row
                data.get(index).map(|item| vec![item.clone()])
            }
            DataSet::Complete(container) => container.get_row(index),
        }
    }
}

impl DataContainer {
    /// Get the number of rows in this container
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
            DataContainer::Set(items) => items.len(),
            DataContainer::ZSet(items) => items.len(),
        }
    }

    /// Get a row at the specified index
    pub fn get_row(&self, index: usize) -> Option<Vec<String>> {
        match self {
            DataContainer::String(opt) => {
                if index == 0 && opt.is_some() {
                    opt.as_ref().map(|s| vec![s.clone()])
                } else {
                    None
                }
            }
            DataContainer::Hash(pairs) => pairs.get(index).map(|(k, v)| vec![k.clone(), v.clone()]),
            DataContainer::List(items) => items.get(index).map(|item| vec![item.clone()]),
            DataContainer::Set(items) => items.get(index).map(|item| vec![item.clone()]),
            DataContainer::ZSet(items) => items
                .get(index)
                .map(|(member, score)| vec![member.clone(), score.to_string()]),
        }
    }
}
