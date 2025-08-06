/// Macro utilities for Redis table operations
///
/// This module provides macros to reduce boilerplate code when implementing
/// operations that need to be applied to all table type variants.

/// Macro to apply a method call to all variants of RedisTableType
macro_rules! table_dispatch {
    ($self:expr, $method:ident($($args:expr),*)) => {
        match $self {
            crate::tables::types::RedisTableType::String(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Hash(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::List(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Set(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::ZSet(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Stream(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::None => {
                Default::default()
            }
        }
    };
    ($self:expr, $method:ident($($args:expr),*) -> $default:expr) => {
        match $self {
            crate::tables::types::RedisTableType::String(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Hash(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::List(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Set(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::ZSet(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Stream(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::None => $default,
        }
    };
}

/// Macro for mut methods that return Result
macro_rules! table_dispatch_mut_result {
    ($self:expr, $method:ident($($args:expr),*)) => {
        match $self {
            crate::tables::types::RedisTableType::String(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Hash(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::List(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Set(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::ZSet(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Stream(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::None => Ok(crate::tables::types::LoadDataResult::Empty),
        }
    };
    ($self:expr, $method:ident($($args:expr),*) -> $result_type:ty, $default:expr) => {
        match $self {
            crate::tables::types::RedisTableType::String(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Hash(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::List(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Set(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::ZSet(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Stream(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::None => $default,
        }
    };
}

/// Macro for mut methods with side effects (no return value)
macro_rules! table_dispatch_mut_void {
    ($self:expr, $method:ident($($args:expr),*)) => {
        match $self {
            crate::tables::types::RedisTableType::String(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Hash(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::List(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Set(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::ZSet(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::Stream(table) => table.$method($($args),*),
            crate::tables::types::RedisTableType::None => {} // No-op for None type
        }
    };
}

pub(crate) use table_dispatch;
pub(crate) use table_dispatch_mut_result;
pub(crate) use table_dispatch_mut_void;
