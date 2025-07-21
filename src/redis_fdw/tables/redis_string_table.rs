use redis::Commands;

use crate::redis_fdw::{
    pushdown::{ComparisonOperator, PushableCondition},
    tables::interface::RedisTableOperations,
};

/// Redis String table type
#[derive(Debug, Clone, Default)]
pub struct RedisStringTable {
    pub data: Option<String>,
}

impl RedisStringTable {
    pub fn new() -> Self {
        Self { data: None }
    }
}

impl RedisTableOperations for RedisStringTable {
    fn load_data(
        &mut self,
        conn: &mut redis::Connection,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<Option<Vec<String>>, redis::RedisError> {
        if let Some(conditions) = conditions {
            if let Some(condition) = conditions.first() {
                // String tables can only be checked for exact value match
                let value: Option<String> = redis::cmd("GET").arg(key_prefix).query(conn)?;

                return if let Some(v) = value {
                    if v == condition.value {
                        Ok(Some(vec![v]))
                    } else {
                        Ok(Some(vec![]))
                    }
                } else {
                    Ok(Some(vec![]))
                };
            }
        }

        // Load all data into internal storage
        self.data = conn.get(key_prefix)?;
        Ok(None)
    }

    fn data_len(&self, filtered_data: Option<&[String]>) -> usize {
        if let Some(filtered_data) = filtered_data {
            if filtered_data.is_empty() {
                0
            } else {
                1
            }
        } else {
            if self.data.is_some() {
                1
            } else {
                0
            }
        }
    }

    fn get_row(&self, index: usize, filtered_data: Option<&[String]>) -> Option<Vec<String>> {
        if let Some(filtered_data) = filtered_data {
            // String data is stored as [value]
            if index == 0 && !filtered_data.is_empty() {
                Some(vec![filtered_data[0].clone()])
            } else {
                None
            }
        } else {
            if index == 0 && self.data.is_some() {
                Some(vec![self.data.as_ref().unwrap().clone()])
            } else {
                None
            }
        }
    }

    fn insert(
        &mut self,
        conn: &mut redis::Connection,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        if let Some(value) = data.first() {
            let _: () = conn.set(key_prefix, value)?;
            self.data = Some(value.clone());
        }
        Ok(())
    }

    fn delete(
        &mut self,
        conn: &mut redis::Connection,
        key_prefix: &str,
        _data: &[String],
    ) -> Result<(), redis::RedisError> {
        let _: () = conn.del(key_prefix)?;
        self.data = None;
        Ok(())
    }

    fn update(
        &mut self,
        conn: &mut redis::Connection,
        key_prefix: &str,
        _old_data: &[String],
        new_data: &[String],
    ) -> Result<(), redis::RedisError> {
        if let Some(value) = new_data.first() {
            let _: () = conn.set(key_prefix, value)?;
            self.data = Some(value.clone());
        }
        Ok(())
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        matches!(operator, ComparisonOperator::Equal)
    }
}
