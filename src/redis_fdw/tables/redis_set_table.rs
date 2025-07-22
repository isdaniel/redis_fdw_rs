
use crate::redis_fdw::{
    pushdown::{ComparisonOperator, PushableCondition},
    tables::interface::RedisTableOperations,
};

/// Redis Set table type
#[derive(Debug, Clone, Default)]
pub struct RedisSetTable {
    pub data: Vec<String>,
}

impl RedisSetTable {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

impl RedisTableOperations for RedisSetTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<Option<Vec<String>>, redis::RedisError> {
        if let Some(conditions) = conditions {
            if !conditions.is_empty() {
                // For sets, we can check membership efficiently
                for condition in conditions {
                    match condition.operator {
                        ComparisonOperator::Equal => {
                            // SISMEMBER for specific member
                            let exists: bool = redis::cmd("SISMEMBER")
                                .arg(key_prefix)
                                .arg(&condition.value)
                                .query(conn)?;

                            return if exists {
                                Ok(Some(vec![condition.value.clone()]))
                            } else {
                                Ok(Some(vec![]))
                            };
                        }
                        ComparisonOperator::In => {
                            // Check multiple members
                            let members: Vec<&str> = condition.value.split(',').collect();
                            let mut result = Vec::new();

                            for member in members {
                                let exists: bool = redis::cmd("SISMEMBER")
                                    .arg(key_prefix)
                                    .arg(member)
                                    .query(conn)?;

                                if exists {
                                    result.push(member.to_string());
                                }
                            }
                            return Ok(Some(result));
                        }
                        _ => {} // Fall back to full scan
                    }
                }
            }
        }

        // Load all data into internal storage
        self.data = redis::cmd("SMEMBERS").arg(key_prefix).query(conn)?;
        Ok(None)
    }

    fn data_len(&self, filtered_data: Option<&[String]>) -> usize {
        if let Some(filtered_data) = filtered_data {
            filtered_data.len()
        } else {
            self.data.len()
        }
    }

    fn get_row(&self, index: usize, filtered_data: Option<&[String]>) -> Option<Vec<String>> {
        if let Some(filtered_data) = filtered_data {
            // Set data is stored as [member1, member2, ...]
            if index < filtered_data.len() {
                Some(vec![filtered_data[index].clone()])
            } else {
                None
            }
        } else {
            self.data.get(index).map(|item| vec![item.clone()])
        }
    }

    fn insert(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        for value in data {
            let added: i32 = redis::cmd("SADD").arg(key_prefix).arg(value).query(conn)?;
            if added > 0 {
                self.data.push(value.clone());
            }
        }
        Ok(())
    }

    fn delete(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        for value in data {
            let _: i32 = redis::cmd("SREM").arg(key_prefix).arg(value).query(conn)?;
            self.data.retain(|x| x != value);
        }
        Ok(())
    }

    fn update(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        old_data: &[String],
        new_data: &[String],
    ) -> Result<(), redis::RedisError> {
        // For sets, update means remove old and add new
        self.delete(conn, key_prefix, old_data)?;
        self.insert(conn, key_prefix, new_data)?;
        Ok(())
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        matches!(operator, ComparisonOperator::Equal | ComparisonOperator::In)
    }
}
