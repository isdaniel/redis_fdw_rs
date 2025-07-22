use crate::{
    query::pushdown_types::{ComparisonOperator, PushableCondition},
    tables::{
        interface::RedisTableOperations,
        types::{DataContainer, DataSet, LoadDataResult},
    },
};

/// Redis Set table type
#[derive(Debug, Clone, Default)]
pub struct RedisSetTable {
    pub dataset: DataSet,
}

impl RedisSetTable {
    pub fn new() -> Self {
        Self {
            dataset: DataSet::Empty,
        }
    }
}

impl RedisTableOperations for RedisSetTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<LoadDataResult, redis::RedisError> {
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
                                let filtered_data = vec![condition.value.clone()];
                                self.dataset = DataSet::Filtered(filtered_data.clone());
                                Ok(LoadDataResult::PushdownApplied(filtered_data))
                            } else {
                                self.dataset = DataSet::Empty;
                                Ok(LoadDataResult::Empty)
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
                            self.dataset = DataSet::Filtered(result.clone());
                            return Ok(LoadDataResult::PushdownApplied(result));
                        }
                        _ => {} // Fall back to full scan
                    }
                }
            }
        }

        // Load all data into internal storage
        let data: Vec<String> = redis::cmd("SMEMBERS").arg(key_prefix).query(conn)?;
        self.dataset = DataSet::Complete(DataContainer::Set(data));
        Ok(LoadDataResult::LoadedToInternal)
    }

    fn get_dataset(&self) -> &DataSet {
        &self.dataset
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
                // Update internal data
                if let DataSet::Complete(DataContainer::Set(ref mut set_data)) = &mut self.dataset {
                    set_data.push(value.clone());
                } else {
                    // Create new set data if not present
                    self.dataset = DataSet::Complete(DataContainer::Set(vec![value.clone()]));
                }
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

            // Remove from local data
            if let DataSet::Complete(DataContainer::Set(ref mut set_data)) = &mut self.dataset {
                set_data.retain(|x| x != value);
            }
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
