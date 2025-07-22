use crate::redis_fdw::{
    pushdown::{ComparisonOperator, PushableCondition},
    tables::interface::RedisTableOperations,
    data_set::{DataSet, DataContainer, LoadDataResult},
};

/// Redis String table type
#[derive(Debug, Clone, Default)]
pub struct RedisStringTable {
    pub dataset: DataSet,
}

impl RedisStringTable {
    pub fn new() -> Self {
        Self { 
            dataset: DataSet::Empty,
        }
    }
}

impl RedisTableOperations for RedisStringTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<LoadDataResult, redis::RedisError> {
        if let Some(conditions) = conditions {
            if let Some(condition) = conditions.first() {
                // String tables can only be checked for exact value match
                let value: Option<String> = redis::cmd("GET").arg(key_prefix).query(conn)?;

                return if let Some(v) = value {
                    if v == condition.value {
                        self.dataset = DataSet::Filtered(vec![v.clone()]);
                        Ok(LoadDataResult::PushdownApplied(vec![v]))
                    } else {
                        self.dataset = DataSet::Empty;
                        Ok(LoadDataResult::Empty)
                    }
                } else {
                    self.dataset = DataSet::Empty;
                    Ok(LoadDataResult::Empty)
                };
            }
        }

        // Load all data into internal storage
        let value: Option<String> = redis::cmd("GET").arg(key_prefix).query(conn)?;
        self.dataset = DataSet::Complete(DataContainer::String(value));
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
        if let Some(value) = data.first() {
            let _: () = redis::cmd("SET").arg(key_prefix).arg(value).query(conn)?;
            self.dataset = DataSet::Complete(DataContainer::String(Some(value.clone())));
        }
        Ok(())
    }

    fn delete(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        _data: &[String],
    ) -> Result<(), redis::RedisError> {
        let _: () = redis::cmd("DEL").arg(key_prefix).query(conn)?;
        self.dataset = DataSet::Complete(DataContainer::String(None));
        Ok(())
    }

    fn update(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        _old_data: &[String],
        new_data: &[String],
    ) -> Result<(), redis::RedisError> {
        if let Some(value) = new_data.first() {
            let _: () = redis::cmd("SET").arg(key_prefix).arg(value).query(conn)?;
            self.dataset = DataSet::Complete(DataContainer::String(Some(value.clone())));
        }
        Ok(())
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        matches!(operator, ComparisonOperator::Equal)
    }
}
