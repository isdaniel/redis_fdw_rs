use crate::redis_fdw::{
    pushdown::{ComparisonOperator, PushableCondition},
    tables::interface::RedisTableOperations,
    data_set::{DataSet, DataContainer, LoadDataResult},
};

/// Redis List table type
#[derive(Debug, Clone, Default)]
pub struct RedisListTable {
    pub dataset: DataSet,
}

impl RedisListTable {
    pub fn new() -> Self {
        Self { 
            dataset: DataSet::Empty,
        }
    }
}

impl RedisTableOperations for RedisListTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        _conditions: Option<&[PushableCondition]>,
    ) -> Result<LoadDataResult, redis::RedisError> {
        // Lists don't have efficient filtering in Redis
        // Fall back to loading all data
        let data: Vec<String> = redis::cmd("LRANGE").arg(key_prefix).arg(0).arg(-1).query(conn)?;
        self.dataset = DataSet::Complete(DataContainer::List(data));
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
            let _: i32 = redis::cmd("RPUSH").arg(key_prefix).arg(value).query(conn)?;
            
            // Update internal data
            if let DataSet::Complete(DataContainer::List(ref mut list_data)) = &mut self.dataset {
                list_data.push(value.clone());
            } else {
                // Create new list data if not present
                self.dataset = DataSet::Complete(DataContainer::List(vec![value.clone()]));
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
            // LREM removes all occurrences of value from the list
            // Using count = 0 to remove all occurrences
            let _: i32 = redis::cmd("LREM").arg(key_prefix).arg(0).arg(value).query(conn)?;
            
            // Remove from local data cache
            if let DataSet::Complete(DataContainer::List(ref mut list_data)) = &mut self.dataset {
                list_data.retain(|x| x != value);
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
        // First, remove all old data values
        self.delete(conn, key_prefix, old_data)?;

        // Then insert new data values
        self.insert(conn, key_prefix, new_data)?;

        Ok(())
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        matches!(
            operator,
            ComparisonOperator::Equal | ComparisonOperator::Like
        )
    }
}
