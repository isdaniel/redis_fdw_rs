
use crate::redis_fdw::{
    pushdown_types::{ComparisonOperator, PushableCondition},
    tables::interface::RedisTableOperations,
    types::{DataSet, DataContainer, LoadDataResult},
};

/// Redis Sorted Set table type
#[derive(Debug, Clone, Default)]
pub struct RedisZSetTable {
    pub dataset: DataSet,
}

impl RedisZSetTable {
    pub fn new() -> Self {
        Self { 
            dataset: DataSet::Empty,
        }
    }
}

impl RedisTableOperations for RedisZSetTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        _conditions: Option<&[PushableCondition]>,
    ) -> Result<LoadDataResult, redis::RedisError> {
        // ZSets could support score-based range queries in the future
        // For now, fall back to loading all data
        let result: Vec<(String, f64)> = redis::cmd("ZRANGE")
            .arg(key_prefix)
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query(conn)?;
        self.dataset = DataSet::Complete(DataContainer::ZSet(result));
        Ok(LoadDataResult::LoadedToInternal)
    }

    fn get_dataset(&self) -> &DataSet {
        &self.dataset
    }

    /// Override the default get_row implementation to handle zset-specific filtered data format
    fn get_row(&self, index: usize) -> Option<Vec<String>> {
        match &self.dataset {
            DataSet::Filtered(data) => {
                // ZSet filtered data is stored as [member1, score1, member2, score2, ...]
                let data_index = index * 2;
                if data_index + 1 < data.len() {
                    Some(vec![
                        data[data_index].clone(),
                        data[data_index + 1].clone(),
                    ])
                } else {
                    None
                }
            },
            _ => self.dataset.get_row(index),
        }
    }

    /// Override data_len to handle zset-specific filtered data format
    fn data_len(&self) -> usize {
        match &self.dataset {
            DataSet::Filtered(data) => data.len() / 2, // member-score pairs
            _ => self.dataset.len(),
        }
    }

    fn insert(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        // Expect data in pairs: [member1, score1, member2, score2, ...]
        let items: Vec<(f64, String)> = data
            .chunks(2)
            .filter_map(|chunk| {
                if chunk.len() == 2 {
                    if let Ok(score) = chunk[1].parse::<f64>() {
                        Some((score, chunk[0].clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        for (score, member) in &items {
            let _: () = redis::cmd("ZADD").arg(key_prefix).arg(*score).arg(member).query(conn)?;
            
            // Update internal data
            if let DataSet::Complete(DataContainer::ZSet(ref mut zset_data)) = &mut self.dataset {
                // Remove existing member if it exists and add new one
                zset_data.retain(|(m, _)| m != member);
                zset_data.push((member.clone(), *score));
            } else {
                // Create new zset data if not present
                let new_data = vec![(member.clone(), *score)];
                self.dataset = DataSet::Complete(DataContainer::ZSet(new_data));
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
        for member in data {
            let _: i32 = redis::cmd("ZREM").arg(key_prefix).arg(member).query(conn)?;
            
            // Remove from local data
            if let DataSet::Complete(DataContainer::ZSet(ref mut zset_data)) = &mut self.dataset {
                zset_data.retain(|(m, _)| m != member);
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
        // For sorted sets, update means remove old members and add new ones
        if !old_data.is_empty() {
            self.delete(conn, key_prefix, old_data)?;
        }
        self.insert(conn, key_prefix, new_data)?;
        Ok(())
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        matches!(operator, ComparisonOperator::Equal | ComparisonOperator::In)
    }
}
