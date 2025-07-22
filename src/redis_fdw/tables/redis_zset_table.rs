
use crate::redis_fdw::{
    pushdown::{ComparisonOperator, PushableCondition},
    tables::interface::RedisTableOperations,
};

/// Redis Sorted Set table type
#[derive(Debug, Clone, Default)]
pub struct RedisZSetTable {
    pub data: Vec<(String, f64)>, // (member, score)
}

impl RedisZSetTable {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

impl RedisTableOperations for RedisZSetTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        _conditions: Option<&[PushableCondition]>,
    ) -> Result<Option<Vec<String>>, redis::RedisError> {
        // ZSets could support score-based range queries in the future
        // For now, fall back to loading all data
        // Load all data into internal storage
        let result: Vec<(String, f64)> = redis::cmd("ZRANGE").arg(key_prefix).arg(0).arg(-1).arg("WITHSCORES").query(conn)?;
        self.data = result;
        Ok(None)
    }

    fn data_len(&self, filtered_data: Option<&[String]>) -> usize {
        if let Some(filtered_data) = filtered_data {
            filtered_data.len() / 2 // member-score pairs
        } else {
            self.data.len()
        }
    }

    fn get_row(&self, index: usize, filtered_data: Option<&[String]>) -> Option<Vec<String>> {
        if let Some(filtered_data) = filtered_data {
            // ZSet data is stored as [member1, score1, member2, score2, ...]
            let data_index = index * 2;
            if data_index + 1 < filtered_data.len() {
                Some(vec![
                    filtered_data[data_index].clone(),
                    filtered_data[data_index + 1].clone(),
                ])
            } else {
                None
            }
        } else {
            self.data
                .get(index)
                .map(|(member, score)| vec![member.clone(), score.to_string()])
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
            self.data.push((member.clone(), *score));
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
            self.data.retain(|(m, _)| m != member);
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
