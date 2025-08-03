use crate::{
    query::{
        pushdown_types::{ComparisonOperator, PushableCondition},
        scan_ops::{extract_scan_conditions},
    },
    tables::{
        interface::RedisTableOperations,
        types::{DataSet, LoadDataResult},
    },
};

/// Redis Stream table type supporting large data sets with streaming and pagination
/// 
/// Redis Streams are append-only log data structures that support:
/// - Time-ordered entry IDs (timestamp-sequence format)
/// - Field-value pairs per entry (like hash fields)
/// - Range queries by time or ID
/// - Consumer groups for distributed processing
/// - Efficient pagination with COUNT and ID cursors
#[derive(Debug, Clone, Default)]
pub struct RedisStreamTable {
    pub dataset: DataSet,
    /// Last processed stream ID for pagination
    pub last_id: Option<String>,
    /// Batch size for streaming operations to handle large data sets
    pub batch_size: usize,
}

impl RedisStreamTable {
    pub fn new() -> Self {
        Self {
            dataset: DataSet::Empty,
            last_id: None,
            batch_size: 1000, // Default batch size for large data sets
        }
    }

    /// Create a new stream table with custom batch size for large data set handling
    pub fn with_batch_size(batch_size: usize) -> Self {
        Self {
            dataset: DataSet::Empty,
            last_id: None,
            batch_size,
        }
    }

    /// Load data using XRANGE with streaming support for large data sets
    fn load_with_xrange(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        start_id: &str,
        end_id: &str,
        count: Option<usize>,
    ) -> Result<LoadDataResult, redis::RedisError> {
        let count = count.unwrap_or(self.batch_size);
        
        // Use XRANGE to get stream entries
        let entries: Vec<(String, Vec<(String, String)>)> = if let Some(c) = Some(count) {
            redis::cmd("XRANGE").arg(key_prefix).arg(start_id).arg(end_id).arg("COUNT").arg(c).query(conn)?
        } else {
            redis::cmd("XRANGE").arg(key_prefix).arg(start_id).arg(end_id).query(conn)?
        };

        if entries.is_empty() {
            return Ok(LoadDataResult::Empty);
        }

        // Store last stream ID for pagination before processing entries
        let last_id = entries.last().map(|(id, _)| id.clone());

        let mut data = Vec::new();
        for (stream_id, fields) in entries {
            // Create a row with stream_id followed by field-value pairs
            let mut row = vec![stream_id];
            for (field, value) in fields {
                row.push(field);
                row.push(value);
            }
            data.push(row.join("\t"));
        }

        // Store last stream ID for pagination
        if let Some(id) = last_id {
            self.last_id = Some(id);
        }

        // Store data as filtered entries
        self.dataset = DataSet::Filtered(data);
        Ok(LoadDataResult::LoadedToInternal)
    }

    /// Load data with stream-specific optimizations and pushdown conditions
    fn load_with_stream_optimization(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        scan_conditions: &crate::query::scan_ops::ScanConditions,
    ) -> Result<LoadDataResult, redis::RedisError> {
        // Extract time-based conditions for ID range queries
        let mut start_id = "-".to_string(); // Start from beginning
        let mut end_id = "+".to_string();   // Go to end
        let mut count = Some(self.batch_size);

        // Check for time-based or ID-based conditions
        for condition in &scan_conditions.exact_conditions {
            match condition.operator {
                ComparisonOperator::Equal => {
                    // Exact ID match - use as both start and end
                    start_id = condition.value.clone();
                    end_id = condition.value.clone();
                    count = Some(1); // Only need one entry
                }
                ComparisonOperator::NotEqual => {
                    // For streams, not equal is less useful but we can handle it
                    // by loading all data except this specific ID
                    continue;
                }
                _ => {} // Other operators not directly applicable to stream IDs
            }
        }

        self.load_with_xrange(conn, key_prefix, &start_id, &end_id, count)
    }

    /// Get the next batch of data for streaming large data sets
    pub fn load_next_batch(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
    ) -> Result<LoadDataResult, redis::RedisError> {
        let start_id = if let Some(ref last_id) = self.last_id {
            format!("({}", last_id) // Exclusive start from last processed ID
        } else {
            "-".to_string() // Start from beginning
        };

        self.load_with_xrange(conn, key_prefix, &start_id, "+", Some(self.batch_size))
    }

    /// Add a new entry to the stream
    pub fn add_entry(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        id: &str, // Use "*" for auto-generated ID
        fields: &[(String, String)],
    ) -> Result<String, redis::RedisError> {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(key_prefix).arg(id);
        
        // Add field-value pairs
        for (field, value) in fields {
            cmd.arg(field).arg(value);
        }
        
        let stream_id: String = cmd.query(conn)?;
        Ok(stream_id)
    }

    /// Get stream length
    pub fn get_stream_length(
        &self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
    ) -> Result<usize, redis::RedisError> {
        let len: usize = redis::cmd("XLEN").arg(key_prefix).query(conn)?;
        Ok(len)
    }
}

impl RedisTableOperations for RedisStreamTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<LoadDataResult, redis::RedisError> {
        if let Some(conditions) = conditions {
            let scan_conditions = extract_scan_conditions(conditions);

            // For streams, we can optimize range queries and pattern matching
            if scan_conditions.has_optimizable_conditions() {
                return self.load_with_stream_optimization(conn, key_prefix, &scan_conditions);
            }
        }

        // Fallback: Load recent entries with default batch size
        self.load_with_xrange(conn, key_prefix, "-", "+", Some(self.batch_size))
    }

    fn get_dataset(&self) -> &DataSet {
        &self.dataset
    }

    fn data_len(&self) -> usize {
        self.dataset.len()
    }

    fn get_row(&self, index: usize) -> Option<Vec<String>> {
        match &self.dataset {
            DataSet::Filtered(entries) => {
                entries.get(index).map(|entry| {
                    // Parse tab-separated entry back to fields
                    entry.split('\t').map(|s| s.to_string()).collect()
                })
            }
            DataSet::Complete(container) => container.get_row(index),
            DataSet::Empty => None,
        }
    }

    fn insert(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        if data.is_empty() {
            return Ok(());
        }

        // For streams, we expect data format: [id?, field1, value1, field2, value2, ...]
        // If first element looks like a stream ID, use it; otherwise auto-generate
        let (id, field_start) = if data.len() >= 1 && (data[0] == "*" || data[0].contains('-')) {
            (data[0].as_str(), 1)
        } else {
            ("*", 0) // Auto-generate ID
        };

        // Convert remaining data to field-value pairs
        let mut fields = Vec::new();
        for chunk in data[field_start..].chunks(2) {
            if chunk.len() == 2 {
                fields.push((chunk[0].clone(), chunk[1].clone()));
            }
        }

        if !fields.is_empty() {
            self.add_entry(conn, key_prefix, id, &fields)?;
        }

        Ok(())
    }

    fn delete(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        if data.is_empty() {
            return Ok(());
        }

        // For streams, data should contain stream IDs to delete
        let ids: Vec<&String> = data.iter().collect();
        let _deleted_count: usize = redis::cmd("XDEL").arg(key_prefix).arg(&ids).query(conn)?;

        Ok(())
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        // Redis Streams support range queries based on stream IDs (timestamps)
        matches!(
            operator,
            ComparisonOperator::Equal
                | ComparisonOperator::NotEqual
                | ComparisonOperator::Like
        )
    }
}