/// Data loading module for Redis FDW
///
/// This module handles the loading of data from Redis with optimizations
/// like pushdown conditions and limit/offset operations.
use crate::{
    core::connection::RedisConnectionType,
    query::{limit::LimitOffsetInfo, pushdown_types::PushdownAnalysis},
    tables::types::{LoadDataResult, RedisTableType},
};
use pgrx::prelude::*;

/// Data loader responsible for loading data from Redis with optimizations
pub struct RedisDataLoader<'a> {
    table_type: &'a mut RedisTableType,
    table_key_prefix: &'a str,
    pushdown_analysis: Option<&'a PushdownAnalysis>,
}

impl<'a> RedisDataLoader<'a> {
    pub fn new(
        table_type: &'a mut RedisTableType,
        table_key_prefix: &'a str,
        pushdown_analysis: Option<&'a PushdownAnalysis>,
    ) -> Self {
        Self {
            table_type,
            table_key_prefix,
            pushdown_analysis,
        }
    }

    /// Load data from Redis, applying pushdown optimizations if available
    pub fn load_data(&mut self, connection: &mut RedisConnectionType) -> Result<(), String> {
        let conn_like = connection.as_connection_like_mut();

        if let Some(analysis) = self.pushdown_analysis {
            if analysis.has_optimizations() {
                // Apply pushdown conditions using the table type's unified method
                match self.table_type.load_data(
                    conn_like,
                    self.table_key_prefix,
                    Some(&analysis.pushable_conditions),
                    analysis
                        .limit_offset
                        .as_ref()
                        .unwrap_or(&LimitOffsetInfo::default()),
                ) {
                    Ok(LoadDataResult::PushdownApplied(filtered_data)) => {
                        // Apply LIMIT/OFFSET if specified
                        if let Some(ref limit_offset) = analysis.limit_offset {
                            if limit_offset.has_constraints() {
                                // Update the internal data with the limited result
                                self.table_type.set_filtered_data(filtered_data);
                            }
                        }
                        log!(
                            "Pushdown optimization applied, loaded {} items with LIMIT/OFFSET",
                            self.table_type.data_len()
                        );
                        return Ok(());
                    }
                    Err(e) => {
                        error!("Pushdown failed, falling back to full scan: {:?}", e);
                    }
                    _ => {
                        return Ok(());
                    }
                }
            } else if analysis.has_limit_pushdown() {
                // Only LIMIT/OFFSET pushdown, no WHERE conditions
                log!("Applying LIMIT/OFFSET only pushdown");
                if let Some(ref limit_offset) = analysis.limit_offset {
                    let _ = self.table_type.load_data(
                        conn_like,
                        self.table_key_prefix,
                        None,
                        limit_offset,
                    );
                    return Ok(());
                }
            }
        }

        // Fall back to loading all data without pushdown
        match self.table_type.load_data(
            conn_like,
            self.table_key_prefix,
            None,
            &LimitOffsetInfo::default(),
        ) {
            Ok(_) => {
                log!("Data loaded without pushdown optimizations");
                Ok(())
            }
            Err(e) => {
                let error_msg = format!("Failed to load data: {:?}", e);
                Err(error_msg)
            }
        }
    }
}
