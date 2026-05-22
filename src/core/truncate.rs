use crate::{
    core::connection_factory::{RedisConnectionConfig, RedisConnectionFactory},
    core::state_manager::is_multi_key_pattern,
    utils::helpers::get_foreign_table_options,
};
use pgrx::prelude::*;

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn exec_foreign_truncate(
    rels: *mut pg_sys::List,
    _behavior: pg_sys::DropBehavior::Type,
    _restart_seqs: bool,
) {
    log!("---> exec_foreign_truncate");

    if rels.is_null() {
        return;
    }

    pgrx::memcx::current_context(|mcx| {
        let rel_list = pgrx::list::List::<*mut std::ffi::c_void>::downcast_ptr_in_memcx(rels, mcx)
            .expect("Failed to downcast rels list");

        for rel_ptr in rel_list.iter() {
            let relation = *rel_ptr as pg_sys::Relation;
            if relation.is_null() {
                continue;
            }
            let relid = (*relation).rd_id;
            let options = get_foreign_table_options(relid);

            let config = match RedisConnectionConfig::from_options(&options) {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to create Redis config for truncate: {}", e);
                }
            };

            let mut conn = match RedisConnectionFactory::create_connection_with_retry(&config) {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to connect to Redis for truncate: {}", e);
                }
            };

            let conn_like = conn.as_connection_like_mut();
            let key_prefix = options.get("table_key_prefix").cloned().unwrap_or_default();

            if is_multi_key_pattern(&key_prefix) {
                let mut cursor: u64 = 0;
                loop {
                    pgrx::check_for_interrupts!();
                    let (new_cursor, keys): (u64, Vec<String>) = match redis::cmd("SCAN")
                        .arg(cursor)
                        .arg("MATCH")
                        .arg(&key_prefix)
                        .arg("COUNT")
                        .arg(1000u32)
                        .query(conn_like)
                    {
                        Ok(r) => r,
                        Err(e) => {
                            error!("Redis SCAN error during truncate: {}", e);
                        }
                    };

                    if !keys.is_empty() {
                        let mut pipe = redis::pipe();
                        for key in &keys {
                            pipe.cmd("UNLINK").arg(key);
                        }
                        if let Err(e) = pipe.query::<Vec<redis::Value>>(conn_like) {
                            error!("Redis UNLINK pipeline failed during truncate: {}", e);
                        }
                    }

                    cursor = new_cursor;
                    if cursor == 0 {
                        break;
                    }
                }
            } else if let Err(e) = redis::cmd("UNLINK")
                .arg(&key_prefix)
                .query::<i64>(conn_like)
            {
                error!("Redis UNLINK failed for key '{}': {}", key_prefix, e);
            }
        }
    });
}
