use pgrx::pg_sys;
use pgrx::prelude::*;
use std::collections::HashMap;

const VALID_TABLE_TYPES: &[&str] = &["string", "hash", "list", "set", "zset", "stream"];

const KNOWN_SERVER_OPTIONS: &[&str] = &["host_port", "password", "username", "cluster_mode"];
const KNOWN_TABLE_OPTIONS: &[&str] = &[
    "table_type",
    "table_key_prefix",
    "database",
    "ttl",
    "batch_size",
    "strict_key_prefix",
    "join_batch_size",
];

// Register the validator function with text[] SQL type so PostgreSQL can find it
// for the VALIDATOR clause in CREATE FOREIGN DATA WRAPPER.
pgrx::extension_sql!(
    "CREATE OR REPLACE FUNCTION redis_fdw_validator(text[], oid) RETURNS void LANGUAGE c AS 'MODULE_PATHNAME', 'redis_fdw_validator_wrapper';",
    name = "redis_fdw_validator_sql",
);

/// Raw C wrapper for the FDW validator.
/// PostgreSQL converts the options list to a text[] array (key=value format)
/// before calling our function, matching the (text[], oid) SQL signature.
#[no_mangle]
#[pg_guard]
pub unsafe extern "C-unwind" fn redis_fdw_validator_wrapper(
    fcinfo: pg_sys::FunctionCallInfo,
) -> pg_sys::Datum {
    let args = (*fcinfo).args.as_slice(2);

    let catalog =
        pg_sys::Oid::from_datum(args[1].value, args[1].isnull).unwrap_or(pg_sys::InvalidOid);

    let opts = parse_options_list_raw(args[0].value, args[0].isnull);

    if catalog == pg_sys::ForeignServerRelationId {
        validate_server_options(&opts);
    } else if catalog == pg_sys::ForeignTableRelationId {
        validate_table_options(&opts);
    }

    pg_sys::Datum::from(0)
}

/// Required by PostgreSQL's fmgr to discover the function at runtime.
#[no_mangle]
pub extern "C" fn pg_finfo_redis_fdw_validator_wrapper() -> &'static pg_sys::Pg_finfo_record {
    static FINFO: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &FINFO
}

unsafe fn parse_options_list_raw(datum: pg_sys::Datum, is_null: bool) -> HashMap<String, String> {
    let mut map = HashMap::new();

    if is_null {
        return map;
    }

    // PostgreSQL passes options as text[] when SQL type is text[]
    // Each element is formatted as "key=value"
    let array: Option<pgrx::Array<&str>> = pgrx::Array::from_datum(datum, false);
    let Some(array) = array else {
        return map;
    };

    for item in array.iter().flatten() {
        if let Some(eq_pos) = item.find('=') {
            let key = item[..eq_pos].to_string();
            let value = item[eq_pos + 1..].to_string();
            map.insert(key, value);
        }
    }

    map
}

fn validate_server_options(opts: &HashMap<String, String>) {
    if let Some(hp) = opts.get("host_port") {
        if !validation_rules::is_valid_host_port(hp) {
            error!("host_port must be in format 'host:port', got '{}'", hp);
        }
    } else {
        error!("missing required option \"host_port\" for redis_fdw server");
    }

    if let Some(cm) = opts.get("cluster_mode") {
        if cm != "true" && cm != "false" {
            error!("cluster_mode must be \"true\" or \"false\", got '{}'", cm);
        }
    }

    for key in opts.keys() {
        if KNOWN_TABLE_OPTIONS.contains(&key.as_str()) {
            warning!(
                "redis_fdw: option \"{}\" is a table option, not a server option",
                key
            );
        } else if !KNOWN_SERVER_OPTIONS.contains(&key.as_str()) {
            warning!("redis_fdw: unrecognized server option \"{}\"", key);
        }
    }
}

fn validate_table_options(opts: &HashMap<String, String>) {
    if let Some(tt) = opts.get("table_type") {
        if !validation_rules::is_valid_table_type(tt) {
            error!(
                "invalid table_type \"{}\". Must be one of: string, hash, list, set, zset, stream",
                tt
            );
        }
    } else {
        error!("missing required option \"table_type\" for redis_fdw foreign table");
    }

    if let Some(prefix) = opts.get("table_key_prefix") {
        if prefix.is_empty() {
            error!("table_key_prefix must not be empty");
        }
    } else {
        error!("missing required option \"table_key_prefix\" for redis_fdw foreign table");
    }

    if let Some(db) = opts.get("database") {
        if !validation_rules::is_valid_database(db) {
            error!("database must be an integer between 0 and 15, got '{}'", db);
        }
    }

    if let Some(ttl_str) = opts.get("ttl") {
        if !validation_rules::is_valid_ttl(ttl_str) {
            error!(
                "ttl must be a positive integer or -1 (persist), got '{}'",
                ttl_str
            );
        }
    }

    if let Some(bs) = opts.get("batch_size") {
        if !validation_rules::is_valid_batch_size(bs) {
            error!("batch_size must be between 100 and 100000, got '{}'", bs);
        }
    }

    if let Some(jbs) = opts.get("join_batch_size") {
        // Avoid `unwrap_or_else(|_| error!(...))`: pgrx::error! is a
        // diverging ereport(ERROR) that longjmps past Rust unwinding, so
        // invoking it inside a closure can skip destructor cleanup in outer
        // scopes. Use a plain match instead.
        let v: i64 = match jbs.parse() {
            Ok(n) => n,
            Err(_) => {
                error!(
                    "redis_fdw: join_batch_size must be an integer, got '{}'",
                    jbs
                );
            }
        };
        if !(1..=4096).contains(&v) {
            error!(
                "redis_fdw: join_batch_size must be between 1 and 4096 (got {})",
                v
            );
        }
    }

    if let Some(skp) = opts.get("strict_key_prefix") {
        if skp != "true" && skp != "false" {
            error!(
                "strict_key_prefix must be \"true\" or \"false\", got '{}'",
                skp
            );
        }
    }

    for key in opts.keys() {
        if KNOWN_SERVER_OPTIONS.contains(&key.as_str()) {
            warning!(
                "redis_fdw: option \"{}\" is a server option, not a table option",
                key
            );
        } else if !KNOWN_TABLE_OPTIONS.contains(&key.as_str()) {
            warning!("redis_fdw: unrecognized table option \"{}\"", key);
        }
    }
}

pub mod validation_rules {
    pub fn is_valid_table_type(s: &str) -> bool {
        super::VALID_TABLE_TYPES.contains(&s.to_lowercase().as_str())
    }

    pub fn is_valid_ttl(s: &str) -> bool {
        match s.parse::<i64>() {
            Ok(n) => n > 0 || n == -1,
            Err(_) => false,
        }
    }

    pub fn is_valid_batch_size(s: &str) -> bool {
        match s.parse::<usize>() {
            Ok(n) => (100..=100_000).contains(&n),
            Err(_) => false,
        }
    }

    pub fn is_valid_database(s: &str) -> bool {
        match s.parse::<i64>() {
            Ok(n) => (0..=15).contains(&n),
            Err(_) => false,
        }
    }

    pub fn is_valid_host_port(s: &str) -> bool {
        if s.is_empty() {
            return false;
        }
        s.split(',').all(|node| {
            let node = node.trim();
            if node.is_empty() {
                return false;
            }
            // Strip scheme case-insensitively (RFC 3986)
            let node_lower = node.to_ascii_lowercase();
            let without_scheme = if node_lower.starts_with("rediss://") {
                &node[9..]
            } else if node_lower.starts_with("redis://") {
                &node[8..]
            } else {
                node
            };
            // Strip fragment (#insecure) if present
            let without_fragment = without_scheme.split('#').next().unwrap_or(without_scheme);
            // Ensure no path segments are present in the host:port part
            let authority = without_fragment.trim_end_matches('/');
            if authority.is_empty() || authority.contains('/') {
                return false;
            }
            // Must have host:port format with numeric port
            match authority.rfind(':') {
                Some(pos) if pos > 0 && pos < authority.len() - 1 => {
                    authority[pos + 1..].parse::<u16>().is_ok()
                }
                _ => false,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::validation_rules::*;

    #[test]
    fn test_valid_table_types() {
        assert!(is_valid_table_type("string"));
        assert!(is_valid_table_type("hash"));
        assert!(is_valid_table_type("list"));
        assert!(is_valid_table_type("set"));
        assert!(is_valid_table_type("zset"));
        assert!(is_valid_table_type("stream"));
        assert!(is_valid_table_type("STRING"));
        assert!(is_valid_table_type("Hash"));
        assert!(!is_valid_table_type("invalid"));
        assert!(!is_valid_table_type(""));
    }

    #[test]
    fn test_valid_ttl() {
        assert!(is_valid_ttl("3600"));
        assert!(is_valid_ttl("1"));
        assert!(is_valid_ttl("-1"));
        assert!(!is_valid_ttl("0"));
        assert!(!is_valid_ttl("-5"));
        assert!(!is_valid_ttl("abc"));
        assert!(!is_valid_ttl(""));
    }

    #[test]
    fn test_valid_batch_size() {
        assert!(is_valid_batch_size("100"));
        assert!(is_valid_batch_size("50000"));
        assert!(is_valid_batch_size("100000"));
        assert!(!is_valid_batch_size("99"));
        assert!(!is_valid_batch_size("100001"));
        assert!(!is_valid_batch_size("abc"));
    }

    #[test]
    fn test_valid_database() {
        assert!(is_valid_database("0"));
        assert!(is_valid_database("15"));
        assert!(!is_valid_database("16"));
        assert!(!is_valid_database("-1"));
        assert!(!is_valid_database("abc"));
    }

    #[test]
    fn test_valid_host_port() {
        assert!(is_valid_host_port("127.0.0.1:6379"));
        assert!(is_valid_host_port("redis.example.com:6379"));
        assert!(!is_valid_host_port(""));
        assert!(!is_valid_host_port("no-port"));
    }

    #[test]
    fn test_valid_host_port_rediss_scheme() {
        assert!(is_valid_host_port("rediss://redis.example.com:6380"));
        assert!(is_valid_host_port("rediss://127.0.0.1:6380"));
        assert!(is_valid_host_port("rediss://host:6380/#insecure"));
        assert!(is_valid_host_port("redis://127.0.0.1:6379"));
    }

    #[test]
    fn test_valid_host_port_rediss_invalid() {
        assert!(!is_valid_host_port("rediss://"));
        assert!(!is_valid_host_port("rediss://no-port"));
    }

    #[test]
    fn test_valid_host_port_cluster_string() {
        assert!(is_valid_host_port("node1:6379,node2:6379,node3:6379"));
        assert!(is_valid_host_port(
            "rediss://node1:6380,rediss://node2:6380,rediss://node3:6380"
        ));
        assert!(!is_valid_host_port("node1:6379,node2"));
        assert!(!is_valid_host_port("node1:6379,,node3:6379"));
    }

    #[test]
    fn test_valid_host_port_rejects_path_segments() {
        assert!(!is_valid_host_port("redis://host:6379/extra/path"));
        assert!(!is_valid_host_port("rediss://host:6380/db/path"));
    }

    #[test]
    fn test_valid_host_port_case_insensitive_scheme() {
        assert!(is_valid_host_port("REDISS://host:6380"));
        assert!(is_valid_host_port("Redis://host:6379"));
        assert!(is_valid_host_port("REDIS://host:6379"));
    }

    #[test]
    fn test_valid_host_port_rejects_ipv6_without_port() {
        assert!(!is_valid_host_port("[::1]"));
        assert!(!is_valid_host_port("redis://[::1]"));
    }

    #[test]
    fn test_valid_host_port_rejects_non_numeric_port() {
        assert!(!is_valid_host_port("host:abc"));
        assert!(!is_valid_host_port("redis://host:notaport"));
    }
}
