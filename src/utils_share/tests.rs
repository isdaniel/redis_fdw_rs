#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema] 
mod tests {
    use pgrx::prelude::*;
    use crate::utils_share::{
        cell::Cell,
        row::Row,
        utils::{cell_to_string, string_from_cstr, string_to_cstr}
    };

    #[pg_test]
    fn test_cell_to_string() {
        // Test with Some cell
        let cell = Cell::String("test_value".to_string());
        let result = cell_to_string(Some(&cell));
        assert_eq!(result, "test_value");

        // Test with None cell
        let result = cell_to_string(None);
        assert_eq!(result, "NULL");
    }

    #[pg_test]
    fn test_string_conversions() {
        let test_string = "Hello, Redis FDW!";
        
        // Test string to CString conversion
        let c_string = string_to_cstr(test_string);
        assert_eq!(c_string.to_str().unwrap(), test_string);
        
        // Test CString to string conversion
        unsafe {
            let back_to_string = string_from_cstr(c_string.as_ptr());
            assert_eq!(back_to_string, test_string);
        }
    }

    #[pg_test]
    fn test_row_operations() {
        let mut row = Row::new();
        
        // Test empty row
        assert_eq!(row.cells.len(), 0);
        
        // Test adding cells
        let cell1 = Cell::String("key1".to_string());
        let cell2 = Cell::String("value1".to_string());
        
        row.push("col1", Some(cell1));
        row.push("col2", Some(cell2));
        
        assert_eq!(row.cells.len(), 2);
        
        // Test cell contents
        if let Some(Cell::String(s)) = &row.cells[0] {
            assert_eq!(s, "key1");
        } else {
            panic!("Expected String cell");
        }
        
        if let Some(Cell::String(s)) = &row.cells[1] {
            assert_eq!(s, "value1");
        } else {
            panic!("Expected String cell");
        }
    }

    #[pg_test]
    fn test_row_with_null_cells() {
        let mut row = Row::new();
        
        // Add a mix of Some and None cells
        let cell1 = Cell::String("value".to_string());
        row.push("col1", Some(cell1));
        row.push("col2", None);
        
        assert_eq!(row.cells.len(), 2);
        assert!(row.cells[0].is_some());
        assert!(row.cells[1].is_none());
        
        // Test cell_to_string with both
        let result1 = cell_to_string(row.cells[0].as_ref());
        let result2 = cell_to_string(row.cells[1].as_ref());
        
        assert_eq!(result1, "value");
        assert_eq!(result2, "NULL");
    }

    #[pg_test]
    fn test_cell_types() {
        // Test different cell types
        let string_cell = Cell::String("test".to_string());
        let i32_cell = Cell::I32(42);
        let i64_cell = Cell::I64(1234567890);
        let bool_cell = Cell::Bool(true);
        
        // Test conversions
        assert_eq!(cell_to_string(Some(&string_cell)), "test");
        assert_eq!(cell_to_string(Some(&i32_cell)), "42");
        assert_eq!(cell_to_string(Some(&i64_cell)), "1234567890");
        assert_eq!(cell_to_string(Some(&bool_cell)), "true");
    }

    #[pg_test]
    fn test_empty_string_handling() {
        let empty_cell = Cell::String("".to_string());
        let result = cell_to_string(Some(&empty_cell));
        assert_eq!(result, "");
    }

    #[pg_test]
    fn test_special_characters_in_strings() {
        let special_chars = "Hello\nWorld\t\"Test\"'Quote'\\Backslash";
        let cell = Cell::String(special_chars.to_string());
        let result = cell_to_string(Some(&cell));
        assert_eq!(result, special_chars);
    }

    #[pg_test]
    fn test_unicode_strings() {
        let unicode_string = "Hello, 世界! 🌍 Redis FDW";
        let cell = Cell::String(unicode_string.to_string());
        let result = cell_to_string(Some(&cell));
        assert_eq!(result, unicode_string);
    }

    #[pg_test]
    fn test_large_numbers() {
        let large_i64 = Cell::I64(i64::MAX);
        let min_i64 = Cell::I64(i64::MIN);
        let large_i32 = Cell::I32(i32::MAX);
        let min_i32 = Cell::I32(i32::MIN);
        
        assert_eq!(cell_to_string(Some(&large_i64)), i64::MAX.to_string());
        assert_eq!(cell_to_string(Some(&min_i64)), i64::MIN.to_string());
        assert_eq!(cell_to_string(Some(&large_i32)), i32::MAX.to_string());
        assert_eq!(cell_to_string(Some(&min_i32)), i32::MIN.to_string());
    }

        #[pg_test]
    fn test_update_and_delete_operations() {
        // Test that UPDATE and DELETE don't crash (even though they're not implemented)
        Spi::run("CREATE FOREIGN DATA WRAPPER redis_wrapper HANDLER redis_fdw_handler;").unwrap();
        Spi::run("
            CREATE SERVER redis_server 
            FOREIGN DATA WRAPPER redis_wrapper
            OPTIONS (host_port '127.0.0.1:8899');
        ").unwrap();
        
        Spi::run("
            CREATE FOREIGN TABLE test_update_delete (key text, value text) 
            SERVER redis_server
            OPTIONS (
                database '0',
                table_type 'hash',
                table_key_prefix 'test:'
            );
        ").unwrap();
        
        // These should not crash, even though they don't actually do anything
        let update_result = std::panic::catch_unwind(|| {
            Spi::run("UPDATE test_update_delete SET value = 'new_value' WHERE key = 'some_key';").unwrap();
        });
        
        let delete_result = std::panic::catch_unwind(|| {
            Spi::run("DELETE FROM test_update_delete WHERE key = 'some_key';").unwrap();
        });
        
        assert!(update_result.is_ok());
        assert!(delete_result.is_ok());
        
        // Clean up
        Spi::run("DROP FOREIGN TABLE test_update_delete;").unwrap();
        Spi::run("DROP SERVER redis_server CASCADE;").unwrap();
        Spi::run("DROP FOREIGN DATA WRAPPER redis_wrapper CASCADE;").unwrap();
    }
}
