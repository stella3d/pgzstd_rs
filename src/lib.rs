use std::io::prelude::*;
use pgrx::prelude::*;
use zstd::{decode_all, encode_all};

pgrx::pg_module_magic!();

#[pg_extern]
fn hello_pgzstd_rs() -> &'static str {
    "Hello, pgzstd_rs"
}

#[pg_extern]
fn to_zstd(data: &[u8], level: i32) -> Result<Vec<u8>, &'static str> {
    encode_all(data, level).map_err(|_| "Compression error")
}

#[pg_extern]
fn from_zstd(data: &[u8]) -> Result<Vec<u8>, &'static str> {
    decode_all(data).map_err(|_| "Decompression error")
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_pgzstd_rs() {
        assert_eq!("Hello, pgzstd_rs", crate::hello_pgzstd_rs());
    }

}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
