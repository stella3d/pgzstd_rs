use pgrx::prelude::*;
use zstd::{decode_all, encode_all};

pgrx::pg_module_magic!();

#[pg_extern]
pub(crate) fn to_zstd(data: &[u8], level: i32) -> Result<Vec<u8>, &'static str> {
    encode_all(data, level).map_err(|_| "Compression error")
}

#[pg_extern]
pub(crate) fn from_zstd(data: &[u8]) -> Result<Vec<u8>, &'static str> {
    decode_all(data).map_err(|_| "Decompression error")
}

#[pg_extern]
fn from_maybe_zstd(data: &[u8]) -> Result<Vec<u8>, &'static str> {
    match crate::from_zstd(data) {
        Ok(decompressed) => Ok(decompressed),
        Err(_) => Ok(data.to_vec()),
    }
}


#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    const TEST_COMP_OUTPUT_LEVEL19_HEX: &str = "28b52ffd0068650200420a0d0ed0a5312b00e0f0089d4c266793141302d5f54372f1c196ffa16bebffb428e8f4bfcf9fb7fee9f69f83dbeffc17fff1f983aa7f0800407e402b202fa60e11f0c5338e60038c8c6206";
    const TEST_COMP_INPUT_HEX: &str = "00000000000000808001000000000000000200000040000000000000002000000000000000000002000800000000080800010000000200000000000000080800100000000000000000800008000000008000080000000000000040000000000000000000020000000000020000000800000000000000001000000010200000000000000000000000002000001000000000000000000000008000000000000000000000000000080000000000000000000000200000000000000000000000000000008000001002000000000000000088000000000000000000000801000000000020000000410000000000000000000000000000000400000000008000008010040402";

    #[pg_test]
    fn test_to_zstd() {
        let bytes = hex::decode(TEST_COMP_INPUT_HEX).unwrap();
        let expected_output = hex::decode(TEST_COMP_OUTPUT_LEVEL19_HEX).unwrap();

        assert_eq!(expected_output, crate::to_zstd(&bytes, 19).unwrap().as_slice());
    }


    #[pg_test]
    fn test_from_zstd() {
        let compressed_input = hex::decode(TEST_COMP_OUTPUT_LEVEL19_HEX).unwrap();
        let expected_output = hex::decode(TEST_COMP_INPUT_HEX).unwrap();

        assert_eq!(expected_output, crate::from_zstd(&compressed_input).unwrap().as_slice());
    }


    #[pg_test]
    fn test_from_maybe_zstd() {
        let compressed_input = hex::decode(TEST_COMP_OUTPUT_LEVEL19_HEX).unwrap();
        let expected_output = hex::decode(TEST_COMP_INPUT_HEX).unwrap();

        assert_eq!(expected_output, crate::from_maybe_zstd(&expected_output).unwrap().as_slice());
        assert_eq!(expected_output, crate::from_maybe_zstd(&compressed_input).unwrap().as_slice());
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

