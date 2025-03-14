use std::sync::RwLock;
use once_cell::sync::Lazy;

use pgrx::prelude::*;
use fastbloom::BloomFilter;
use serde::{Deserialize, Serialize};

pgrx::pg_module_magic!();

/// Bloom filter implementation for PostgreSQL
#[derive(Serialize, Deserialize)]
pub struct PgBloomFilter {
    filter: BloomFilter,
}

impl PgBloomFilter {
    #[inline]
    pub fn contains(&self, item: &[u8]) -> bool {
        self.filter.contains(item)
    }

    #[inline]
    pub fn insert(&mut self, item: &[u8]) -> bool {
        self.filter.insert(item)
    }
}

// Use RwLock instead of Mutex for better read concurrency
pub(crate) static FILTERS: Lazy<RwLock<Vec<PgBloomFilter>>> = Lazy::new(|| RwLock::new(Vec::new()));

#[pg_extern]
pub fn new_bloom_filter(false_positive_rate: f64, expected_item_count: i64) -> i32 {
    let filter = BloomFilter::with_false_pos(false_positive_rate)
        .expected_items(expected_item_count as usize);
    
    let mut filters = FILTERS.write().expect("Failed to acquire write lock for FILTERS");
    filters.push(PgBloomFilter { filter });
    (filters.len() - 1) as i32
}

#[pg_extern]
pub fn bloom_filter_insert(filter_id: i32, item: &[u8]) -> bool {
    let f_index = filter_id as usize;
    let mut filters = FILTERS.write().expect("Failed to acquire write lock for FILTERS");
    
    match filters.get_mut(f_index) {
        Some(bf) => { bf.insert(item) },
        None => false,
    }
}


#[pg_extern]
pub fn bloom_filter_insert_batch(filter_id: i32, items: pgrx::Array<&[u8]>) -> bool {
    let f_index = filter_id as usize;
    let mut filters = FILTERS.write().expect("Failed to acquire write lock for FILTERS");
    
    match filters.get_mut(f_index) {
        Some(bf) => {
            let mut ret = false;
            for elem in items.iter() {
                match elem {
                    Some(item) => ret |= bf.insert(item),
                    None => continue,
                }
            }
            ret
        },
        None => false,
    }
}

#[pg_extern]
pub fn bloom_filter_contains(filter_id: i32, item: &[u8]) -> bool {
    let f_index = filter_id as usize;
    let filters = FILTERS.read().expect("Failed to acquire read lock for FILTERS");

    match filters.get(f_index) {
        Some(bf) => bf.contains(item),
        None => false,
    }
}

#[pg_extern]
pub fn bloom_filter_contains_batch(filter_id: i32, items: pgrx::Array<&[u8]>) -> Vec<bool> {
    let f_index = filter_id as usize;
    let filters = FILTERS.read().expect("Failed to acquire read lock for FILTERS");

    match filters.get(f_index) {
        Some(bf) => {
            items
                .iter()
                .map(|elem| {
                    match elem {
                        Some(item) => bf.contains(item),
                        None => false,
                    }
                })
                .collect()
        },
        None => vec![false; items.len()],
    }    
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    /* 
    use pgrx::prelude::*;
    #[pg_test]
    fn test_bloom_filter() {
        let filter = crate::bloom_filter_new(1000);
        let filter = crate::bloom_filter_insert(filter, b"hello");
        let filter = crate::bloom_filter_insert(filter, b"world");
        
        assert!(crate::bloom_filter_contains(filter, b"hello"));
        assert!(crate::bloom_filter_contains(filter, b"world"));
        assert!(!crate::bloom_filter_contains(filter, b"test"));
    }
    */
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
