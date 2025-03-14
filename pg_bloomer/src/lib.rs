use std::sync::{Arc, RwLock};
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

    pub fn contains_batch<'a>(&self, items: impl Iterator<Item = &'a [u8]>) -> Vec<bool> {
        // Acquire lock once for entire batch
        //let filter = self.filter.read().expect("Failed to read filter");
        items
            .map(|elem| self.filter.contains(elem))
            .collect()
    }

    pub fn contains_optional_batch<'a>(&self, items: impl Iterator<Item = Option<&'a [u8]>>) -> Vec<bool> {
        // Acquire lock once for entire batch
        //let filter = self.filter.read().expect("Failed to read filter");
        items
            .map(|elem| match elem {
                Some(item) => self.filter.contains(item),
                None => false,
            })
            .collect()
    }

    pub fn contains_non_null_batch(&self, items: &[&[u8]]) -> Vec<bool> {
        items.iter()
            .map(|&elem| self.filter.contains(elem))
            .collect()
    }
    
    pub fn insert_batch<'a>(&mut self, items: impl Iterator<Item = Option<&'a [u8]>>) -> i32 {
        // Acquire lock once for entire batch
        //let mut filter = self.filter.write().expect("Failed to write to filter");
        
        let mut insert_count  = 0;
        for elem in items {
            if let Some(item) = elem {
                self.filter.insert(item);
                insert_count += 1;
            }
        }
        
        insert_count
    }
}


/// A handle to a bloom filter that doesn't require locks for lookup
#[derive(Clone)]
pub struct BloomFilterHandle {
    filter: Arc<PgBloomFilter>,
}

impl BloomFilterHandle {
    pub fn contains(&self, item: &[u8]) -> bool {
        self.filter.contains(item)
    }
    
    pub fn contains_batch<'a>(&self, items: impl Iterator<Item = &'a [u8]>) -> Vec<bool> {
        self.filter.contains_batch(items)
    }

    pub fn contains_optional_batch<'a>(&self, items: impl Iterator<Item = Option<&'a [u8]>>) -> Vec<bool> {
        self.filter.contains_optional_batch(items)
    }
}


// Storage for actual filter instances
pub(crate) static FILTER_REGISTRY: Lazy<RwLock<Vec<Arc<PgBloomFilter>>>> = 
    Lazy::new(|| RwLock::new(Vec::new()));

// Thread-local cache of filter handles
thread_local! {
    static HANDLE_CACHE: std::cell::RefCell<Vec<Option<BloomFilterHandle>>> = std::cell::RefCell::new(Vec::new());
}

#[pg_extern]
pub fn new_bloom_filter(false_positive_rate: f64, expected_item_count: i64) -> i32 {
    let filter = BloomFilter::with_false_pos(false_positive_rate)
        .expected_items(expected_item_count as usize);
    
    let filter = Arc::new(PgBloomFilter { filter });
    
    // Register the filter
    let mut registry = FILTER_REGISTRY.write().expect("Failed to acquire write lock");
    registry.push(filter);
    registry.len() as i32 - 1
}

#[pg_extern]
pub fn new_bloom_filter_from_items<'a>(false_positive_rate: f64, items: pgrx::Array<&'a [u8]>) -> i32 {
    let mut filter = BloomFilter::with_false_pos(false_positive_rate)
        .expected_items(items.len());

    for item in items.iter() {
        if let Some(item) = item {
            filter.insert(item);
        }
    }
    
    let filter = Arc::new(PgBloomFilter { filter });
    
    // Atomically increment the counter without lock
    //let filter_id = NEXT_FILTER_ID.fetch_add(1, Ordering::SeqCst);
    
    // Register the filter
    let mut registry = FILTER_REGISTRY.write().expect("Failed to acquire write lock");
    registry.push(filter);
    registry.len() as i32 - 1
}

/// Get a handle to a bloom filter without locks after the first access
pub(crate) fn get_bloom_filter_handle(filter_id_int: i32) -> Option<BloomFilterHandle> {
    let filter_id = filter_id_int as usize;

    // Check the thread-local cache first
    let mut found_in_cache = false;
    let handle = HANDLE_CACHE.with(|cache| {
        let cache = cache.borrow();
        if filter_id < cache.len() {
            if let Some(handle) = &cache[filter_id] {
                found_in_cache = true;
                return Some(handle.clone());
            }
        }
        None
    });
    
    if found_in_cache {
        return handle;
    }
    
    // If not in cache, get from registry (this requires a lock)
    let registry = FILTER_REGISTRY.read().expect("Failed to acquire registry read lock");
    
    let handle = if (filter_id) < registry.len() {
        Some(BloomFilterHandle {
            filter: registry[filter_id].clone(),
        })
    } else {
        None
    };
    
    // Cache the handle for future lock-free access
    if let Some(handle) = &handle {
        HANDLE_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if (filter_id) >= cache.len() {
                cache.resize_with(filter_id + 1, || None);
            }
            cache[filter_id] = Some(handle.clone());
        });
    }
    
    handle
}

// Optimized contains that uses handles
#[pg_extern]
pub fn bloom_filter_contains(filter_id: i32, item: &[u8]) -> bool {
    match get_bloom_filter_handle(filter_id) {
        Some(handle) => handle.contains(item),
        None => false,
    }
}

#[pg_extern]
pub fn bloom_filter_contains_batch(filter_id: i32, items: pgrx::Array<&[u8]>) -> Vec<bool> {
    match get_bloom_filter_handle(filter_id) {
        Some(handle) => {
            handle.contains_optional_batch(items.iter())
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
