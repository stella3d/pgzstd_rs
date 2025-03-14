use std::sync::{Arc, RwLock};
use once_cell::sync::Lazy;

use pgrx::{prelude::*, spi::Query};
use fastbloom::BloomFilter;
use serde::{Deserialize, Serialize};

use rayon::prelude::*;

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

    pub fn contains_optional_batch<'a, T: Iterator<Item = Option<&'a [u8]>>>(&self, items: T) -> Vec<bool> {
        items
            .map(|elem| match elem {
                Some(item) => self.filter.contains(item),
                None => false,
            })
            .collect()
    }

    pub fn contains_batch_parallel<'a, T: Iterator<Item = Option<&'a [u8]>> + Send>(&self, items: T) -> Vec<bool> {
        items.par_bridge()
            .into_par_iter()
            .map(|elem| match elem {
                Some(item) => self.filter.contains(item),
                None => false,
            })
            .collect()
    }

    pub fn contains_batch_par<'a>(&self, items: Vec<Option<&'a [u8]>>) -> Vec<bool> {
        items
            .into_par_iter()
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
    #[inline]
    pub fn contains(&self, item: &[u8]) -> bool {
        self.filter.contains(item)
    }
    #[inline]
    pub fn contains_batch<'a>(&self, items: impl Iterator<Item = &'a [u8]>) -> Vec<bool> {
        self.filter.contains_batch(items)
    }
    #[inline]
    pub fn contains_optional_batch<'a>(&self, items: impl Iterator<Item = Option<&'a [u8]>>) -> Vec<bool> {
        self.filter.contains_optional_batch(items)
    }
    #[inline]
    pub fn contains_batch_parallel<'a, T: Iterator<Item = Option<&'a [u8]>> + Send>(&self, items: T) -> Vec<bool> {
        self.filter.contains_batch_parallel(items)
    }
    #[inline]
    pub fn contains_batch_par<'a>(&self, items: Vec<Option<&'a [u8]>>) -> Vec<bool> {
        self.filter.contains_batch_par(items)
    }
}


// Storage for actual filter instances
pub(crate) static FILTER_REGISTRY: Lazy<RwLock<Vec<Arc<PgBloomFilter>>>> = 
    Lazy::new(|| RwLock::new(Vec::new()));

// Thread-local cache of filter handles
thread_local! {
    static HANDLE_CACHE: std::cell::RefCell<Vec<BloomFilterHandle>> = std::cell::RefCell::new(Vec::new());
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
    let filter = BloomFilter::with_false_pos(false_positive_rate).items(items.iter());

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
    let cached_handle = HANDLE_CACHE.with(|cache| {
        let cache = cache.borrow();
        if filter_id < cache.len() {
            Some(cache[filter_id].clone())
        } else {
            None
        }
    });

    if let Some(handle) = cached_handle {
        return Some(handle); // Successfully retrieved from cache
    }

    // Cache miss - get from registry with lock
    let registry_guard = FILTER_REGISTRY.read()
        .expect("Failed to acquire registry read lock");
    
    if filter_id >= registry_guard.len() {
        // No filter with this ID exists
        info!("Filter ID {} not found in registry (len: {})", filter_id, registry_guard.len());
        return None;
    }
    
    // Create a new handle from the registry entry
    let handle = BloomFilterHandle {
        filter: registry_guard[filter_id].clone(),
    };
    
    // Cache the handle for future access
    HANDLE_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        
        // Make sure the cache is large enough
        if filter_id >= cache.len() {
            info!("Resizing cache from {} to {}", cache.len(), filter_id + 1);
            cache.resize_with(filter_id + 1, || handle.clone());
        }
        
        // Store the actual handle
        cache[filter_id] = handle.clone();
    });
    
    // Return the newly created handle
    Some(handle)
}

// Add this at the top with other imports
use pgrx::info;

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

#[pg_extern]
pub fn bf_contains_parallel(filter_id: i32, items: pgrx::Array<&[u8]>) -> Vec<bool> {
    match get_bloom_filter_handle(filter_id) {
        Some(handle) => {
            let owned_items: Vec<Option<Vec<u8>>> = items
                .iter()
                .map(|item| item.map(|s| s.to_owned()))
                .collect();

            owned_items
                .par_iter()
                .map(|item| match item {
                    Some(item) => handle.contains(item),
                    None => false,
                })
                .collect()
        },
        None => vec![false; items.len()],
    }
}


#[pg_extern]
pub fn bf_contains_parallel_slice(filter_id: i32, items: pgrx::Array<&[u8]>) -> Vec<bool> {
    match get_bloom_filter_handle(filter_id) {
        Some(handle) => {
            let owned_items: Vec<Option<&[u8]>> = items
                .iter()
                .map(|item| item)
                .collect();

            owned_items
                .par_iter()
                .map(|item| match item {
                    Some(item) => handle.contains(item),
                    None => false,
                })
                .collect()
        },
        None => vec![false; items.len()],
    }
}

#[pg_extern]
pub fn bf_contains_parallel_nanos(filter_id: i32, items: pgrx::Array<&[u8]>) -> i64 {
    let start = std::time::Instant::now();

    match get_bloom_filter_handle(filter_id) {
        Some(handle) => {
            let owned_items: Vec<Option<&[u8]>> = items
                .iter()
                .map(|item| item)
                .collect();

            let _res: Vec<bool> = handle.contains_batch_par(owned_items);

            let duration = start.elapsed();
            let duration_nanos = duration.as_nanos() as i64;
            duration_nanos
        },
        None => 0,
    }
}

#[pg_extern]
pub fn bf_contains_nanos(filter_id: i32, items: pgrx::Array<&[u8]>) -> i64 {
    let start = std::time::Instant::now();

    match get_bloom_filter_handle(filter_id) {
        Some(handle) => {
            let _res = handle.contains_optional_batch(items.iter());
            let duration = start.elapsed();
            let duration_nanos = duration.as_nanos() as i64;
            duration_nanos
        },
        None => 0,
    }
}

#[pg_extern]
pub fn bloom_filter_serialize(filter_id: i32) -> Option<Vec<u8>> {
    let registry = FILTER_REGISTRY.read().expect("Failed to acquire registry read lock");

    match registry.get(filter_id as usize) {
        Some(filter) => {
            match postcard::to_allocvec(filter.as_ref()) {
                Ok(serialized) => Some(serialized),
                Err(_) => None,
            }
        },
        None => None,
    }
}

// A special type to hold a pre-bound bloom filter handle
struct BoundBloomFilter {
    handle: BloomFilterHandle,
}

#[pg_extern(volatile)]
pub fn create_bound_filter_function(filter_id: i32) -> String {
    // Get the filter handle once
    match get_bloom_filter_handle(filter_id) {
        Some(handle) => {
            // Create a new bound filter and store it
            let bound_filter = Box::new(BoundBloomFilter { handle });
            let function_name = format!("bf_contains_bound_{}", filter_id);
            
            // Leak the bound filter so it lives for the program duration
            // This is intentional as we want this to persist as long as PostgreSQL is running
            let bound_ptr = Box::into_raw(bound_filter) as usize;
            
            // Register functions using the bound pointer
            pgrx::Spi::connect(|client| {
                let batch_sql = format!(
                    "CREATE OR REPLACE FUNCTION {}_batch(items bytea[]) RETURNS boolean[] AS $$
                        SELECT pg_bloomer._bf_contains_bound_batch({}, items);
                    $$ LANGUAGE SQL STRICT;", 
                    function_name, bound_ptr
                );
                match client.prepare(&batch_sql, &vec![]) {
                    Ok(prepped) => {
                        let _ = prepped.execute(client, None, &vec![]);
                    },
                    Err(e) => {
                        eprintln!("Error creating function {}: {}", function_name, e);
                    }
                }

                let sql = format!(
                    "CREATE OR REPLACE FUNCTION {}_nanos(items bytea[]) RETURNS bigint AS $$
                        SELECT pg_bloomer._bf_contains_bound_nanos({}, items);
                    $$ LANGUAGE SQL STRICT;", 
                    function_name, bound_ptr
                );
                match client.prepare(&sql, &vec![]) {
                    Ok(prepped) => {
                        let _ = prepped.execute(client, None, &vec![]);
                    },
                    Err(e) => {
                        eprintln!("Error creating function {}: {}", function_name, e);
                    }
                }
            });
            
            format!("Created bound functions: {}_nanos and {}_batch", function_name, function_name)
        },
        None => format!("Error: Filter with ID {} not found", filter_id),
    }
}

// Internal function that will be called by the dynamically created SQL function
#[pg_extern(name = "_bf_contains_bound_nanos")]
pub fn _bf_contains_bound_nanos(bound_ptr_addr: i64, items: pgrx::Array<&[u8]>) -> i64 {
    let start = std::time::Instant::now();
    
    // Safety: This pointer was created in create_bound_filter_function and is valid 
    // for the lifetime of the PostgreSQL server
    let bound_filter = unsafe { &*(bound_ptr_addr as *const BoundBloomFilter) };
    
    let _res = bound_filter.handle.contains_optional_batch(items.iter());
    
    let duration = start.elapsed();
    duration.as_nanos() as i64
}

// Internal function that will be called by the dynamically created SQL function
#[pg_extern(name = "_bf_contains_bound_batch")]
pub fn _bf_contains_bound_batch(bound_ptr_addr: i64, items: pgrx::Array<&[u8]>) -> Vec<bool> {
    // Safety: This pointer was created in create_bound_filter_function and is valid
    // for the lifetime of the PostgreSQL server
    let bound_filter = unsafe { &*(bound_ptr_addr as *const BoundBloomFilter) };
    
    bound_filter.handle.contains_optional_batch(items.iter())
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
