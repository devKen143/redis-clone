//! A Redis-compatible in-memory data store.
pub mod db;
pub mod glob;
pub mod resp;
#[cfg(not(target_arch = "wasm32"))]
pub mod server;

/// WebAssembly exports for the in-browser redis-cli demo: one shared keyspace,
/// commands passed as text lines, replies returned in redis-cli format.
#[cfg(target_arch = "wasm32")]
pub mod wasm {
    use std::cell::RefCell;
    thread_local! {
        static DB: RefCell<crate::db::Db> = RefCell::new(crate::db::Db::new());
        static OUT: RefCell<Vec<u8>> = RefCell::new(Vec::new());
    }

    #[no_mangle]
    pub extern "C" fn alloc(len: usize) -> *mut u8 {
        let mut v = Vec::<u8>::with_capacity(len);
        let p = v.as_mut_ptr();
        std::mem::forget(v);
        p
    }

    #[no_mangle]
    pub unsafe extern "C" fn dealloc(ptr: *mut u8, len: usize) {
        drop(Vec::from_raw_parts(ptr, 0, len));
    }

    /// Executes one command line at time `now_ms` (passed from JS's Date.now()).
    #[no_mangle]
    pub unsafe extern "C" fn exec(ptr: *const u8, len: usize, now_ms: f64) -> u32 {
        let line = String::from_utf8_lossy(std::slice::from_raw_parts(ptr, len)).into_owned();
        let (code, text) = match crate::resp::split_args(&line) {
            Err(e) => (1, format!("(error) ERR {e}")),
            Ok(args) if args.is_empty() => (0, String::new()),
            Ok(args) => {
                let r = DB.with(|db| db.borrow_mut().execute(&args, now_ms as u64));
                (matches!(r, crate::resp::Reply::Error(_)) as u32, r.pretty())
            }
        };
        OUT.with(|o| *o.borrow_mut() = text.into_bytes());
        code
    }

    #[no_mangle]
    pub extern "C" fn output_ptr() -> *const u8 {
        OUT.with(|o| o.borrow().as_ptr())
    }

    #[no_mangle]
    pub extern "C" fn output_len() -> usize {
        OUT.with(|o| o.borrow().len())
    }

    #[no_mangle]
    pub extern "C" fn dbsize() -> usize {
        DB.with(|db| db.borrow().len())
    }
}
