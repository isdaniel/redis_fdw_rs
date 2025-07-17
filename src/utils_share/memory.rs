use pgrx::{pg_sys::{AsPgCStr, MemoryContext}, PgMemoryContexts};

const ROOT_MEMCTX_NAME: &str = "WrappersRootMemCtx";

pub unsafe fn create_wrappers_memctx(name: &str) -> MemoryContext {
    let mut root = ensure_root_wrappers_memctx();
    let name = root.switch_to(|_| name.as_pg_cstr());
    pgrx::pg_sys::AllocSetContextCreateExtended(
        root.value(),
        name,
        pgrx::pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
        pgrx::pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
        pgrx::pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
    )
}

unsafe fn ensure_root_wrappers_memctx() -> PgMemoryContexts {
    find_memctx_under(ROOT_MEMCTX_NAME, PgMemoryContexts::CacheMemoryContext).unwrap_or_else(|| {
        let name = PgMemoryContexts::CacheMemoryContext.pstrdup(ROOT_MEMCTX_NAME);
        let ctx = pgrx::pg_sys::AllocSetContextCreateExtended(
            PgMemoryContexts::CacheMemoryContext.value(),
            name,
            pgrx::pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pgrx::pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pgrx::pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );
        PgMemoryContexts::For(ctx)
    })
}

// search memory context by name under specified MemoryContext
unsafe fn find_memctx_under(name: &str, under: PgMemoryContexts) -> Option<PgMemoryContexts> {
    let mut ctx = (*under.value()).firstchild;
    while !ctx.is_null() {
        if let Ok(ctx_name) = std::ffi::CStr::from_ptr((*ctx).name).to_str() {
            if ctx_name == name {
                return Some(PgMemoryContexts::For(ctx));
            }
        }
        ctx = (*ctx).nextchild;
    }
    None
}
