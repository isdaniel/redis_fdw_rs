use pgrx::{pg_sys, FromDatum};

#[derive(Debug, Default)]
#[repr(C)]
struct Limit {
    count: i64,
    offset: i64,
}

pub unsafe fn extract_limit(
    root: *mut pg_sys::PlannerInfo,
) -> Option<Limit> {
    let parse = (*root).parse;

    // only push down constant LIMITs that are not NULL
    let limit_count = (*parse).limitCount as *mut pg_sys::Const;
    if limit_count.is_null() || !pgrx::is_a(limit_count as *mut pg_sys::Node, pg_sys::NodeTag::T_Const) {
        return None;
    }

    let mut limit = Limit::default();

    if let Some(count) = i64::from_polymorphic_datum(
        (*limit_count).constvalue,
        (*limit_count).constisnull,
        (*limit_count).consttype,
    ) {
        limit.count = count;
    } else {
        return None;
    }

    // only consider OFFSETS that are non-NULL constants
    let limit_offset = (*parse).limitOffset as *mut pg_sys::Const;
    if !limit_offset.is_null() && pgrx::is_a(limit_offset as *mut pg_sys::Node, pg_sys::NodeTag::T_Const)
    {
        if let Some(offset) = i64::from_polymorphic_datum(
            (*limit_offset).constvalue,
            (*limit_offset).constisnull,
            (*limit_offset).consttype,
        ) {
            limit.offset = offset;
        }
    }

    Some(limit)
}
