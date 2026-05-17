use crate::utils::{cell::Cell, row::Row};
use pgrx::{
    list::{self, List},
    memcx::{self, MemCx},
    pg_sys::{
        self, defGetString, fmgr_info, getTypeInputInfo, list_concat, Datum, FmgrInfo,
        FormData_pg_attribute, InputFunctionCall, MemoryContext, Oid, TupleDescData,
    },
    FromDatum, IntoDatum, PgBox, PgTupleDesc,
};
use std::{
    collections::HashMap,
    ffi::{c_void, CStr, CString},
    num::NonZeroUsize,
};

pub unsafe fn get_foreign_table_options(relid: pgrx::pg_sys::Oid) -> HashMap<String, String> {
    let mut options = HashMap::new();
    let opts_list = get_options_from_fdw(relid);
    if opts_list.is_null() {
        return options;
    }

    memcx::current_context(|mcx| {
        let opts_list = pg_list_to_rust_list::<*mut c_void>(opts_list, mcx);
        for option in opts_list.iter() {
            let def_elem = option.cast::<pg_sys::DefElem>();
            if def_elem.is_null() {
                continue;
            }
            options.insert(
                string_from_cstr((*def_elem).defname),
                string_from_cstr(defGetString(def_elem)),
            );
        }
    });
    options
}

unsafe fn get_options_from_fdw(relid: Oid) -> *mut pg_sys::List {
    let table = pg_sys::GetForeignTable(relid);
    let server = pg_sys::GetForeignServer((*table).serverid);
    let wrapper = pg_sys::GetForeignDataWrapper((*server).fdwid);
    // let mapping= pg_sys::GetUserMapping(pg_sys::GetUserId(), (*server).fdwid);
    let mut opts_list = std::ptr::null_mut();

    opts_list = list_concat(opts_list, (*wrapper).options);
    opts_list = list_concat(opts_list, (*server).options);
    opts_list = list_concat(opts_list, (*table).options);
    //opts_list = list_concat(opts_list, (*mapping).options);
    opts_list
}

#[inline]
pub unsafe fn exec_clear_tuple(slot: *mut pgrx::pg_sys::TupleTableSlot) {
    if let Some(clear) = (*(*slot).tts_ops).clear {
        clear(slot);
    }
}

pub unsafe fn tuple_table_slot_to_row(slot: *mut pgrx::pg_sys::TupleTableSlot) -> Row {
    let tup_desc = PgTupleDesc::from_pg_copy((*slot).tts_tupleDescriptor);

    let mut should_free = false;
    let htup = pgrx::pg_sys::ExecFetchSlotHeapTuple(slot, false, &mut should_free);
    let htup = PgBox::from_pg(htup);
    let mut row = Row::new();

    for (att_idx, attr) in tup_desc.iter().filter(|a| !a.attisdropped).enumerate() {
        let col = pgrx::name_data_to_str(&attr.attname);
        let attno = NonZeroUsize::new(att_idx + 1).unwrap();
        let cell: Option<Cell> = pgrx::htup::heap_getattr(&htup, attno, &tup_desc);
        row.push(col, cell);
    }

    row
}

#[inline]
pub fn string_from_cstr(c_str: *const i8) -> String {
    if c_str.is_null() {
        return String::new();
    }
    unsafe {
        CStr::from_ptr(c_str)
            .to_string_lossy()
            .trim_end_matches('\0')
            .to_string()
    }
}

pub unsafe fn get_datum(value_str: &str, typid: Oid) -> Datum {
    if value_str.is_empty() {
        return Datum::null();
    }

    let c_value = CString::new(value_str).unwrap();
    let mut typeinput = Oid::default();
    let mut typeioparam = Oid::default();
    let mut finfo = FmgrInfo::default();
    getTypeInputInfo(typid, &mut typeinput, &mut typeioparam);
    fmgr_info(typeinput, &mut finfo);

    InputFunctionCall(&mut finfo, c_value.as_ptr().cast_mut(), typeioparam, -1)
}

pub unsafe fn pg_list_to_rust_list<'a, T: list::Enlist>(
    list: *mut pg_sys::List,
    mcx: &'a MemCx<'_>,
) -> list::List<'a, T> {
    list::List::<T>::downcast_ptr_in_memcx(list, mcx).expect("Failed to downcast list pointer")
}

/// Serialize a raw pointer to a PG List (for passing state through fdw_private)
pub unsafe fn serialize_ptr_to_list(ptr: *mut c_void) -> *mut pg_sys::List {
    memcx::current_context(|mcx| {
        let mut ret = List::<*mut c_void>::Nil;
        let val = ptr as i64;
        let cst: *mut pg_sys::Const = pg_sys::makeConst(
            pg_sys::INT8OID,
            -1,
            pg_sys::InvalidOid,
            8,
            val.into_datum().unwrap(),
            false,
            true,
        );
        ret.unstable_push_in_context(cst as _, mcx);
        ret.into_ptr()
    })
}

/// Deserialize a raw pointer from a PG List (for retrieving state from fdw_private)
pub unsafe fn deserialize_ptr_from_list(list: *mut pg_sys::List) -> *mut c_void {
    memcx::current_context(|mcx| {
        if let Some(list) = List::<*mut c_void>::downcast_ptr_in_memcx(list, mcx) {
            if let Some(cst) = list.get(0) {
                let cst = *(*cst as *mut pg_sys::Const);
                let ptr = i64::from_datum(cst.constvalue, cst.constisnull).unwrap();
                return ptr as *mut c_void;
            }
        }
        std::ptr::null_mut()
    })
}

pub unsafe fn delete_wrappers_memctx(ctx: MemoryContext) {
    if !ctx.is_null() {
        pg_sys::pfree((*ctx).name as _);
        pg_sys::MemoryContextDelete(ctx)
    }
}

pub fn cell_to_string(cell: Option<&Cell>) -> String {
    cell.map(|c| c.to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

pub unsafe fn write_datum_to_slot(
    slot: *mut pgrx::pg_sys::TupleTableSlot,
    tupdesc: pgrx::pg_sys::TupleDesc,
    colno: usize,
    value: &str,
) {
    let pgtype = (*tuple_desc_attr(tupdesc, colno)).atttypid;
    let datum = get_datum(value, pgtype);
    (*slot).tts_values.add(colno).write(datum);
    (*slot).tts_isnull.add(colno).write(false);
}

#[allow(dead_code)]
pub unsafe fn tuple_desc_attr_address(desc: *mut TupleDescData) -> *mut FormData_pg_attribute {
    #[cfg(feature = "pg18")]
    {
        let _ = desc;
        unreachable!("Use pg_sys::TupleDescAttr on PG18");
    }
    #[cfg(not(feature = "pg18"))]
    {
        let base = desc as *mut u8;
        let offset = std::mem::size_of::<TupleDescData>();
        base.add(offset) as *mut FormData_pg_attribute
    }
}

pub unsafe fn tuple_desc_attr(desc: *mut TupleDescData, i: usize) -> *mut FormData_pg_attribute {
    assert!(!desc.is_null());
    assert!(i < (*desc).natts as usize);

    #[cfg(feature = "pg18")]
    {
        pg_sys::TupleDescAttr(desc, i as _)
    }
    #[cfg(not(feature = "pg18"))]
    {
        let attrs = tuple_desc_attr_address(desc);
        attrs.add(i)
    }
}

#[inline]
pub unsafe fn relation_get_descr(relation: pg_sys::Relation) -> pg_sys::TupleDesc {
    (*relation).rd_att
}

pub unsafe fn exec_get_junk_attribute(
    slot: *mut pg_sys::TupleTableSlot,
    attno: pg_sys::AttrNumber,
    is_null: *mut bool,
) -> pg_sys::Datum {
    assert!(!slot.is_null());
    assert!(attno > 0);

    let slot = &mut *slot;
    let attno_usize = attno as usize;

    // Ensure attributes up to attno are fetched
    if attno_usize > slot.tts_nvalid as usize {
        pg_sys::slot_getsomeattrs(slot, attno_usize as i32);
    }

    // Get the value and null flag
    *is_null = *slot.tts_isnull.add(attno_usize - 1);
    *slot.tts_values.add(attno_usize - 1)
}
