use pgrx::{
    prelude::*,
    pg_sys::{Oid, Datum},
    fcinfo,
};
use std::{fmt, ffi::CStr};

#[derive(Debug)]
pub enum Cell {
    Bool(bool),
    I8(i8),
    I16(i16),
    F32(f32),
    I32(i32),
    F64(f64),
    I64(i64),
    Numeric(AnyNumeric),
    String(String),
    Date(Date),
    Time(Time),
    Timestamp(Timestamp),
    Timestamptz(TimestampWithTimeZone),
    Interval(Interval),
    BoolArray(Vec<Option<bool>>),
    I16Array(Vec<Option<i16>>),
    I32Array(Vec<Option<i32>>),
    I64Array(Vec<Option<i64>>),
    F32Array(Vec<Option<f32>>),
    F64Array(Vec<Option<f64>>),
    StringArray(Vec<Option<String>>),
}

impl FromDatum for Cell {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, typoid: Oid) -> Option<Self>
    where
        Self: Sized,
    {
        let oid = PgOid::from(typoid);
        match oid {
            PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
                bool::from_datum(datum, is_null).map(Cell::Bool)
            }
            PgOid::BuiltIn(PgBuiltInOids::CHAROID) => i8::from_datum(datum, is_null).map(Cell::I8),
            PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
                i16::from_datum(datum, is_null).map(Cell::I16)
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
                f32::from_datum(datum, is_null).map(Cell::F32)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
                i32::from_datum(datum, is_null).map(Cell::I32)
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => {
                f64::from_datum(datum, is_null).map(Cell::F64)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
                i64::from_datum(datum, is_null).map(Cell::I64)
            }
            PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
                AnyNumeric::from_datum(datum, is_null).map(Cell::Numeric)
            }
            PgOid::BuiltIn(PgBuiltInOids::TEXTOID) => {
                String::from_datum(datum, is_null).map(Cell::String)
            }
            PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
                Date::from_datum(datum, is_null).map(Cell::Date)
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMEOID) => {
                Time::from_datum(datum, is_null).map(Cell::Time)
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
                Timestamp::from_datum(datum, is_null).map(Cell::Timestamp)
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
                TimestampWithTimeZone::from_datum(datum, is_null).map(Cell::Timestamptz)
            }
            PgOid::BuiltIn(PgBuiltInOids::INTERVALOID) => {
                Interval::from_datum(datum, is_null).map(Cell::Interval)
            }
            PgOid::BuiltIn(PgBuiltInOids::BOOLARRAYOID) => {
                Vec::<Option<bool>>::from_datum(datum, false).map(Cell::BoolArray)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT2ARRAYOID) => {
                Vec::<Option<i16>>::from_datum(datum, false).map(Cell::I16Array)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT4ARRAYOID) => {
                Vec::<Option<i32>>::from_datum(datum, false).map(Cell::I32Array)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT8ARRAYOID) => {
                Vec::<Option<i64>>::from_datum(datum, false).map(Cell::I64Array)
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4ARRAYOID) => {
                Vec::<Option<f32>>::from_datum(datum, false).map(Cell::F32Array)
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT8ARRAYOID) => {
                Vec::<Option<f64>>::from_datum(datum, false).map(Cell::F64Array)
            }
            PgOid::BuiltIn(PgBuiltInOids::TEXTARRAYOID) => {
                Vec::<Option<String>>::from_datum(datum, false).map(Cell::StringArray)
            }
            _ => None,
        }
    }
}

impl IntoDatum for Cell {
    fn into_datum(self) -> Option<Datum> {
        match self {
            Cell::Bool(v) => v.into_datum(),
            Cell::I8(v) => v.into_datum(),
            Cell::I16(v) => v.into_datum(),
            Cell::F32(v) => v.into_datum(),
            Cell::I32(v) => v.into_datum(),
            Cell::F64(v) => v.into_datum(),
            Cell::I64(v) => v.into_datum(),
            Cell::Numeric(v) => v.into_datum(),
            Cell::String(v) => v.into_datum(),
            Cell::Date(v) => v.into_datum(),
            Cell::Time(v) => v.into_datum(),
            Cell::Timestamp(v) => v.into_datum(),
            Cell::Timestamptz(v) => v.into_datum(),
            Cell::Interval(v) => v.into_datum(),
            Cell::BoolArray(v) => v.into_datum(),
            Cell::I16Array(v) => v.into_datum(),
            Cell::I32Array(v) => v.into_datum(),
            Cell::I64Array(v) => v.into_datum(),
            Cell::F32Array(v) => v.into_datum(),
            Cell::F64Array(v) => v.into_datum(),
            Cell::StringArray(v) => v.into_datum(),
        }
    }

    fn type_oid() -> Oid {
        Oid::INVALID
    }

    fn is_compatible_with(other: Oid) -> bool {
        Self::type_oid() == other
            || other == pgrx::pg_sys::BOOLOID
            || other == pgrx::pg_sys::CHAROID
            || other == pgrx::pg_sys::INT2OID
            || other == pgrx::pg_sys::FLOAT4OID
            || other == pgrx::pg_sys::INT4OID
            || other == pgrx::pg_sys::FLOAT8OID
            || other == pgrx::pg_sys::INT8OID
            || other == pgrx::pg_sys::NUMERICOID
            || other == pgrx::pg_sys::TEXTOID
            || other == pgrx::pg_sys::DATEOID
            || other == pgrx::pg_sys::TIMEOID
            || other == pgrx::pg_sys::TIMESTAMPOID
            || other == pgrx::pg_sys::TIMESTAMPTZOID
            || other == pgrx::pg_sys::INTERVALOID
            || other == pgrx::pg_sys::JSONBOID
            || other == pgrx::pg_sys::BYTEAOID
            || other == pgrx::pg_sys::UUIDOID
            || other == pgrx::pg_sys::BOOLARRAYOID
            || other == pgrx::pg_sys::INT2ARRAYOID
            || other == pgrx::pg_sys::INT4ARRAYOID
            || other == pgrx::pg_sys::INT8ARRAYOID
            || other == pgrx::pg_sys::FLOAT4ARRAYOID
            || other == pgrx::pg_sys::FLOAT8ARRAYOID
            || other == pgrx::pg_sys::TEXTARRAYOID
    }
}

impl Clone for Cell {
    fn clone(&self) -> Self {
        match self {
            Cell::Bool(v) => Cell::Bool(*v),
            Cell::I8(v) => Cell::I8(*v),
            Cell::I16(v) => Cell::I16(*v),
            Cell::F32(v) => Cell::F32(*v),
            Cell::I32(v) => Cell::I32(*v),
            Cell::F64(v) => Cell::F64(*v),
            Cell::I64(v) => Cell::I64(*v),
            Cell::Numeric(v) => Cell::Numeric(v.clone()),
            Cell::String(v) => Cell::String(v.clone()),
            Cell::Date(v) => Cell::Date(*v),
            Cell::Time(v) => Cell::Time(*v),
            Cell::Timestamp(v) => Cell::Timestamp(*v),
            Cell::Timestamptz(v) => Cell::Timestamptz(*v),
            Cell::Interval(v) => Cell::Interval(*v),
            Cell::BoolArray(v) => Cell::BoolArray(v.clone()),
            Cell::I16Array(v) => Cell::I16Array(v.clone()),
            Cell::I32Array(v) => Cell::I32Array(v.clone()),
            Cell::I64Array(v) => Cell::I64Array(v.clone()),
            Cell::F32Array(v) => Cell::F32Array(v.clone()),
            Cell::F64Array(v) => Cell::F64Array(v.clone()),
            Cell::StringArray(v) => Cell::StringArray(v.clone()),
        }
    }
}

impl fmt::Display for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cell::Bool(v) => write!(f, "{}", v),
            Cell::I8(v) => write!(f, "{}", v),
            Cell::I16(v) => write!(f, "{}", v),
            Cell::F32(v) => write!(f, "{}", v),
            Cell::I32(v) => write!(f, "{}", v),
            Cell::F64(v) => write!(f, "{}", v),
            Cell::I64(v) => write!(f, "{}", v),
            Cell::Numeric(v) => write!(f, "{}", v),
            Cell::String(v) => write!(f, "'{}'", v),
            Cell::Date(v) => unsafe {
                let dt =
                    fcinfo::direct_function_call_as_datum(pgrx::pg_sys::date_out, &[(*v).into_datum()])
                        .expect("cell should be a valid date");
                let dt_cstr = CStr::from_ptr(dt.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    dt_cstr.to_str().expect("date should be a valid string")
                )
            },
            Cell::Time(v) => unsafe {
                let ts =
                    fcinfo::direct_function_call_as_datum(pgrx::pg_sys::time_out, &[(*v).into_datum()])
                        .expect("cell should be a valid time");
                let ts_cstr = CStr::from_ptr(ts.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    ts_cstr.to_str().expect("time hould be a valid string")
                )
            },
            Cell::Timestamp(v) => unsafe {
                let ts = fcinfo::direct_function_call_as_datum(
                    pgrx::pg_sys::timestamp_out,
                    &[(*v).into_datum()],
                )
                .expect("cell should be a valid timestamp");
                let ts_cstr = CStr::from_ptr(ts.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    ts_cstr
                        .to_str()
                        .expect("timestamp should be a valid string")
                )
            },
            Cell::Timestamptz(v) => unsafe {
                let ts = fcinfo::direct_function_call_as_datum(
                    pgrx::pg_sys::timestamptz_out,
                    &[(*v).into_datum()],
                )
                .expect("cell should be a valid timestamptz");
                let ts_cstr = CStr::from_ptr(ts.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    ts_cstr
                        .to_str()
                        .expect("timestamptz should be a valid string")
                )
            },
            Cell::Interval(v) => write!(f, "{}", v),
            Cell::BoolArray(v) => write_array(v, f),
            Cell::I16Array(v) => write_array(v, f),
            Cell::I32Array(v) => write_array(v, f),
            Cell::I64Array(v) => write_array(v, f),
            Cell::F32Array(v) => write_array(v, f),
            Cell::F64Array(v) => write_array(v, f),
            Cell::StringArray(v) => write_array(v, f),
        }
    }
}

fn write_array<T: std::fmt::Display>(
    array: &[Option<T>],
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    let res = array
        .iter()
        .map(|e| match e {
            Some(val) => format!("{}", val),
            None => "null".to_owned(),
        })
        .collect::<Vec<String>>()
        .join(",");
    write!(f, "[{}]", res)
}

pub fn parse_cell(val: &str) -> Cell {
    match val.parse::<i32>() {
        Ok(i) => Cell::I32(i),
        Err(_) => Cell::String(val.to_string()),
    }
}
