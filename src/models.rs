#![allow(proc_macro_derive_resolution_fallback)]

use chrono;

#[derive(Serialize, Queryable, Debug)]
pub struct Build {
    pub id: i32,
    pub is_published: bool,
    pub created: chrono::NaiveDateTime,
}
