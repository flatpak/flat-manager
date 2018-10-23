#![allow(proc_macro_derive_resolution_fallback)]

use chrono;

use schema::{ build_refs, };

#[derive(Serialize, Queryable, Debug)]
pub struct Build {
    pub id: i32,
    pub is_published: bool,
    pub created: chrono::NaiveDateTime,
    pub repo_state: i16
}

#[derive(Deserialize, Insertable, Debug)]
#[table_name = "build_refs"]
pub struct NewBuildRef {
    pub build_id: i32,
    pub ref_name: String,
    pub commit: String,
}

#[derive(Serialize, Queryable, Debug)]
pub struct BuildRef {
    pub id: i32,
    pub build_id: i32,
    pub ref_name: String,
    pub commit: String,
}
