#![allow(proc_macro_derive_resolution_fallback)]

table! {
    builds (id) {
        id -> Int4,
        is_published -> Bool,
        created -> Timestamptz,
    }
}
