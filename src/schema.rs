#![allow(proc_macro_derive_resolution_fallback)]

table! {
    builds (id) {
        id -> Int4,
        is_published -> Bool,
        created_at -> Timestamp,
        repo_state -> Int2,
    }
}

table! {
    logs (id) {
        id -> Int4,
        build_id -> Int4,
        log_type -> Int2,
        log_text -> Text,
    }
}

table! {
    refs (id) {
        id -> Int4,
        build_id -> Int4,
        ref_type -> Int2,
        ref_name -> Text,
        commit -> Text,
    }
}

joinable!(logs -> builds (build_id));
joinable!(refs -> builds (build_id));

allow_tables_to_appear_in_same_query!(
    builds,
    logs,
    refs,
);
