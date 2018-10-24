table! {
    build_refs (id) {
        id -> Int4,
        build_id -> Int4,
        ref_name -> Text,
        commit -> Text,
    }
}

table! {
    builds (id) {
        id -> Int4,
        is_published -> Bool,
        created_at -> Timestamp,
        repo_state -> Int2,
        repo_state_reason -> Nullable<Text>,
    }
}

table! {
    commit_logs (id) {
        id -> Int4,
        build_id -> Int4,
        text -> Text,
    }
}

table! {
    published_refs (id) {
        id -> Int4,
        build_id -> Int4,
        ref_name -> Text,
        commit -> Text,
    }
}

joinable!(build_refs -> builds (build_id));
joinable!(commit_logs -> builds (build_id));
joinable!(published_refs -> builds (build_id));

allow_tables_to_appear_in_same_query!(
    build_refs,
    builds,
    commit_logs,
    published_refs,
);
