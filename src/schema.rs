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
        created_at -> Timestamp,
        repo_state -> Int2,
        repo_state_reason -> Nullable<Text>,
        published_state -> Int2,
        published_state_reason -> Nullable<Text>,
        commit_job_id -> Nullable<Int4>,
        publish_job_id -> Nullable<Int4>,
        repo -> Text,
        extra_ids -> Array<Text>,
    }
}

table! {
    job_dependencies (job_id, depends_on) {
        job_id -> Int4,
        depends_on -> Int4,
    }
}

table! {
    jobs (id) {
        id -> Int4,
        kind -> Int2,
        status -> Int2,
        contents -> Text,
        results -> Nullable<Text>,
        log -> Text,
        start_after -> Nullable<Timestamp>,
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
joinable!(published_refs -> builds (build_id));

allow_tables_to_appear_in_same_query!(
    build_refs,
    builds,
    job_dependencies,
    jobs,
    published_refs,
);
