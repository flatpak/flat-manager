// @generated automatically by Diesel CLI.

diesel::table! {
    build_refs (id) {
        id -> Int4,
        build_id -> Int4,
        ref_name -> Text,
        commit -> Text,
        build_log_url -> Nullable<Text>,
    }
}

diesel::table! {
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
        extra_ids -> Array<Nullable<Text>>,
        app_id -> Nullable<Text>,
        public_download -> Bool,
        build_log_url -> Nullable<Text>,
        token_name -> Nullable<Text>,
        token_type -> Nullable<Text>,
        token_branches -> Nullable<Array<Text>>,
    }
}

diesel::table! {
    checks (check_name, build_id) {
        check_name -> Text,
        build_id -> Int4,
        job_id -> Int4,
        status -> Int2,
        status_reason -> Nullable<Text>,
        results -> Nullable<Text>,
    }
}

diesel::table! {
    job_dependencies (job_id, depends_on) {
        job_id -> Int4,
        depends_on -> Int4,
    }
}

diesel::table! {
    jobs (id) {
        id -> Int4,
        kind -> Int2,
        status -> Int2,
        contents -> Text,
        results -> Nullable<Text>,
        log -> Text,
        start_after -> Nullable<Timestamp>,
        repo -> Nullable<Text>,
    }
}

diesel::table! {
    published_refs (id) {
        id -> Int4,
        build_id -> Int4,
        ref_name -> Text,
        commit -> Text,
    }
}

diesel::table! {
    tokens (token_id) {
        token_id -> Text,
        expires -> Nullable<Timestamp>,
        last_used -> Nullable<Timestamp>,
        revoked_at -> Nullable<Timestamp>,
    }
}

diesel::joinable!(build_refs -> builds (build_id));
diesel::joinable!(checks -> builds (build_id));
diesel::joinable!(checks -> jobs (job_id));
diesel::joinable!(published_refs -> builds (build_id));

diesel::allow_tables_to_appear_in_same_query!(
    build_refs,
    builds,
    checks,
    job_dependencies,
    jobs,
    published_refs,
    tokens,
);
