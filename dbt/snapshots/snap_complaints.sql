{% snapshot snap_complaints %}

{{
    config(
        unique_key='complaint_id',
        strategy='check',
        check_cols=['status_hash', 'attribute_hash'],
        updated_at='snapshot_ts',
        hard_deletes='invalidate',
    )
}}

-- SCD2 history of every complaint. A new version is written when either hash changes;
-- a complaint missing from the snapshot is closed. Validity timestamps are the snapshot
-- date (see macros/snapshot_time.sql), so applying the same snapshot twice changes nothing.
select
    complaint_id,
    date_received,
    product,
    sub_product,
    issue,
    sub_issue,
    company,
    state,
    zip_code,
    tags,
    submitted_via,
    date_sent_to_company,
    company_response,
    company_public_response,
    timely_response,
    snapshot_date,
    run_id,
    {{ snapshot_ts() }} as snapshot_ts,
    {{ hash_columns(['company_response', 'company_public_response', 'timely_response']) }} as status_hash,
    {{ hash_columns([
        'date_received', 'product', 'sub_product', 'issue', 'sub_issue', 'company', 'state',
        'zip_code', 'tags', 'submitted_via', 'date_sent_to_company',
    ]) }} as attribute_hash
from {{ source('raw', 'complaints_snapshot') }}
where snapshot_date = {{ snapshot_date() }}

{% endsnapshot %}
