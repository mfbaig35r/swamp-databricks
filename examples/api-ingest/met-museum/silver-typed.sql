-- Met Museum Silver: typed columns extracted from raw_json.
--
-- Run via sql_warehouse.run_query as part of the workflow, or
-- materialize as a DLT view.
--
-- Idempotent: CREATE OR REPLACE on every run. Bronze stays the
-- canonical archive; this is just a typed projection.
--
-- Replace {{ catalog }} with your target catalog (e.g. workspace on Free).

CREATE OR REPLACE TABLE {{ catalog }}.met_museum.silver_objects AS
SELECT
    object_id                                                          AS object_id,
    CAST(raw_json:objectID            AS BIGINT)                       AS met_object_id,
    CAST(raw_json:accessionNumber     AS STRING)                       AS accession_number,
    CAST(raw_json:accessionYear       AS STRING)                       AS accession_year,
    CAST(raw_json:isHighlight         AS BOOLEAN)                      AS is_highlight,
    CAST(raw_json:isPublicDomain      AS BOOLEAN)                      AS is_public_domain,
    CAST(raw_json:title               AS STRING)                       AS title,
    CAST(raw_json:objectDate          AS STRING)                       AS object_date,
    CAST(raw_json:objectBeginDate     AS INT)                          AS object_begin_date,
    CAST(raw_json:objectEndDate       AS INT)                          AS object_end_date,
    CAST(raw_json:medium              AS STRING)                       AS medium,
    CAST(raw_json:dimensions          AS STRING)                       AS dimensions,
    CAST(raw_json:classification      AS STRING)                       AS classification,
    CAST(raw_json:department          AS STRING)                       AS department,
    CAST(raw_json:culture             AS STRING)                       AS culture,
    CAST(raw_json:period              AS STRING)                       AS period,
    CAST(raw_json:dynasty             AS STRING)                       AS dynasty,
    CAST(raw_json:reign               AS STRING)                       AS reign,
    CAST(raw_json:artistDisplayName   AS STRING)                       AS artist_display_name,
    CAST(raw_json:artistDisplayBio    AS STRING)                       AS artist_display_bio,
    CAST(raw_json:artistRole          AS STRING)                       AS artist_role,
    CAST(raw_json:artistNationality   AS STRING)                       AS artist_nationality,
    CAST(raw_json:artistBeginDate     AS STRING)                       AS artist_begin_date,
    CAST(raw_json:artistEndDate       AS STRING)                       AS artist_end_date,
    CAST(raw_json:primaryImage        AS STRING)                       AS primary_image_url,
    CAST(raw_json:primaryImageSmall   AS STRING)                       AS primary_image_small_url,
    CAST(raw_json:objectURL           AS STRING)                       AS object_url,
    -- Nested fields kept as raw JSON strings; extract in Gold layer or
    -- query with from_json / get_json_object as needed.
    CAST(raw_json:tags                AS STRING)                       AS tags_raw_json,
    CAST(raw_json:measurements        AS STRING)                       AS measurements_raw_json,
    CAST(raw_json:additionalImages    AS STRING)                       AS additional_images_raw_json,
    CAST(raw_json:constituents        AS STRING)                       AS constituents_raw_json,
    ingested_at                                                        AS ingested_at
FROM {{ catalog }}.met_museum.bronze_objects
WHERE http_status = 200
  AND raw_json IS NOT NULL;
