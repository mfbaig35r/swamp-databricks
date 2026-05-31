# Met Museum API → Databricks (reference implementation)

Full Swamp workflow + Databricks notebook that pulls the Metropolitan
Museum of Art's open collection (~470K objects) into a Bronze + Silver
table pair in Unity Catalog.

**Why this example:** the Met API needs **no authentication** and **no
API key**, so the workflow is runnable on any Databricks workspace
(including Free Edition) with zero credential setup. Read
[../README.md](../README.md) first for the universal pattern this
implements.

## What you get

| Table | Shape | Rows | Use |
|---|---|---|---|
| `<catalog>.met_museum.bronze_objects` | `object_id, http_status, raw_json, error_message, ingested_at` | ~470K | Raw archive, immune to schema drift |
| `<catalog>.met_museum.silver_objects` | 20+ typed columns (title, artist, dates, medium, image URLs, etc.) | ~470K (200-only) | Analytics, dashboards, ML training |

## Endpoints used

- `GET https://collectionapi.metmuseum.org/public/collection/v1/objects`  
  returns a JSON object `{"total": N, "objectIDs": [...]}` with every public
  object ID (one request).
- `GET https://collectionapi.metmuseum.org/public/collection/v1/objects/{id}`  
  returns one object's full record (~2KB JSON, ~50 fields).

No `search` or `departments` endpoint used in this reference; add them if
you want filtered subsets.

## Runtime expectations

| Mode | Wall time | Bronze rows |
|---|---|---|
| `MAX_OBJECTS=10` (default in the example for fast testing) | ~5 sec | 10 |
| `MAX_OBJECTS=1000` | ~60 sec | 1000 |
| `MAX_OBJECTS=None` (full backfill) | **~100 min** | ~470K |

The Met API's unofficial rate cap is roughly 80 req/sec. With
`NUM_PARTITIONS=8` and `per_partition_rate=10`, the workflow stays under
the cap and the full pull is bounded by Met's rate (not by your Databricks
compute). Bumping `NUM_PARTITIONS` doesn't help past 80 req/sec total.

## Files

- [`workflow.yaml`](./workflow.yaml) — the Swamp workflow that ties it
  together: ensure schema exists, upload notebook, define job, run + wait,
  run Silver transform, snapshot result. **The notebook Python and Silver
  SQL are both inlined in this file**, so it's fully self-contained and
  runnable directly.
- [`notebook-bronze.py`](https://github.com/mfbaig35r/swamp-databricks/blob/main/examples/api-ingest/met-museum/notebook-bronze.py) — the standalone notebook source, for clean review
  and editing. **GitHub-only**: the Swamp registry restricts published
  archives to `.ts .json .md .yaml .yml .txt` extensions, so `.py` files
  don't ship in the pulled archive. The same Python lives inside
  `workflow.yaml` under the `notebook.upload` step's `content:` field.
- [`silver-typed.sql`](https://github.com/mfbaig35r/swamp-databricks/blob/main/examples/api-ingest/met-museum/silver-typed.sql) — the standalone Silver SQL, for clean review.
  **GitHub-only**: same `.sql` extension restriction as above. The SQL
  is inlined in `workflow.yaml` under the `sql_warehouse.run_query`
  step's `statement:` field.

## Running it

```sh
# Adopt your existing warehouse if you haven't already, then:
swamp workflow run met-museum-bronze-ingest.yaml \
  --input max_objects=100  # remove for full ~100-min pull
```

After the workflow completes:

```sql
SELECT * FROM <catalog>.met_museum.silver_objects LIMIT 10;
SELECT department, COUNT(*) FROM <catalog>.met_museum.silver_objects
  GROUP BY department ORDER BY 2 DESC;
SELECT COUNT(*) FROM <catalog>.met_museum.bronze_objects
  WHERE http_status != 200;  -- inspect failures
```

## What's reusable beyond Met

Everything below the `# === Universal engine ===` line in
`notebook-bronze.py` works for any HTTP API. The config block at the top
is what changes. Read [../README.md](../README.md) for the breakdown of
universal vs API-specific pieces.

For a public‑no‑auth API similar to Met, you can clone this directory,
update the config block + Silver SQL, and have a working pipeline in
under an hour.

For authenticated APIs (Stripe, GitHub, Salesforce): add a
`secret_scope.create_or_update` + `secret.put` step at the start of the
workflow, then read the token in `build_headers()` via
`dbutils.secrets.get(scope, key)`.

## Known limitations of this reference

1. **Full‑refresh mode only.** No incremental support. To make this
   incremental, add a step that reads existing `bronze_objects.object_id`
   into a set, intersects against `/objects` response, and only fetches
   the diff.
2. **No restart safety.** If the long pull crashes at 60%, the next run
   redoes everything. The volume‑staging variant in
   [../README.md](../README.md) is the production answer.
3. **In‑memory list of IDs.** ~470K integers fits comfortably (~4MB), so
   we hold them on the driver. For APIs with tens of millions of IDs,
   stream the list into a Bronze "ids" table first and fan out from that.
4. **Met's nested fields stored as JSON strings in Silver.**
   `tags`, `measurements`, `additional_images`, `constituents` are kept as
   `STRING` (raw JSON). Use `from_json` + an explicit schema, or
   `get_json_object`, in your Gold layer if you need them as typed
   structs. Databricks `VARIANT` is an option if you don't care about
   Iceberg portability.
