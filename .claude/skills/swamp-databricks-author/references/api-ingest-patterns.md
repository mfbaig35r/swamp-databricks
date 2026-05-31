# API ingest pagination patterns

Common shapes for how APIs return lists of IDs (the LIST step in the universal
Bronze ingest). Use these to fill in `parse_list_response()` and adapt the
list-fetch logic. The detail-fetch loop and Bronze write are unchanged across
all of these.

## All-at-once (Met Museum)

```python
LIST_URL = "https://collectionapi.metmuseum.org/public/collection/v1/objects"

def parse_list_response(response_json):
    return response_json.get("objectIDs", []) or []

response = session.get(LIST_URL, timeout=30)
all_ids = parse_list_response(response.json())
```

Single request returns the full list. Works for catalogs with up to ~1M IDs
(fits in driver memory). For larger catalogs, switch to cursor pagination.

## Cursor pagination (Stripe, GitHub modern)

```python
LIST_URL = "https://api.stripe.com/v1/customers"
PAGE_SIZE = 100

def parse_list_response(response_json):
    return [obj["id"] for obj in response_json.get("data", [])]

all_ids = []
cursor = None
while True:
    params = {"limit": PAGE_SIZE}
    if cursor:
        params["starting_after"] = cursor
    response = session.get(LIST_URL, params=params, timeout=30)
    response.raise_for_status()
    body = response.json()
    page_ids = parse_list_response(body)
    if not page_ids:
        break
    all_ids.extend(page_ids)
    if not body.get("has_more"):
        break
    cursor = page_ids[-1]
```

Each request returns up to `PAGE_SIZE` IDs plus a `has_more` flag. The cursor
is the last ID seen. Variants: Salesforce uses `nextRecordsUrl` (full URL),
Shopify uses `Link` header (see below).

## Link header pagination (GitHub legacy, some REST APIs)

```python
import re

LIST_URL = "https://api.github.com/orgs/example/repos"
PAGE_SIZE = 100

def parse_link_header(headers):
    """Return next URL from RFC 5988 Link header, or None."""
    link = headers.get("Link", "")
    m = re.search(r'<([^>]+)>;\s*rel="next"', link)
    return m.group(1) if m else None

def parse_list_response(response_json):
    return [obj["id"] for obj in response_json]

all_ids = []
url = f"{LIST_URL}?per_page={PAGE_SIZE}"
while url:
    response = session.get(url, timeout=30)
    response.raise_for_status()
    all_ids.extend(parse_list_response(response.json()))
    url = parse_link_header(response.headers)
```

The server returns the next URL in the `Link` header. Client follows until
no `rel="next"` is present.

## Offset pagination (Salesforce SOQL, legacy REST)

```python
LIST_URL = "https://example.com/api/items"
PAGE_SIZE = 200

def parse_list_response(response_json):
    return [obj["id"] for obj in response_json.get("results", [])]

all_ids = []
offset = 0
while True:
    response = session.get(
        LIST_URL,
        params={"limit": PAGE_SIZE, "offset": offset},
        timeout=30,
    )
    response.raise_for_status()
    page_ids = parse_list_response(response.json())
    if not page_ids:
        break
    all_ids.extend(page_ids)
    if len(page_ids) < PAGE_SIZE:
        break
    offset += PAGE_SIZE
```

Client tracks offset and increments by page size. Stop when a page is shorter
than `PAGE_SIZE` (last page) or empty.

## Streaming list endpoint (very large catalogs)

For catalogs with tens of millions of IDs, holding all IDs in driver memory
breaks. Stream the list into a Bronze "ids" table first, then drive the
detail fetch from that:

```python
# Step 1: stream list endpoint to staging table
def stream_list_to_table(url, target_table):
    ids = []
    BATCH = 50_000
    while url:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        body = response.json()
        ids.extend(parse_list_response(body))
        if len(ids) >= BATCH:
            spark.createDataFrame([(i,) for i in ids], ["id"]).write \
                 .mode("append").saveAsTable(target_table)
            ids = []
        url = body.get("next_url") or parse_link_header(response.headers)
    if ids:
        spark.createDataFrame([(i,) for i in ids], ["id"]).write \
             .mode("append").saveAsTable(target_table)

# Step 2: fan out from the staging table
id_df = spark.table(target_table).select("id")
results_df = (
    id_df.repartition(NUM_PARTITIONS)
    .mapInPandas(fetch_partition, schema=result_schema)
    .withColumn("ingested_at", F.current_timestamp())
)
```

This pattern also gives you natural restartability: rerun the detail fetch
filtering by `id NOT IN (SELECT object_id FROM bronze)` to pick up only
missing IDs.

## Auth header patterns

Three common ones, all returned from `build_headers()`:

```python
# API key in header (Stripe-style)
def build_headers():
    return {
        "Authorization": f"Bearer {dbutils.secrets.get('stripe', 'api_key')}",
    }

# API key in custom header (Mailchimp, some others)
def build_headers():
    return {
        "X-Api-Key": dbutils.secrets.get("vendor", "api_key"),
    }

# OAuth Bearer with periodic refresh (Salesforce, OAuth2 services)
# Note: needs a token refresh helper outside build_headers. Fetch a fresh
# access_token before the fan-out and pass it through globally.
import time
_token_cache = {"access_token": None, "expires_at": 0}

def get_oauth_token():
    if _token_cache["access_token"] and time.time() < _token_cache["expires_at"] - 60:
        return _token_cache["access_token"]
    client_id = dbutils.secrets.get("vendor", "client_id")
    client_secret = dbutils.secrets.get("vendor", "client_secret")
    response = requests.post(
        "https://oauth.example.com/token",
        data={"grant_type": "client_credentials",
              "client_id": client_id,
              "client_secret": client_secret},
        timeout=30,
    )
    response.raise_for_status()
    body = response.json()
    _token_cache["access_token"] = body["access_token"]
    _token_cache["expires_at"] = time.time() + body.get("expires_in", 3600)
    return _token_cache["access_token"]

def build_headers():
    return {"Authorization": f"Bearer {get_oauth_token()}"}
```

For OAuth client-credentials flow, refresh the token at the top of the notebook
and pass it through. Per-partition refresh works but multiplies token-mint
requests; not recommended unless your IdP allows it.

## Choosing partition count

The constraint is the provider's global rate limit, not Databricks compute:

| Provider rate | NUM_PARTITIONS | per_partition_rate |
|---|---|---|
| Met (~80 req/s, public) | 8 | 10 req/s |
| Stripe (100 req/s, default) | 10 | 10 req/s |
| GitHub authenticated (5000 req/hr ≈ 1.4 req/s) | 2 | 0.7 req/s |
| Shopify basic (2 req/s) | 2 | 1 req/s |
| Salesforce REST (varies, default 100 conc) | 8 | depends |

Higher partition counts give better parallelism inside Spark but do not help
past the provider's cap. If your `per_partition_rate` drops below 1, consider
reducing partitions; the token bucket math breaks down below 1 token/sec.
