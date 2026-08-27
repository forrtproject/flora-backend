# FLORA Backend 

Serverless backend for the FORRT Replication Checker — the service behind the Zotero
plugin and [forrt.org](https://forrt.org) that tells you whether a paper in your library
has been replicated, reproduced, or contradicted.

It ingests the FORRT Replication Database (FLoRA) as a CSV, flattens it into a
DOI-centric record store in DynamoDB, and exposes four read-only HTTP endpoints on
AWS Lambda.

## Contents

- [Architecture](#architecture)
- [API](#api)
- [Data model](#data-model)
- [Getting started](#getting-started)
- [Deploying](#deploying)
- [Seeding the database (ETL)](#seeding-the-database-etl)
- [Repository layout](#repository-layout)
- [Notes and gotchas](#notes-and-gotchas)

## Architecture

```
FLoRA CSV ──► etl/seed_prefixes.py ──► DynamoDB ─┬─ *-prefix   (hash prefix → DOIs)
                                                ├─ *-doi      (DOI → full record)
                                                ├─ *-search   (chunked search index)
                                                └─ *-sets     (short id → DOIs, written by the API)
                                                       │
Zotero plugin / forrt.org ──► HTTP API (v2) ──► Lambda ─┘
```

- **Runtime:** Node.js 24 on AWS Lambda, region `eu-central-1`
- **Gateway:** API Gateway HTTP API (v2), CORS enabled
- **Storage:** four on-demand DynamoDB tables, no provisioned capacity
- **Build:** Serverless Framework v3 with `serverless-esbuild` (bundled, minified, source-mapped)
- **IAM:** the Lambda role has `GetItem` and `BatchGetItem` on the three data tables and nothing else — the FLoRA data itself can never be written by the API. `PutItem` is granted only on `*-sets`, which holds client-submitted DOI lists.

Privacy is the reason for the prefix table. The Zotero client never sends the DOIs in
your library; it hashes each one and sends only the first three characters of the hash.
The server answers with every DOI record under those prefixes, and the client picks out
the ones it actually asked about.

## API

Base URL is the API Gateway endpoint printed by `serverless deploy`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET`/`POST` | `/v1/prefix-lookup` | Hash prefixes → full DOI records |
| `GET`/`POST` | `/v1/original-lookup` | DOIs → full DOI records |
| `GET` | `/v1/dois` | Every DOI in the database (identifiers only) |
| `GET`/`POST` | `/v1/search` | Fuzzy search over title, authors, and year |
| `POST` | `/v1/sets` | Store a list of DOIs, get back a short id |
| `GET` | `/v1/sets/{id}` | Read a stored list back (expires after 30 days) |

Every endpoint accepts parameters either as a JSON body or as a query string, handles
`OPTIONS` preflight, and returns `{ "error": ... }` with a 400 or 500 on failure.

### `POST /v1/prefix-lookup`

```bash
curl -X POST "$API/v1/prefix-lookup" \
  -H 'Content-Type: application/json' \
  -d '{"prefixes": ["198", "30e"]}'

# GET forms: ?prefixes=198,30e  |  ?prefix=198&prefix=30e
```

```json
{ "results": { "198": [ { "doi": "...", "record": { ... } } ], "30e": [] } }
```

Capped at 200 prefixes per request and 5000 hydrated DOIs per response. Responses are
cacheable for one hour.

### `POST /v1/original-lookup`

```bash
curl -X POST "$API/v1/original-lookup" \
  -H 'Content-Type: application/json' \
  -d '{"dois": ["10.1037/a0012833"]}'
```

DOIs are normalised first, so `https://doi.org/10.x/y`, `doi:10.x/y`, and `10.X/Y` all
resolve to the same key. Returns a `{ doi: record | null }` map — `null` means the DOI is
not in the database. Capped at 200 DOIs per request.

### `GET /v1/dois`

```json
{ "total": 12345, "dois": ["10.1037/a0012833", "..."] }
```

Identifiers only, intended for clients that want to filter locally before asking for
records.

### `POST /v1/search`

Fuzzy search powered by [Fuse.js](https://fusejs.io) over the precomputed search index,
weighted title `0.6`, authors `0.25`, year `0.1`, replication authors `0.05`.

```bash
curl -X POST "$API/v1/search" \
  -H 'Content-Type: application/json' \
  -d '{"query": "social priming 2012", "limit": 50}'
```

| Field | Type | Notes |
| --- | --- | --- |
| `query` (or `q`) | string | Free text; a bare 1800–2099 year in the string is extracted as a year filter |
| `mustHave` | string[] | All terms must match (AND) |
| `anyOf` | string[] | At least one term must match (OR) |
| `exclude` | string[] | Terms that must be absent from all fields |
| `yearFrom` / `yearTo` | number | Inclusive range; entries with an unparseable year are kept |
| `paperTypes` | string[] | `original`, `replication`, `reproduction` |
| `outcomes` | string[] | Keeps records whose replication outcomes all fall in the set |
| `limit` / `offset` | number | `limit` clamped to 1–1000 (default 1000) |

`mustHave`, `anyOf`, and `exclude` support wildcards: `term*` (prefix), `*term` (suffix),
`*term*` (contains), `?` (single character).

```json
{ "query": "...", "total": 87, "offset": 0, "limit": 50, "hasMore": true, "results": { "10.x/y": { ... } } }
```

Setting `paperTypes` also expands the result set with linked papers in both directions —
an original pulls in its replications and reproductions, a replication pulls in its
originals.

### `POST /v1/sets`

Stores a list of DOIs under a short id so a large selection can travel as `?set=ab12cd34`
instead of a query string long enough to trip the URI length limit. Sets are scratch
space, not permalinks: the client re-`POST`s the list whenever it needs an id.

```bash
curl -X POST "$API/v1/sets" \
  -H 'Content-Type: application/json' \
  -d '{"dois": ["10.1037/xge0001132", "10.1016/j.jesp.2015.10.012"]}'
```

```json
{ "id": "ab12cd34", "count": 312, "created": "2026-08-27T10:14:02Z", "expires": "2026-09-26T10:14:02Z" }
```

DOIs are normalised and deduplicated the same way `/v1/original-lookup` does them, so
`count` can be lower than the number sent. Capped at 5000 DOIs per set. The body must be
JSON with a non-empty `dois` array — no query-string form.

### `GET /v1/sets/{id}`

```json
{ "id": "ab12cd34", "dois": ["10.1037/xge0001132", "..."], "count": 312, "created": "2026-08-27T10:14:02Z", "expires": "2026-09-26T10:14:02Z" }
```

A set is immutable but not permanent, so the `immutable` cache header carries a `max-age`
of whatever is left of its 30 days — never long enough to outlive the record.

Unknown, malformed, and expired ids all answer `404` with the same body:

```json
{ "error": "This link has expired. Please generate it again.", "code": "set_expired" }
```

They share one response because once DynamoDB has swept a row, an expired id and one that
never existed are indistinguishable — and the client's move is the same either way.
**Treat `404` as "re-`POST` the list", not as an error.** Branch on `code`, not on the
`error` sentence, which is meant for humans and may be reworded.

## Data model

Four tables, all `PAY_PER_REQUEST`, named `zotero-replication-backend-<stage>-<suffix>`:

**`*-prefix`** — key `prefix` (S)

```json
{ "prefix": "198", "dois": ["10.1037/a0012833", "..."] }
```

**`*-doi`** — key `doi` (S). `record` is a JSON *string*, not a nested map.

```json
{
  "doi": "10.1037/a0012833",
  "record": "{\"types\":[\"original\"],\"record\":{\"originals\":[],\"replications\":[],\"reproductions\":[],\"stats\":{...}}}"
}
```

A DOI may hold more than one role in `types` — the same paper can be an original for one
study and a replication of another. Each record carries `stats` (counts of replications,
reproductions, and originals, with and without DOIs) plus derived fields seeded by the
ETL (`outcome_mix`, `replication_year_counts`, `first_replication_year`,
`first_replication_outcome`) and by the OpenCitations pipeline (`citation_timeline`,
`n_citations`).

Those derived fields are stored *inside* `record` but returned at the **top level** of the
API response, and [src/recordFields.ts](src/recordFields.ts) strips the nested copies on
the way out so each key ships exactly once. Everything else in `record` — `originals`,
`replications`, `reproductions`, `stats` — is passed through untouched. The handlers also
recompute the derived fields at read time, so the response shape stays consistent even for
rows seeded by an older ETL run.

**`*-search`** — key `chunk_id` (S)

```json
{ "chunk_id": "meta", "data": "42" }
{ "chunk_id": "0",    "data": "[{\"_doi\":\"...\",\"title\":\"...\",\"authors\":[],\"rep_authors\":[],\"year\":\"2012\"}]" }
```

`meta` holds the chunk count; chunks `0…n-1` hold ~500 compact entries each. Lambda loads
every chunk on a cold start and caches the assembled index — and the built Fuse instance —
in memory for one hour.

**`*-sets`** — key `id` (S). The only table the API writes to, and the only one the ETL
does not touch. `dois` is a JSON *string*, like `record` above.

```json
{ "id": "ab12cd34", "dois": "[\"10.1037/xge0001132\"]", "count": 1,
  "created": "2026-08-27T10:14:02Z", "ttl": 1790503242 }
```

Ids are 8 random hex characters, written with `attribute_not_exists(id)` so a collision
retries rather than overwriting an existing set. `ttl` is epoch seconds, 30 days after
creation, and DynamoDB TTL is enabled on the table.

## Getting started

Requirements: Node.js 24+, an AWS account, and credentials with permission to deploy
CloudFormation, Lambda, API Gateway, and DynamoDB.

```bash
npm install
npx serverless config credentials --provider aws --key <KEY> --secret <SECRET>
```

Invoke a function locally against your deployed tables:

```bash
npm run invoke:prefix     # prefixLookup with {"prefixes":["a1b"]}
npm run invoke:original   # originalLookup with {"dois":["10.1234/abc"]}
npm run invoke:search     # fuzzySearch with {"query":"social priming"}
npm run invoke:sets       # setsCreate with {"dois":["10.1234/abc"]}
```

Local invocation needs `PREFIX_TABLE`, `DOI_TABLE`, `SEARCH_TABLE`, and `SETS_TABLE` in the
environment; `serverless invoke local` reads them from `serverless.yml`, so the tables
must already exist in your account.

## Deploying

```bash
npm run build     # serverless package — bundle only, no upload
npm run deploy    # serverless deploy — creates/updates the whole stack
npm run clean     # remove .serverless and dist
```

The first deploy creates the four DynamoDB tables along with the functions, then prints
the API Gateway base URL. Deploy to another stage with
`npx serverless deploy --stage prod` — table names carry the stage, so stages are fully
isolated.

## Seeding the database (ETL)

[`etl/seed_prefixes.py`](etl/seed_prefixes.py) reads a FLoRA export and populates all three
tables in one pass.

```bash
pip install boto3 pandas ftfy

export AWS_REGION=eu-central-1
export PREFIX_TABLE=zotero-replication-backend-dev-prefix
export DOI_TABLE=zotero-replication-backend-dev-doi
export SEARCH_TABLE=zotero-replication-backend-dev-search

python etl/seed_prefixes.py path/to/FLoRA.csv
```

The CSV must have `doi_o`, `doi_o_hash`, and `type` columns; `doi_r` and `doi_r_hash` are
used when present. Rows with an original but no replication DOI are still recorded, and
deduplicated on a fingerprint of their title, year, journal, references, and outcome.

| Variable | Default | Meaning |
| --- | --- | --- |
| `PREFIX_TABLE`, `DOI_TABLE`, `SEARCH_TABLE` | — | Required; the script exits if any is unset |
| `AWS_REGION` | `eu-central-1` | |
| `PREFIX_LEN` | `3` | Characters of the DOI hash used as the prefix key |
| `SEARCH_CHUNK_SIZE` | `500` | Entries per search-index chunk |
| `CLEAR_TABLES` | `1` | **Wipes all three tables before writing.** Set to `0` to append |

`CLEAR_TABLES` defaults to on, so a plain run is a full rebuild, not an incremental
update. Set it to `0` if that is not what you want.

## Repository layout

```
src/
  handler.ts    prefixLookup   — GET/POST /v1/prefix-lookup
  original.ts   originalLookup — GET/POST /v1/original-lookup
  dois.ts       doiList        — GET      /v1/dois
  search.ts     fuzzySearch    — GET/POST /v1/search
  sets.ts       setsCreate     — POST     /v1/sets
                setsGet        — GET      /v1/sets/{id}
  doi.ts        DOI normalisation shared by the lookup endpoints
etl/
  seed_prefixes.py             FLoRA CSV → DynamoDB
serverless.yml                 Functions, routes, IAM, table definitions
tsconfig.json
```

## Notes and gotchas

- **CORS is not uniform.** `/v1/search` only reflects an `Access-Control-Allow-Origin` for
  `https://forrt.org` (hardcoded in [src/search.ts](src/search.ts#L9)); the other three
  endpoints answer `*`. A new frontend origin needs that constant updated.
- **`/v1/dois` reads the search table,** not the DOI table — it lists the DOIs that made it
  into the search index, which is what clients want to filter against.
- **Lambda has a 6 MB response limit.** It is why search paginates at 1000 and why linked-paper
  expansion only runs when `paperTypes` is set.
- **Caches are per-container.** The one-hour index cache lives in Lambda memory, so a reseed is
  not visible everywhere at once; cold containers pick it up immediately, warm ones within
  the hour.
- **`record` is a JSON string.** Clients must `JSON.parse` the inner payload — the handlers
  do this for you on the way out, but the table itself stores text.
- **Sets expire after 30 days,** and `GET /v1/sets/{id}` enforces that itself rather than
  trusting the table. DynamoDB deletes expired items lazily — up to ~48h late — so a swept-late
  row would otherwise stay readable past its date. The handler 404s the moment `ttl` passes;
  the sweeper only reclaims the storage.
- No test suite yet: `npm test` is a stub.

## License

ISC.
