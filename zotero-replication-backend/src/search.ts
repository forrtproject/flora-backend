import { DynamoDBClient, GetItemCommand, BatchGetItemCommand } from "@aws-sdk/client-dynamodb";
import Fuse from "fuse.js";

const ddb = new DynamoDBClient({});
const DOI_TABLE = process.env.DOI_TABLE!;
const SEARCH_TABLE = process.env.SEARCH_TABLE!;
const CACHE_TTL_MS = 60 * 60 * 1000;

const ALLOWED_ORIGIN = "https://forrt.org";

function getCorsHeaders(event: any): Record<string, string> {
  const origin = event?.headers?.origin ?? event?.headers?.Origin ?? "";
  return {
    "Access-Control-Allow-Origin": origin === ALLOWED_ORIGIN ? ALLOWED_ORIGIN : "",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Vary": "Origin",
  };
}

interface SearchEntry {
  _doi: string;
  title: string;
  authors: string[];
  rep_authors: string[];
  year: string;
}

let cachedEntries: SearchEntry[] | null = null;
let cachedFuse: Fuse<SearchEntry> | null = null;
let cacheExp = 0;

async function loadSearchIndex(): Promise<SearchEntry[]> {
  if (cachedEntries && Date.now() < cacheExp) return cachedEntries;

  const metaResp = await ddb.send(
    new GetItemCommand({ TableName: SEARCH_TABLE, Key: { chunk_id: { S: "meta" } } })
  );

  const totalChunks = parseInt(metaResp.Item?.data?.S ?? "0", 10);
  if (totalChunks === 0) {
    cachedEntries = [];
    cacheExp = Date.now() + CACHE_TTL_MS;
    return cachedEntries;
  }

  const allEntries: SearchEntry[] = [];
  const chunkIds = Array.from({ length: totalChunks }, (_, i) => String(i));

  for (let i = 0; i < chunkIds.length; i += 100) {
    const batch = chunkIds.slice(i, i + 100);
    let requestItems: any = {
      [SEARCH_TABLE]: { Keys: batch.map((id) => ({ chunk_id: { S: id } })) },
    };

    for (let attempt = 0; attempt < 5 && requestItems; attempt++) {
      const resp = await ddb.send(new BatchGetItemCommand({ RequestItems: requestItems }));

      for (const item of resp.Responses?.[SEARCH_TABLE] ?? []) {
        const dataStr = item.data?.S;
        if (!dataStr) continue;
        try {
          const entries: SearchEntry[] = JSON.parse(dataStr);
          allEntries.push(...entries);
        } catch {}
      }

      requestItems =
        resp.UnprocessedKeys && Object.keys(resp.UnprocessedKeys).length
          ? resp.UnprocessedKeys
          : null;
    }
  }

  cachedEntries = allEntries;
  cachedFuse = null;
  cacheExp = Date.now() + CACHE_TTL_MS;
  return cachedEntries;
}

function getFuse(entries: SearchEntry[]): Fuse<SearchEntry> {
  if (cachedFuse) return cachedFuse;
  cachedFuse = new Fuse(entries, {
    keys: [
      { name: "title",       weight: 0.6  },
      { name: "authors",     weight: 0.25 },
      { name: "rep_authors", weight: 0.05 },
      { name: "year",        weight: 0.1  },
    ],
    useExtendedSearch: true,
    threshold: 0.15,
    distance: 200,
    includeScore: true,
    minMatchCharLength: 2,
    ignoreLocation: true,
  });
  return cachedFuse;
}

const YEAR_RE = /\b(1[89]\d{2}|20\d{2})\b/;

function parseQuery(raw: string): { tokens: string[]; year: string | null } {
  const yearMatch = raw.match(YEAR_RE);
  const year = yearMatch?.[1] ?? null;
  const rest = yearMatch ? raw.replace(yearMatch[0], "") : raw;
  const tokens = rest.split(/\s+/).map((t) => t.trim()).filter((t) => t.length >= 2);
  return { tokens, year };
}

// Convert a user wildcard pattern to a regex for the exclude-only path.
// No wildcards → whole-word match; * → any chars; ? → any one char.
function wildcardToRegex(pattern: string): RegExp {
  const hasWildcard = pattern.includes("*") || pattern.includes("?");
  const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&");
  if (hasWildcard) {
    return new RegExp(escaped.replace(/\*/g, ".*").replace(/\?/g, ".?"), "i");
  }
  return new RegExp(`\\b${escaped}\\b`, "i");
}

function hasWildcard(token: string): boolean {
  return token.includes("*") || token.includes("?");
}

// Translate a user wildcard token to a Fuse.js extended-search include pattern.
// *token* → include-match ('token); token* → prefix (^token); *token → suffix (token$);
// tokens with ? → include-match (Fuse can't express 0-or-1, closest is substring); token → fuzzy
function toFusePattern(token: string): string {
  if (!hasWildcard(token)) return token;
  if (token.startsWith("*") && token.endsWith("*") && token.length > 2)
    return `'${token.slice(1, -1)}`;
  if (token.endsWith("*") && !token.startsWith("*")) return `^${token.slice(0, -1).replace(/\?/g, "")}`;
  if (token.startsWith("*") && !token.endsWith("*")) return `${token.slice(1).replace(/\?/g, "")}$`;
  // Contains ? or mixed wildcards — strip wildcards and use include-match
  return `'${token.replace(/[*?]/g, "")}`;
}

// Translate a user wildcard token to a Fuse.js extended-search exclude pattern.
function toFuseExcludePattern(token: string): string {
  if (!hasWildcard(token)) return `!${token}`;
  if (token.startsWith("*") && token.endsWith("*") && token.length > 2)
    return `!'${token.slice(1, -1)}`;
  if (token.endsWith("*") && !token.startsWith("*")) return `!^${token.slice(0, -1).replace(/\?/g, "")}`;
  if (token.startsWith("*") && !token.endsWith("*")) return `!${token.slice(1).replace(/\?/g, "")}$`;
  return `!'${token.replace(/[*?]/g, "")}`;
}

function termClause(token: string): any {
  const p = toFusePattern(token);
  return { $or: [{ title: p }, { authors: p }, { rep_authors: p }] };
}

// Exclude from ALL fields so the term is truly absent.
function notTermClause(token: string): any {
  const p = toFuseExcludePattern(token);
  return { $and: [{ title: p }, { authors: p }, { rep_authors: p }] };
}

function buildAdvancedExpr(
  mustHave: string[],
  anyOf: string[],
  exclude: string[],
  year: string | null
): any {
  const parts: any[] = [];

  for (const token of mustHave) parts.push(termClause(token));

  if (anyOf.length === 1) {
    parts.push(termClause(anyOf[0]!));
  } else if (anyOf.length > 1) {
    parts.push({ $or: anyOf.map(termClause) });
  }

  for (const token of exclude) parts.push(notTermClause(token));

  if (year) parts.push({ year: `=${year}` });

  if (parts.length === 0) return null;
  if (parts.length === 1) return parts[0];
  return { $and: parts };
}

type FuseHit = { item: SearchEntry; score?: number; refIndex: number };

function filterByYearRange(hits: FuseHit[], yearFrom?: number, yearTo?: number): FuseHit[] {
  if (yearFrom == null && yearTo == null) return hits;
  return hits.filter((h) => {
    const y = parseInt(h.item.year, 10);
    if (isNaN(y)) return true;
    if (yearFrom != null && y < yearFrom) return false;
    if (yearTo != null && y > yearTo) return false;
    return true;
  });
}

function filterByPaperTypes(records: Record<string, any>, paperTypes?: string[]): Record<string, any> {
  if (!paperTypes || paperTypes.length === 0) return records;
  const normalized = paperTypes.map((t) => t.toLowerCase());
  const filtered: Record<string, any> = {};

  for (const [doi, record] of Object.entries(records)) {
    const types: string[] = (record?.types ?? []).map((t: string) => t.toLowerCase());

    if (types.length === 0) {
      const replications  = record?.record?.replications  ?? [];
      const originals     = record?.record?.originals     ?? [];
      const reproductions = record?.record?.reproductions ?? [];
      if (replications.length > 0 || reproductions.length > 0) types.push("original");
      if (originals.length > 0) types.push("replication", "reproduction");
    }

    const match = normalized.some((t) => {
      if (t === "original") {
        // include papers that ARE originals OR that link back to originals (replications/reproductions)
        return types.includes("original") ||
          (record?.record?.originals ?? []).length > 0;
      }
      if (t === "replication" || t === "reproduction") {
        // include papers that ARE replications/reproductions OR originals that have them
        return types.includes(t) ||
          (record?.record?.replications  ?? []).length > 0 ||
          (record?.record?.reproductions ?? []).length > 0;
      }
      return types.includes(t);
    });

    if (match) filtered[doi] = record;
  }
  return filtered;
}

function filterByOutcomes(records: Record<string, any>, outcomes?: string[]): Record<string, any> {
  if (!outcomes || outcomes.length === 0) return records;
  const normalized = outcomes.map((o) => o.toLowerCase());
  const filtered: Record<string, any> = {};
  for (const [doi, record] of Object.entries(records)) {
    const recordOutcomes: string[] = (record?.record?.replications ?? [])
      .map((r: any) => r?.outcome)
      .filter(Boolean)
      .map((o: string) => o.toLowerCase());
    // All replication outcomes must be within the selected set (no mix of other outcomes)
    if (recordOutcomes.length > 0 && recordOutcomes.every((o) => normalized.includes(o))) {
      filtered[doi] = record;
    }
  }
  return filtered;
}

function computeCitationGraphFields(rec: any) {
  const replications: any[] = rec?.record?.replications ?? [];
  const outcome_mix: Record<string, number> = {};
  const citation_timeline: Record<string, number> = {};
  let first_year: number | null = null;
  let first_outcome: string | null = null;

  for (const rep of replications) {
    const outcome: string | undefined = rep.outcome;
    if (outcome) outcome_mix[outcome] = (outcome_mix[outcome] ?? 0) + 1;
    const y = rep.year != null ? parseInt(String(rep.year), 10) : NaN;
    if (!isNaN(y)) {
      citation_timeline[String(y)] = (citation_timeline[String(y)] ?? 0) + 1;
      if (first_year === null || y < first_year) { first_year = y; first_outcome = outcome ?? null; }
    }
  }

  return {
    outcome_mix,
    replication_year_counts: citation_timeline,
    first_replication_year: first_year !== null ? String(first_year) : null,
    first_replication_outcome: first_outcome,
  };
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function hydrateDois(dois: string[], score = 0): Promise<Record<string, any>> {
  const out: Record<string, any> = {};
  if (!dois.length) return out;

  for (const part of chunk(dois, 100)) {
    let requestItems: any = {
      [DOI_TABLE]: {
        Keys: part.map((doi) => ({ doi: { S: doi } })),
        ProjectionExpression: "doi,#rec",
        ExpressionAttributeNames: { "#rec": "record" },
      },
    };

    for (let attempt = 0; attempt < 5 && requestItems; attempt++) {
      const resp = await ddb.send(new BatchGetItemCommand({ RequestItems: requestItems }));

      for (const it of resp.Responses?.[DOI_TABLE] ?? []) {
        const doi = it.doi?.S;
        const recStr = it.record?.S;
        if (!doi) continue;
        if (recStr) {
          const rec = JSON.parse(recStr);
          out[doi] = { ...rec, score, ...computeCitationGraphFields(rec) };
        }
      }

      requestItems =
        resp.UnprocessedKeys && Object.keys(resp.UnprocessedKeys).length
          ? resp.UnprocessedKeys
          : null;
    }
  }

  return out;
}

async function hydrateRecords(hits: FuseHit[]): Promise<Record<string, any>> {
  const scores: Record<string, number> = {};
  const dois: string[] = [];

  for (const h of hits) {
    if (!h.item._doi) continue;
    const doi = h.item._doi;
    if (scores[doi] === undefined) dois.push(doi);
    if (scores[doi] === undefined || (h.score ?? 1) < scores[doi]) {
      scores[doi] = h.score ?? 1;
    }
  }

  if (!dois.length) return {};

  const out = await hydrateDois(dois);
  for (const doi of dois) {
    if (out[doi]) out[doi].score = scores[doi];
  }
  return out;
}

interface SearchParams {
  query: string;
  mustHave: string[];
  anyOf: string[];
  exclude: string[];
  yearFrom?: number;
  yearTo?: number;
  outcomes?: string[];
  paperTypes?: string[];
  limit: number;
  offset: number;
}

function toStringArray(val: unknown): string[] {
  if (Array.isArray(val)) return val.map(String).map((s) => s.trim()).filter(Boolean);
  if (typeof val === "string" && val.trim()) return [val.trim()];
  return [];
}

function clampInt(val: number, min: number, max: number, fallback: number): number {
  const n = Math.floor(val);
  return isNaN(n) ? fallback : Math.min(Math.max(n, min), max);
}

function extractSearchParams(event: any): SearchParams {
  let query = "";
  let mustHave: string[] = [];
  let anyOf: string[] = [];
  let exclude: string[] = [];
  let yearFrom: number | undefined;
  let yearTo: number | undefined;
  let outcomes: string[] | undefined;
  let paperTypes: string[] | undefined;
  let limit = 1000;
  let offset = 0;

  if (event?.body) {
    try {
      const body = JSON.parse(event.body);
      if (typeof body?.query === "string") query = body.query.trim();
      if (typeof body?.q === "string" && !query) query = body.q.trim();
      mustHave = toStringArray(body?.mustHave);
      anyOf    = toStringArray(body?.anyOf);
      exclude  = toStringArray(body?.exclude);
      if (typeof body?.yearFrom === "number") yearFrom = body.yearFrom;
      if (typeof body?.yearTo   === "number") yearTo   = body.yearTo;
      if (Array.isArray(body?.outcomes))   outcomes   = body.outcomes.map(String);
      if (Array.isArray(body?.paperTypes)) paperTypes = body.paperTypes.map(String);
      if (typeof body?.limit  === "number") limit  = clampInt(body.limit,  1, 1000, 1000);
      if (typeof body?.offset === "number") offset = clampInt(body.offset, 0, Infinity, 0);
    } catch {}
  }

  const qsp = event?.queryStringParameters;
  if (!query && qsp?.query) query = String(qsp.query).trim();
  if (!query && qsp?.q)     query = String(qsp.q).trim();
  if (!yearFrom   && qsp?.yearFrom)   yearFrom   = parseInt(qsp.yearFrom, 10) || undefined;
  if (!yearTo     && qsp?.yearTo)     yearTo     = parseInt(qsp.yearTo,   10) || undefined;
  if (!outcomes   && qsp?.outcomes)   outcomes   = String(qsp.outcomes).split(",").map((s) => s.trim()).filter(Boolean);
  if (!paperTypes && qsp?.paperTypes) paperTypes = String(qsp.paperTypes).split(",").map((s) => s.trim()).filter(Boolean);
  if (qsp?.limit)  limit  = clampInt(parseInt(qsp.limit,  10), 1, 1000, 1000);
  if (qsp?.offset) offset = clampInt(parseInt(qsp.offset, 10), 0, Infinity, 0);

  return { query, mustHave, anyOf, exclude, yearFrom, yearTo, outcomes, paperTypes, limit, offset };
}

export const handler = async (event: any) => {
  const cors = getCorsHeaders(event);
  try {
    const method = event?.requestContext?.http?.method || event?.httpMethod || "GET";
    if (method === "OPTIONS") return { statusCode: 200, headers: cors, body: "" };

    const { query, mustHave, anyOf, exclude, yearFrom, yearTo, outcomes, paperTypes, limit, offset } = extractSearchParams(event);

    const isAdvanced = mustHave.length > 0 || anyOf.length > 0 || exclude.length > 0;
    const hasInput = query || isAdvanced;

    if (!hasInput) {
      return {
        statusCode: 400,
        headers: { ...cors, "Content-Type": "application/json" },
        body: JSON.stringify({ error: 'Provide a search term via "query", "mustHave", "anyOf", or "exclude"' }),
      };
    }

    const entries = await loadSearchIndex();
    const fuse = getFuse(entries);

    const excludeOnly = exclude.length > 0 && mustHave.length === 0 && anyOf.length === 0 && !query;

    let hits: FuseHit[];

    if (excludeOnly) {
      const excludeRegexes = exclude.map((e) => wildcardToRegex(e));
      const filtered = entries.filter((e) => {
        const text = [e.title, ...e.authors, ...e.rep_authors].join(" ");
        return !excludeRegexes.some((re) => re.test(text));
      });
      hits = filtered.map((item, refIndex) => ({ item, score: 0, refIndex }));
    } else {
      let expr: any;
      if (isAdvanced) {
        expr = buildAdvancedExpr(mustHave, anyOf, exclude, null);
      } else {
        const { tokens, year } = parseQuery(query);
        expr = buildAdvancedExpr(tokens, [], [], year);
      }

      if (!expr) {
        return {
          statusCode: 200,
          headers: { ...cors, "Content-Type": "application/json", "Cache-Control": "public, max-age=300" },
          body: JSON.stringify({ query, total: 0, results: {} }),
        };
      }

      hits = fuse.search(expr);
    }

    hits = filterByYearRange(hits, yearFrom, yearTo);

    const total = hits.length;
    const page = hits.slice(offset, offset + limit);

    let fullRecords = await hydrateRecords(page);
    fullRecords = filterByPaperTypes(fullRecords, paperTypes);
    fullRecords = filterByOutcomes(fullRecords, outcomes);

    // Expand: only when paperTypes is set — pulls in linked papers in both directions.
    // Skipped for general text/exclude searches to avoid ballooning response size past Lambda's 6MB limit.
    if (paperTypes && paperTypes.length > 0) {
      const linkedDois = new Set<string>();

      for (const record of Object.values(fullRecords)) {
        for (const rep of record?.record?.replications ?? []) {
          if (rep?.doi && !fullRecords[rep.doi]) linkedDois.add(rep.doi);
        }
        for (const rep of record?.record?.reproductions ?? []) {
          if (rep?.doi && !fullRecords[rep.doi]) linkedDois.add(rep.doi);
        }
        for (const orig of record?.record?.originals ?? []) {
          if (orig?.doi && !fullRecords[orig.doi]) linkedDois.add(orig.doi);
        }
      }

      if (linkedDois.size > 0) {
        const extra = await hydrateDois([...linkedDois]);
        Object.assign(fullRecords, extra);
      }
    }

    // Re-apply filters to expanded records.
    // filterByOutcomes only applies to papers that have replications (originals);
    // pure replication papers have no record.replications so they pass through.
    fullRecords = filterByPaperTypes(fullRecords, paperTypes);
    if (outcomes && outcomes.length > 0) {
      const normalized = outcomes.map((o) => o.toLowerCase());
      fullRecords = Object.fromEntries(
        Object.entries(fullRecords).filter(([, record]) => {
          const recordOutcomes: string[] = (record?.record?.replications ?? [])
            .map((r: any) => r?.outcome)
            .filter(Boolean)
            .map((o: string) => o.toLowerCase());
          // Replication papers (no sub-replications) pass through; only filter originals
          return recordOutcomes.length === 0 || recordOutcomes.every((o) => normalized.includes(o));
        })
      );
    }

    return {
      statusCode: 200,
      headers: { ...cors, "Content-Type": "application/json", "Cache-Control": "public, max-age=300" },
      body: JSON.stringify({
        query,
        total,
        offset,
        limit,
        hasMore: offset + limit < total,
        results: fullRecords,
      }),
    };
  } catch (err: any) {
    console.error("search error:", err);
    return {
      statusCode: 500,
      headers: { ...cors, "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Internal Server Error", details: err?.message || String(err) }),
    };
  }
};
