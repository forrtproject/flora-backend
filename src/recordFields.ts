/**
 * Derived fields attached to every DOI record before it leaves the API.
 *
 * All three lookup endpoints (prefix-lookup, original-lookup, search) return the
 * same shape. This lived as three separate copies that drifted:
 * replication_year_counts was only ever returned by /v1/search.
 */

export type DerivedRecordFields = {
  outcome_mix: Record<string, number>;
  replication_year_counts: Record<string, number>;
  first_replication_year: string | null;
  first_replication_outcome: string | null;
  citation_timeline?: unknown;
  n_citations?: number;
};

/** Reads `apiEmail` from a POST body or a GET query string. */
export function extractApiEmail(event: any): string | null {
  let email: unknown;

  if (event?.body) {
    try {
      email = JSON.parse(event.body)?.apiEmail;
    } catch {}
  }
  if (typeof email !== "string" || !email) {
    email = event?.queryStringParameters?.apiEmail;
  }
  if (typeof email !== "string" || !email) {
    const mv = event?.multiValueQueryStringParameters?.apiEmail;
    if (Array.isArray(mv) && mv.length) email = mv[0];
  }

  return typeof email === "string" && email.trim()
    ? email.trim().toLowerCase()
    : null;
}

/**
 * Derived fields ship only to anonymous callers. Any caller that identifies
 * itself with `apiEmail` — the Zotero plugin included — gets the trimmed
 * payload of stored data only, which matters most on prefix-lookup, where one
 * response can carry thousands of records.
 */
export function includeDerivedFields(event: any): boolean {
  return extractApiEmail(event) === null;
}

export function computeRecordFields(
  rec: any,
  includeCitations = true,
): DerivedRecordFields {
  const replications: any[] = rec?.record?.replications ?? [];
  const outcome_mix: Record<string, number> = {};
  const replication_year_counts: Record<string, number> = {};
  let first_year: number | null = null;
  let first_outcome: string | null = null;

  for (const rep of replications) {
    const outcome: string | undefined = rep.outcome;
    if (outcome) outcome_mix[outcome] = (outcome_mix[outcome] ?? 0) + 1;

    const y = rep.year != null ? parseInt(String(rep.year), 10) : NaN;
    if (!isNaN(y)) {
      replication_year_counts[String(y)] = (replication_year_counts[String(y)] ?? 0) + 1;
      if (first_year === null || y < first_year) {
        first_year = y;
        first_outcome = outcome ?? null;
      }
    }
  }

  const out: DerivedRecordFields = {
    outcome_mix,
    replication_year_counts,
    first_replication_year: first_year !== null ? String(first_year) : null,
    first_replication_outcome: first_outcome,
  };

  // refresh_data.py writes these alongside `record`, not inside it; the nested
  // lookup is a fallback. Absent until that pipeline has run.
  if (includeCitations) {
    const timeline = rec?.citation_timeline ?? rec?.record?.citation_timeline;
    const count = rec?.n_citations ?? rec?.record?.n_citations;
    if (timeline !== undefined) out.citation_timeline = timeline;
    if (count !== undefined) out.n_citations = count;
  }

  return out;
}

/** Seeded into `record` by the ETL; the API recomputes them and returns them at the top level instead. */
const PROMOTED_KEYS = [
  "outcome_mix",
  "replication_year_counts",
  "first_replication_year",
  "first_replication_outcome",
] as const;

const CITATION_KEYS = ["citation_timeline", "n_citations"] as const;

/**
 * Builds the outgoing record. Every derived key is cleared from the stored blob
 * first — the ETL nests its copies inside `record`, refresh_data.py writes the
 * citation pair alongside it — then re-attached once at the top level, or left
 * off entirely for identified callers.
 */
export function serializeRecord(rec: any, includeDerived = true): any {
  const out: Record<string, any> = { ...rec };
  for (const key of CITATION_KEYS) delete out[key];

  const inner = out.record;
  if (inner && typeof inner === "object") {
    const rest: Record<string, any> = { ...inner };
    for (const key of [...PROMOTED_KEYS, ...CITATION_KEYS]) delete rest[key];
    out.record = rest;
  }

  return includeDerived ? { ...out, ...computeRecordFields(rec, true) } : out;
}
