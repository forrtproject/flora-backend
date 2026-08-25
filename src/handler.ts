import { DynamoDBClient, GetItemCommand, BatchGetItemCommand } from "@aws-sdk/client-dynamodb";

const ddb = new DynamoDBClient({});
const PREFIX_TABLE = process.env.PREFIX_TABLE!;
const DOI_TABLE = process.env.DOI_TABLE!;

const cors = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

function extractPrefixes(event: any): string[] {
  let prefixes: string[] = [];

  // POST: { "prefixes": ["198","30e"] }
  if (event?.body) {
    try {
      const body = JSON.parse(event.body);
      if (Array.isArray(body?.prefixes)) prefixes = body.prefixes;
    } catch {}
  }

  // GET: ?prefixes=198,30e
  if (!prefixes.length && event?.queryStringParameters?.prefixes) {
    prefixes = String(event.queryStringParameters.prefixes).split(",");
  }

  // GET multi: ?prefix=198&prefix=30e OR ?prefixes=198&prefixes=30e
  const mv = event?.multiValueQueryStringParameters;
  if (!prefixes.length && mv) {
    if (Array.isArray(mv.prefixes) && mv.prefixes.length) prefixes = mv.prefixes;
    else if (Array.isArray(mv.prefix) && mv.prefix.length) prefixes = mv.prefix;
  }

  return Array.from(new Set(prefixes.map((p) => String(p).trim().toLowerCase()).filter(Boolean))).slice(0, 200);
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function computeCitationGraphFields(rec: any) {
  const replications: any[] = rec?.record?.replications ?? [];
  const outcome_mix: Record<string, number> = {};
  let first_year: number | null = null;
  let first_outcome: string | null = null;

  for (const rep of replications) {
    const outcome: string | undefined = rep.outcome;
    if (outcome) outcome_mix[outcome] = (outcome_mix[outcome] ?? 0) + 1;
    const y = rep.year != null ? parseInt(String(rep.year), 10) : NaN;
    if (!isNaN(y)) {
      if (first_year === null || y < first_year) { first_year = y; first_outcome = outcome ?? null; }
    }
  }

  return {
    outcome_mix,
    first_replication_year: first_year !== null ? String(first_year) : null,
    first_replication_outcome: first_outcome,
  };
}

function decodeDoisAttr(item: any): string[] {
  if (item?.dois?.L) return item.dois.L.map((x: any) => x?.S).filter(Boolean);
  if (item?.dois?.S) {
    try {
      const arr = JSON.parse(item.dois.S);
      if (Array.isArray(arr)) return arr.map(String);
    } catch {}
  }
  return [];
}

async function batchGetDois(dois: string[]): Promise<Record<string, any | null>> {
  const out: Record<string, any | null> = {};
  for (const d of dois) out[d] = null;

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

      const items = resp.Responses?.[DOI_TABLE] || [];
      for (const it of items) {
        const doi = it.doi?.S;
        const recStr = it.record?.S;
        if (!doi) continue;
        if (recStr) { const rec = JSON.parse(recStr); out[doi] = { ...rec, ...computeCitationGraphFields(rec) }; }
      }

      requestItems = resp.UnprocessedKeys && Object.keys(resp.UnprocessedKeys).length ? resp.UnprocessedKeys : null;
    }
  }

  return out;
}

export const handler = async (event: any) => {
  try {
    const method = event?.requestContext?.http?.method || event?.httpMethod || "GET";
    if (method === "OPTIONS") return { statusCode: 200, headers: cors, body: "" };

    const prefixes = extractPrefixes(event);
    if (!prefixes.length) {
      return {
        statusCode: 400,
        headers: { ...cors, "Content-Type": "application/json" },
        body: JSON.stringify({ error: "No prefixes provided" }),
      };
    }

    // 1) prefix -> dois
    const prefixToDois: Record<string, string[]> = {};
    for (const p of prefixes) prefixToDois[p] = [];

    await Promise.all(
      prefixes.map(async (p) => {
        const res = await ddb.send(
          new GetItemCommand({
            TableName: PREFIX_TABLE,
            Key: { prefix: { S: p } },
            ProjectionExpression: "dois",
          })
        );
        prefixToDois[p] = res.Item ? decodeDoisAttr(res.Item) : [];
      })
    );

    // 2) hydrate all DOIs
    const allDois = Array.from(new Set(Object.values(prefixToDois).flat())).slice(0, 5000);
    const doiToRecord = await batchGetDois(allDois);

    // 3) results per prefix (return parsed DOI records directly)
    const results: Record<string, any[]> = {};
    for (const p of prefixes) {
      const dois = prefixToDois[p] ?? [];
      results[p] = dois.map((doi) => doiToRecord[doi]).filter(Boolean) as any[];
    }

    return {
      statusCode: 200,
      headers: { ...cors, "Content-Type": "application/json", "Cache-Control": "public, max-age=3600" },
      body: JSON.stringify({ results }),
    };
  } catch (err: any) {
    console.error("prefix-lookup error:", err);
    return {
      statusCode: 500,
      headers: { ...cors, "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Internal Server Error", details: err?.message || String(err) }),
    };
  }
};
