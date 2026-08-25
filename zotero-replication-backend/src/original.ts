import { DynamoDBClient, BatchGetItemCommand } from "@aws-sdk/client-dynamodb";

const ddb = new DynamoDBClient({});
const DOI_TABLE = process.env.DOI_TABLE!;

const cors = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

function normDoi(s: string) {
  if (!s) return "";
  let x = s.trim().toLowerCase();
  for (const p of [
    "https://doi.org/",
    "http://doi.org/",
    "https://dx.doi.org/",
    "http://dx.doi.org/",
    "doi:",
  ]) {
    x = x.replace(p, "");
  }
  return x.trim();
}

function extractDois(event: any): string[] {
  let dois: string[] = [];

  // POST: { "dois": ["10..","10.."] }
  if (event?.body) {
    try {
      const body = JSON.parse(event.body);
      if (Array.isArray(body?.dois)) dois = body.dois;
    } catch {}
  }

  // GET: ?dois=10..,10..
  if (!dois.length && event?.queryStringParameters?.dois) {
    dois = String(event.queryStringParameters.dois).split(",");
  }

  // GET multi: ?dois=...&dois=...
  const mv = event?.multiValueQueryStringParameters;
  if (!dois.length && Array.isArray(mv?.dois) && mv.dois.length) {
    dois = mv.dois;
  }

  return Array.from(new Set(dois.map((d) => normDoi(String(d))).filter(Boolean))).slice(0, 200);
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

async function batchGetDois(dois: string[]): Promise<Record<string, any | null>> {
  const out: Record<string, any | null> = {};
  for (const d of dois) out[d] = null;

  for (const part of chunk(dois, 100)) {
    let requestItems: any = {
      [DOI_TABLE]: {
        Keys: part.map((doi) => ({ doi: { S: doi } })),
        ProjectionExpression: "doi,#rec",
        ExpressionAttributeNames: { "#rec": "record" }, // "record" is a reserved keyword
      },
    };

    // Retry UnprocessedKeys a few times
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

    const dois = extractDois(event);
    if (!dois.length) {
      return {
        statusCode: 400,
        headers: { ...cors, "Content-Type": "application/json" },
        body: JSON.stringify({ error: "No DOIs provided" }),
      };
    }

    const results = await batchGetDois(dois);

    return {
      statusCode: 200,
      headers: { ...cors, "Content-Type": "application/json", "Cache-Control": "public, max-age=3600" },
      body: JSON.stringify({ results }),
    };
  } catch (err: any) {
    console.error("original-lookup error:", err);
    return {
      statusCode: 500,
      headers: { ...cors, "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Internal Server Error", details: err?.message || String(err) }),
    };
  }
};
