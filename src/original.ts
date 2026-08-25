import { DynamoDBClient, BatchGetItemCommand } from "@aws-sdk/client-dynamodb";
import { includeDerivedFields, serializeRecord } from "./recordFields";

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

async function batchGetDois(
  dois: string[],
  includeDerived: boolean,
): Promise<Record<string, any | null>> {
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
        if (recStr) { const rec = JSON.parse(recStr); out[doi] = serializeRecord(rec, includeDerived); }
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

    const results = await batchGetDois(dois, includeDerivedFields(event));

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
