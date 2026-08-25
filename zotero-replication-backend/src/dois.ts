import { DynamoDBClient, GetItemCommand, BatchGetItemCommand } from "@aws-sdk/client-dynamodb";

const ddb = new DynamoDBClient({});
const SEARCH_TABLE = process.env.SEARCH_TABLE!;
const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour

const cors = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

/* ── In-memory cache ──────────────────────────────────────────────── */

let cachedDois: string[] | null = null;
let cacheExp = 0;

async function loadAllDois(): Promise<string[]> {
  if (cachedDois && Date.now() < cacheExp) return cachedDois;

  const metaResp = await ddb.send(
    new GetItemCommand({
      TableName: SEARCH_TABLE,
      Key: { chunk_id: { S: "meta" } },
    })
  );

  const totalChunks = parseInt(metaResp.Item?.data?.S ?? "0", 10);
  if (totalChunks === 0) {
    cachedDois = [];
    cacheExp = Date.now() + CACHE_TTL_MS;
    return cachedDois;
  }

  const allDois: string[] = [];
  const chunkIds = Array.from({ length: totalChunks }, (_, i) => String(i));

  for (let i = 0; i < chunkIds.length; i += 100) {
    const batch = chunkIds.slice(i, i + 100);
    let requestItems: any = {
      [SEARCH_TABLE]: {
        Keys: batch.map((id) => ({ chunk_id: { S: id } })),
      },
    };

    for (let attempt = 0; attempt < 5 && requestItems; attempt++) {
      const resp = await ddb.send(new BatchGetItemCommand({ RequestItems: requestItems }));

      for (const item of resp.Responses?.[SEARCH_TABLE] ?? []) {
        const dataStr = item.data?.S;
        if (!dataStr) continue;
        try {
          const entries: { _doi: string }[] = JSON.parse(dataStr);
          for (const e of entries) if (e._doi) allDois.push(e._doi);
        } catch {}
      }

      requestItems =
        resp.UnprocessedKeys && Object.keys(resp.UnprocessedKeys).length
          ? resp.UnprocessedKeys
          : null;
    }
  }

  cachedDois = allDois;
  cacheExp = Date.now() + CACHE_TTL_MS;
  return cachedDois;
}

/* ── Lambda handler ───────────────────────────────────────────────── */

export const handler = async (event: any) => {
  try {
    const method = event?.requestContext?.http?.method || event?.httpMethod || "GET";
    if (method === "OPTIONS") return { statusCode: 200, headers: cors, body: "" };

    const dois = await loadAllDois();

    return {
      statusCode: 200,
      headers: { ...cors, "Content-Type": "application/json", "Cache-Control": "public, max-age=3600" },
      body: JSON.stringify({ total: dois.length, dois }),
    };
  } catch (err: any) {
    console.error("dois error:", err);
    return {
      statusCode: 500,
      headers: { ...cors, "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Internal Server Error", details: err?.message || String(err) }),
    };
  }
};
