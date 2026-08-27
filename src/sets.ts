import { randomBytes } from "node:crypto";
import {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  ConditionalCheckFailedException,
} from "@aws-sdk/client-dynamodb";
import { normDoi } from "./doi";

const ddb = new DynamoDBClient({});
const SETS_TABLE = process.env.SETS_TABLE!;

const MAX_DOIS = 5000;
const ID_ATTEMPTS = 5;
const TTL_SECONDS = 30 * 24 * 60 * 60;

const cors = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const json = (statusCode: number, body: unknown, extraHeaders: Record<string, string> = {}) => ({
  statusCode,
  headers: { ...cors, "Content-Type": "application/json", ...extraHeaders },
  body: JSON.stringify(body),
});

/** ISO 8601, seconds precision — `2026-08-27T10:14:02Z`. */
function isoSeconds(d: Date) {
  return d.toISOString().replace(/\.\d{3}Z$/, "Z");
}

function newId() {
  return randomBytes(4).toString("hex");
}

/**
 * A missing row is indistinguishable from one DynamoDB has already swept, so an unknown
 * id gets the same answer as an expired one: the client's move is to re-POST either way.
 */
const expired = () =>
  json(404, {
    error: "This link has expired. Please generate it again.",
    code: "set_expired",
  });

/* ── POST /v1/sets ────────────────────────────────────────────────── */

export const create = async (event: any) => {
  try {
    const method = event?.requestContext?.http?.method || event?.httpMethod || "POST";
    if (method === "OPTIONS") return { statusCode: 200, headers: cors, body: "" };

    let body: any;
    try {
      body = JSON.parse(event?.body ?? "");
    } catch {
      return json(400, { error: "Body must be JSON" });
    }

    if (!Array.isArray(body?.dois)) {
      return json(400, { error: "Expected { dois: string[] }" });
    }

    const dois = Array.from(
      new Set(body.dois.map((d: unknown) => normDoi(String(d ?? ""))).filter(Boolean)),
    ) as string[];

    if (!dois.length) return json(400, { error: "No DOIs provided" });
    if (dois.length > MAX_DOIS) {
      return json(400, { error: `Too many DOIs (${dois.length}); maximum is ${MAX_DOIS}` });
    }

    const now = new Date();
    const created = isoSeconds(now);
    const expiresAt = Math.floor(now.getTime() / 1000) + TTL_SECONDS;
    const item = {
      dois: { S: JSON.stringify(dois) },
      count: { N: String(dois.length) },
      created: { S: created },
      ttl: { N: String(expiresAt) },
    };

    for (let attempt = 0; attempt < ID_ATTEMPTS; attempt++) {
      const id = newId();
      try {
        await ddb.send(
          new PutItemCommand({
            TableName: SETS_TABLE,
            Item: { id: { S: id }, ...item },
            ConditionExpression: "attribute_not_exists(id)",
          }),
        );
        return json(200, {
          id,
          count: dois.length,
          created,
          expires: isoSeconds(new Date(expiresAt * 1000)),
        });
      } catch (err) {
        if (!(err instanceof ConditionalCheckFailedException)) throw err;
      }
    }

    throw new Error(`Could not allocate a free set id in ${ID_ATTEMPTS} attempts`);
  } catch (err: any) {
    console.error("sets create error:", err);
    return json(500, { error: "Internal Server Error", details: err?.message || String(err) });
  }
};

/* ── GET /v1/sets/:id ─────────────────────────────────────────────── */

export const get = async (event: any) => {
  try {
    const method = event?.requestContext?.http?.method || event?.httpMethod || "GET";
    if (method === "OPTIONS") return { statusCode: 200, headers: cors, body: "" };

    const id = String(event?.pathParameters?.id ?? "").toLowerCase();
    if (!/^[0-9a-f]{8}$/.test(id)) return expired();

    const resp = await ddb.send(
      new GetItemCommand({ TableName: SETS_TABLE, Key: { id: { S: id } } }),
    );
    if (!resp.Item) return expired();

    // DynamoDB sweeps expired items lazily (up to ~48h late), so enforce the TTL on read.
    const nowSeconds = Math.floor(Date.now() / 1000);
    const expiresAt = Number(resp.Item.ttl?.N ?? 0) || nowSeconds + TTL_SECONDS;
    if (expiresAt <= nowSeconds) return expired();

    const dois: string[] = JSON.parse(resp.Item.dois?.S ?? "[]");

    // A set never changes, so it is cacheable right up to the moment it expires.
    return json(
      200,
      {
        id,
        dois,
        count: Number(resp.Item.count?.N ?? dois.length),
        created: resp.Item.created?.S ?? null,
        expires: isoSeconds(new Date(expiresAt * 1000)),
      },
      { "Cache-Control": `public, max-age=${expiresAt - nowSeconds}, immutable` },
    );
  } catch (err: any) {
    console.error("sets get error:", err);
    return json(500, { error: "Internal Server Error", details: err?.message || String(err) });
  }
};
