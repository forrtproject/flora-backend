import { randomBytes } from "node:crypto";
import {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  ConditionalCheckFailedException,
} from "@aws-sdk/client-dynamodb";
import { normDoi } from "./doi";
import { KEY_CHARS, decodeKey, encodeKey, newKey, open, seal } from "./crypto";

const ddb = new DynamoDBClient({});
const SETS_TABLE = process.env.SETS_TABLE!;

const MAX_DOIS = 5000;
const ID_ATTEMPTS = 5;
const TTL_SECONDS = 30 * 24 * 60 * 60;

/** `<8 hex id>.<base64url key>` — the id addresses the row, the key never reaches the table. */
const TOKEN_RE = new RegExp(`^([0-9a-f]{8})\\.([A-Za-z0-9_-]{${KEY_CHARS}})$`);

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
 * A missing row is indistinguishable from one DynamoDB has already swept, and a wrong key
 * is deliberately given the same answer so the endpoint is not an oracle for guessing keys.
 * The client's move is to re-POST in every one of those cases.
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

    // Everything the caller submitted goes inside the sealed payload; the row keeps only
    // what DynamoDB itself has to read, which is the key and the TTL attribute.
    const key = newKey();
    const sealed = seal(JSON.stringify({ dois, count: dois.length, created }), key);
    const item = {
      data: { S: sealed.ciphertext },
      iv: { S: sealed.iv },
      tag: { S: sealed.tag },
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
          id: `${id}.${encodeKey(key)}`,
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

    const token = String(event?.pathParameters?.id ?? "");
    const parts = TOKEN_RE.exec(token);
    if (!parts) return expired();

    const id = parts[1]!;
    const encodedKey = parts[2]!;
    const key = decodeKey(encodedKey);
    if (!key) return expired();

    const resp = await ddb.send(
      new GetItemCommand({ TableName: SETS_TABLE, Key: { id: { S: id } } }),
    );
    if (!resp.Item) return expired();

    // DynamoDB sweeps expired items lazily (up to ~48h late), so enforce the TTL on read.
    const nowSeconds = Math.floor(Date.now() / 1000);
    const expiresAt = Number(resp.Item.ttl?.N ?? 0) || nowSeconds + TTL_SECONDS;
    if (expiresAt <= nowSeconds) return expired();

    const plaintext = open(
      {
        ciphertext: resp.Item.data?.S ?? "",
        iv: resp.Item.iv?.S ?? "",
        tag: resp.Item.tag?.S ?? "",
      },
      key,
    );
    if (plaintext === null) return expired();

    const payload = JSON.parse(plaintext) as { dois: string[]; count: number; created: string };

    // A set never changes, so it is cacheable right up to the moment it expires — but the
    // URL carries the key, so only the one browser that holds the link may keep a copy.
    return json(
      200,
      {
        id: token,
        dois: payload.dois,
        count: payload.count ?? payload.dois.length,
        created: payload.created ?? null,
        expires: isoSeconds(new Date(expiresAt * 1000)),
      },
      { "Cache-Control": `private, max-age=${expiresAt - nowSeconds}, immutable` },
    );
  } catch (err: any) {
    console.error("sets get error:", err);
    return json(500, { error: "Internal Server Error", details: err?.message || String(err) });
  }
};
