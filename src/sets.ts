import { randomBytes } from "node:crypto";
import {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  ConditionalCheckFailedException,
  type AttributeValue,
} from "@aws-sdk/client-dynamodb";

const ddb = new DynamoDBClient({});
const SETS_TABLE = process.env.SETS_TABLE!;

const ID_ATTEMPTS = 5;
const TTL_SECONDS = 30 * 24 * 60 * 60;

/** AES-GCM's 96-bit IV, as unpadded base64url. */
const IV_BYTES = 12;
/** A GCM ciphertext is at least its 16-byte tag plus one byte of plaintext. */
const MIN_DATA_BYTES = 17;
/** 5000 DOIs of JSON come to ~150 KiB; the cap keeps a row well inside DynamoDB's 400 KB. */
const MAX_DATA_BYTES = 256 * 1024;

/** 8 hex characters. The key that decrypts a set never reaches the API. */
const ID_RE = /^[0-9a-f]{8}$/;

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

/** Strict unpadded base64url — Node's decoder skips stray characters, so confirm the round trip. */
function decodeB64url(encoded: unknown): Buffer | null {
  if (typeof encoded !== "string" || !/^[A-Za-z0-9_-]+$/.test(encoded)) return null;
  const bytes = Buffer.from(encoded, "base64url");
  return bytes.toString("base64url") === encoded ? bytes : null;
}

/**
 * A missing row is indistinguishable from one DynamoDB has already swept, so both get the
 * same answer. The client's move is to re-POST in either case.
 */
const expired = () =>
  json(404, {
    error: "This link has expired. Please generate it again.",
    code: "set_expired",
  });

/** Write `item` under a fresh id, retrying on the rare collision. */
async function putWithFreshId(item: Record<string, AttributeValue>) {
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
      return id;
    } catch (err) {
      if (!(err instanceof ConditionalCheckFailedException)) throw err;
    }
  }
  throw new Error(`Could not allocate a free set id in ${ID_ATTEMPTS} attempts`);
}

/** The row for `id`, or null if it is missing or past its TTL. */
async function loadLive(id: string) {
  const resp = await ddb.send(
    new GetItemCommand({ TableName: SETS_TABLE, Key: { id: { S: id } } }),
  );
  if (!resp.Item) return null;

  // DynamoDB sweeps expired items lazily (up to ~48h late), so enforce the TTL on read.
  const nowSeconds = Math.floor(Date.now() / 1000);
  const expiresAt = Number(resp.Item.ttl?.N ?? 0) || nowSeconds + TTL_SECONDS;
  if (expiresAt <= nowSeconds) return null;

  return { item: resp.Item, nowSeconds, expiresAt };
}

/** A set never changes, so it is cacheable right up to the moment it expires. */
const cacheUntil = (expiresAt: number, nowSeconds: number) => ({
  "Cache-Control": `private, max-age=${expiresAt - nowSeconds}, immutable`,
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

    // The browser encrypts the list itself and keeps the key, so all that arrives here is
    // ciphertext — the API stores bytes it has no way to read.
    const iv = decodeB64url(body?.iv);
    const data = decodeB64url(body?.data);
    if (!iv || !data) {
      return json(400, { error: "Expected { data, iv } as unpadded base64url" });
    }
    if (iv.length !== IV_BYTES) {
      return json(400, { error: `iv must be ${IV_BYTES} bytes` });
    }
    if (data.length < MIN_DATA_BYTES || data.length > MAX_DATA_BYTES) {
      return json(400, { error: `data must be ${MIN_DATA_BYTES}–${MAX_DATA_BYTES} bytes` });
    }

    const expiresAt = Math.floor(Date.now() / 1000) + TTL_SECONDS;
    const id = await putWithFreshId({
      data: { B: data },
      iv: { B: iv },
      ttl: { N: String(expiresAt) },
    });

    return json(200, { id, expires: isoSeconds(new Date(expiresAt * 1000)) });
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
    if (!ID_RE.test(token)) return expired();

    const live = await loadLive(token);
    if (!live) return expired();

    const data = live.item.data?.B;
    const iv = live.item.iv?.B;
    if (!data || !iv) return expired();

    // Ciphertext only: the key never left the browser that made the set, and the
    // browser opening the link decrypts it from the URL fragment.
    return json(
      200,
      {
        id: token,
        data: Buffer.from(data).toString("base64url"),
        iv: Buffer.from(iv).toString("base64url"),
        expires: isoSeconds(new Date(live.expiresAt * 1000)),
      },
      cacheUntil(live.expiresAt, live.nowSeconds),
    );
  } catch (err: any) {
    console.error("sets get error:", err);
    return json(500, { error: "Internal Server Error", details: err?.message || String(err) });
  }
};
