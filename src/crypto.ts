import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";

const ALGORITHM = "aes-256-gcm";
const KEY_BYTES = 32;
const IV_BYTES = 12;

/** A 32-byte key encoded as unpadded base64url is always 43 characters. */
export const KEY_CHARS = 43;

export type Sealed = { ciphertext: string; iv: string; tag: string };

export function newKey() {
  return randomBytes(KEY_BYTES);
}

export function encodeKey(key: Buffer) {
  return key.toString("base64url");
}

export function decodeKey(encoded: string): Buffer | null {
  if (encoded.length !== KEY_CHARS) return null;
  const key = Buffer.from(encoded, "base64url");
  // base64url decoding is lenient about stray characters, so confirm the round trip.
  if (key.length !== KEY_BYTES || key.toString("base64url") !== encoded) return null;
  return key;
}

export function seal(plaintext: string, key: Buffer): Sealed {
  const iv = randomBytes(IV_BYTES);
  const cipher = createCipheriv(ALGORITHM, key, iv);
  const ciphertext = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
  return {
    ciphertext: ciphertext.toString("base64"),
    iv: iv.toString("base64"),
    tag: cipher.getAuthTag().toString("base64"),
  };
}

/** Returns null for a wrong key or tampered ciphertext — GCM makes the two indistinguishable. */
export function open(sealed: Sealed, key: Buffer): string | null {
  try {
    const decipher = createDecipheriv(ALGORITHM, key, Buffer.from(sealed.iv, "base64"));
    decipher.setAuthTag(Buffer.from(sealed.tag, "base64"));
    const plaintext = Buffer.concat([
      decipher.update(Buffer.from(sealed.ciphertext, "base64")),
      decipher.final(),
    ]);
    return plaintext.toString("utf8");
  } catch {
    return null;
  }
}
