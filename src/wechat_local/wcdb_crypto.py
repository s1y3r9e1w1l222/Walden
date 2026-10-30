"""Minimal audited WCDB/SQLCipher4 derivation and page-1 verification.

The HMAC verification routine is adapted from CloudDreamAI / TANGandXUE's
wcdb-key-tool under the MIT License. See THIRD_PARTY_NOTICES.md.
"""

from __future__ import annotations

import hashlib
import hmac
import struct


PAGE_SIZE = 4096
KEY_SIZE = 32
SALT_SIZE = 16
HMAC_SIZE = 64
RESERVE_SIZE = 80
KDF_ITERATIONS = 256_000
MAC_KDF_ITERATIONS = 2


def derive_database_key(passphrase: bytes, salt: bytes) -> bytes:
    if len(passphrase) != KEY_SIZE:
        raise ValueError("Passphrase material must be exactly 32 bytes")
    if len(salt) != SALT_SIZE:
        raise ValueError("Database salt must be exactly 16 bytes")
    return hashlib.pbkdf2_hmac(
        "sha512", passphrase, salt, KDF_ITERATIONS, dklen=KEY_SIZE
    )


def verify_encryption_key(encryption_key: bytes, page_one: bytes) -> bool:
    if len(encryption_key) != KEY_SIZE or len(page_one) != PAGE_SIZE:
        return False
    salt = page_one[:SALT_SIZE]
    mac_salt = bytes(value ^ 0x3A for value in salt)
    mac_key = hashlib.pbkdf2_hmac(
        "sha512",
        encryption_key,
        mac_salt,
        MAC_KDF_ITERATIONS,
        dklen=KEY_SIZE,
    )
    hmac_data = page_one[SALT_SIZE : PAGE_SIZE - RESERVE_SIZE + 16]
    stored_hmac = page_one[PAGE_SIZE - HMAC_SIZE : PAGE_SIZE]
    digest = hmac.new(mac_key, hmac_data, hashlib.sha512)
    digest.update(struct.pack("<I", 1))
    return hmac.compare_digest(digest.digest(), stored_hmac)

