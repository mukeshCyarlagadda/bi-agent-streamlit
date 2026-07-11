"""
Supabase Storage helpers for persisting uploaded SQLite files.

SQLite files live on disk for active queries. After upload they are mirrored
to Supabase Storage so they survive Render restarts. On reconnect, if the
local file is missing it is downloaded from Storage transparently.

Storage layout: bucket=user-files, key=<safe_uid>/<uuid>.db
"""
from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

BUCKET = "user-files"
_UPLOADS_ROOT = Path.home() / ".bi_agent_uploads"


def _client():
    from supabase import create_client
    from core.config import settings
    return create_client(settings.supabase_url, settings.supabase_secret_key)


def ensure_bucket() -> None:
    """Create the storage bucket at startup if it does not exist."""
    from core.config import settings
    if not settings.supabase_url or not settings.supabase_secret_key:
        return
    try:
        client = _client()
        existing = {b.name for b in client.storage.list_buckets()}
        if BUCKET not in existing:
            client.storage.create_bucket(BUCKET, options={"public": False})
            logger.info("Created Supabase Storage bucket: %s", BUCKET)
    except Exception as exc:
        logger.warning("ensure_bucket failed (non-fatal): %s", exc)


def local_to_key(local_path: str) -> str:
    """
    Convert an absolute local path to a storage key.
    e.g. /root/.bi_agent_uploads/abc/def.db  →  abc/def.db
    """
    try:
        return str(Path(local_path).relative_to(_UPLOADS_ROOT))
    except ValueError:
        return Path(local_path).name


def key_to_local(storage_key: str) -> Path:
    """Reconstruct the expected local path from a storage key."""
    return _UPLOADS_ROOT / storage_key


def upload_sqlite(local_path: str) -> str:
    """
    Upload the SQLite file at *local_path* to Supabase Storage.
    Returns the storage key (relative path within the bucket).
    """
    from core.config import settings
    storage_key = local_to_key(local_path)

    if not settings.supabase_url or not settings.supabase_secret_key:
        logger.warning("Supabase credentials not set — skipping storage backup")
        return storage_key

    try:
        data = Path(local_path).read_bytes()
        _client().storage.from_(BUCKET).upload(
            storage_key,
            data,
            {"content-type": "application/octet-stream", "upsert": "true"},
        )
        logger.info("Backed up %s → storage://%s/%s", Path(local_path).name, BUCKET, storage_key)
    except Exception as exc:
        logger.warning("Storage upload failed (non-fatal): %s", exc)

    return storage_key


def download_sqlite(storage_key: str) -> Path:
    """
    Download *storage_key* from Supabase Storage to the expected local path.
    Returns the local Path. Raises RuntimeError if download fails.
    """
    local_path = key_to_local(storage_key)
    if local_path.exists():
        return local_path

    try:
        data = _client().storage.from_(BUCKET).download(storage_key)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(data)
        logger.info("Restored %s from Supabase Storage", storage_key)
        return local_path
    except Exception as exc:
        raise RuntimeError(f"Could not restore file from storage: {exc}") from exc
