from __future__ import annotations

import gzip
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import HTTPException
from sqlmodel import Session

from database.session import supabase_client
from services.tenant_backup_service import export_tenant_snapshot

BACKUP_DIR = Path("backups")
BACKUP_DIR.mkdir(parents=True, exist_ok=True)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _backup_filename(tenant_id: Optional[int] = None) -> str:
    prefix = f"tenant_{tenant_id}_" if tenant_id is not None else ""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{prefix}db_backup_{timestamp}.json.gz"


def create_backup_file(session: Session, tenant_id: Optional[int] = None) -> dict[str, Any]:
    if tenant_id is None:
        raise HTTPException(status_code=400, detail="tenant_id is required for backups")

    snapshot = export_tenant_snapshot(session, tenant_id)
    snapshot["timestamp"] = _now_utc_iso()
    filename = _backup_filename(tenant_id)
    path = BACKUP_DIR / filename

    with gzip.open(path, "wt", encoding="utf-8") as fh:
        json.dump(snapshot, fh, ensure_ascii=False)

    try:
        enforce_retention_policy(max_backups=10, tenant_id=tenant_id)
    except Exception as e:
        print(f"Warning: Failed to enforce retention policy: {e}")

    result = {
        "status": "success",
        "filename": filename,
        "path": str(path),
        "timestamp": snapshot["timestamp"],
        "size_bytes": path.stat().st_size,
    }

    bucket_name = os.getenv("SUPABASE_BACKUP_BUCKET")
    if bucket_name and supabase_client:
        try:
            with path.open("rb") as fh:
                remote_name = f"db/{filename}"
                supabase_client.storage.from_(bucket_name).upload(
                    path=remote_name,
                    file=fh,
                    file_options={"content-type": "application/gzip", "upsert": "false"},
                )
            result["supabase"] = {"uploaded": True, "bucket": bucket_name, "object": remote_name}
        except Exception as exc:
            result["supabase"] = {"uploaded": False, "error": str(exc)}

    return result


def list_local_backups(tenant_id: Optional[int] = None) -> list[dict[str, Any]]:
    pattern = f"tenant_{tenant_id}_db_backup_*.json.gz" if tenant_id is not None else "db_backup_*.json.gz"
    entries: list[dict[str, Any]] = []
    for p in sorted(BACKUP_DIR.glob(pattern), key=lambda item: item.stat().st_mtime, reverse=True):
        st = p.stat()
        entries.append(
            {
                "filename": p.name,
                "size_bytes": st.st_size,
                "modified_at": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
            }
        )
    return entries


def get_local_backup_path(filename: str, tenant_id: Optional[int] = None) -> Path:
    safe_name = os.path.basename(filename)
    if tenant_id is not None and not safe_name.startswith(f"tenant_{tenant_id}_"):
        raise HTTPException(status_code=404, detail="Backup file not found")
    path = BACKUP_DIR / safe_name
    if not path.exists() or path.suffix != ".gz":
        raise HTTPException(status_code=404, detail="Backup file not found")
    return path


def enforce_retention_policy(max_backups: int = 10, tenant_id: Optional[int] = None) -> None:
    backups = list_local_backups(tenant_id=tenant_id)
    if len(backups) > max_backups:
        for old_backup in backups[max_backups:]:
            path = BACKUP_DIR / old_backup["filename"]
            try:
                path.unlink(missing_ok=True)
            except Exception as e:
                print(f"Warning: failed to delete old backup {path.name}: {e}")
