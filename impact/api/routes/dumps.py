from __future__ import annotations

import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, File, HTTPException, UploadFile, status

from impact.api.dependencies import load_bundle, load_manifest, validate_dump_path

router = APIRouter(tags=["dumps"], prefix="/dumps")

# Max upload size (100MB; prevents abuse/large dumps)
MAX_UPLOAD_FILE_SIZE = 100 * 1024 * 1024


@router.post("/upload", summary="Upload and validate GitHub dump ZIP", response_model=dict)
async def upload_dump(
    file: Annotated[UploadFile, File(description="ZIP file containing dump structure (manifest + canonical/*.jsonl)")],
) -> dict:
    """Upload ZIP, unpack to /tmp, verify canonical structure, return path or error.

    Minimal deps (stdlib zip/tempfile + reuse existing validators).
    """
    if not file.filename or not file.filename.endswith(".zip"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be a .zip archive",
        )

    # Size guard (before full read)
    if file.size is not None and file.size > MAX_UPLOAD_FILE_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File too large (max {MAX_UPLOAD_FILE_SIZE // (1024*1024)}MB)",
        )

    # Save uploaded ZIP to temp
    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
        tmp_zip.write(await file.read())
        zip_path = Path(tmp_zip.name)

    try:
        # Unpack to unique /tmp dir (mkdtemp avoids path conflicts)
        extract_dir = Path(tempfile.mkdtemp(dir="/tmp", prefix="devrank_dump_"))

        with zipfile.ZipFile(zip_path) as zf:
            # Reject ZIP entries with path traversal (e.g. ../../etc/passwd)
            for member in zf.namelist():
                target = (extract_dir / member).resolve()
                if not str(target).startswith(str(extract_dir.resolve())):
                    raise ValueError(f"ZIP entry escapes target directory: {member}")
            zf.extractall(extract_dir)

        # Verify structure: find manifest recursively (handles __MACOSX etc + nested zips)
        validated_path = validate_dump_path(str(extract_dir))
        manifests = list(validated_path.rglob("dump_manifest.json"))
        if not manifests:
            raise FileNotFoundError("No dump_manifest.json found in ZIP")
        # Use first-found manifest's parent as dump root (canonical structure from there)
        validated_path = manifests[0].parent
        manifest = load_manifest(str(validated_path))  # raises on missing/invalid
        _ = load_bundle(str(validated_path))  # full canonical check

        return {
            "status": "success",
            "message": "Dump uploaded and validated",
            "path": str(validated_path),
            "user": manifest.get("user"),
            "from": manifest.get("from"),
            "to": manifest.get("to"),
        }
    except Exception as e:
        # Cleanup on error
        if extract_dir.exists():
            shutil.rmtree(extract_dir, ignore_errors=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Upload failed: {str(e)}",
        ) from e
    finally:
        zip_path.unlink(missing_ok=True)
