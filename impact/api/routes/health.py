from __future__ import annotations

from typing import Any

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health", summary="Health check")
async def health_check() -> dict[str, Any]:
    return {"status": "healthy", "service": "devrank-impact"}
