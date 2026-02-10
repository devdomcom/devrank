from __future__ import annotations

from fastapi import APIRouter

from impact.api.schemas import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", summary="Health check", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    return HealthResponse(status="healthy", service="devrank-impact")
