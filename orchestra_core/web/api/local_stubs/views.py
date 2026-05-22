"""Single-tenant no-ops for the unify-SDK account/billing endpoints."""

from typing import Any, Dict, List

from fastapi import APIRouter, status
from fastapi.responses import Response

router = APIRouter()

_LOCAL_BALANCE = 1_000_000_000.0


@router.get("/user/basic-info")
def user_basic_info() -> Dict[str, Any]:
    return {
        "user_id": "1",
        "email": "local@orchestra-core",
        "first_name": "Local",
        "last_name": "User",
        "organization_id": None,
    }


@router.post(
    "/user/spend",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def user_spend(_payload: Dict[str, Any] = None):
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/user/spending-limit-reached",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def spending_limit_reached(_payload: Dict[str, Any] = None):
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/credits/deduct")
def credits_deduct(_payload: Dict[str, Any] = None) -> Dict[str, Any]:
    return {
        "credits_remaining": _LOCAL_BALANCE,
        "credits_deducted": 0.0,
    }


@router.get("/assistant")
def list_assistants() -> List[Dict[str, Any]]:
    return []
