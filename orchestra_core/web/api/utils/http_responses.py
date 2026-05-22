import hashlib

from fastapi import HTTPException
from starlette import status


class OutOfCreditError(RuntimeError):
    """Raised when a user runs out of credits."""


class AccountSuspendedError(RuntimeError):
    """Raised when a user's account is suspended due to billing issues."""


invalid_api_key = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Invalid API key. You can generate one at https://console.unify.ai/login",
)

account_frozen = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail="Your account has been suspended. Please reach out to hello@unify.ai if you have any questions.",
)

account_suspended = HTTPException(
    status_code=status.HTTP_402_PAYMENT_REQUIRED,
    detail=(
        "Your account has been suspended due to an unpaid invoice. "
        "Please update your payment method at https://console.unify.ai/ to resume service."
    ),
)

insufficient_credits_error = HTTPException(
    status_code=status.HTTP_402_PAYMENT_REQUIRED,
    detail=(
        "Whoops! It seems like this account doesn't have enough credits. "
        "To get a recharge, visit https://console.unify.ai/"
    ),
)

admin_not_authorized = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail="Admin access unauthorized, this incident will be reported.",
)

staging_restricted = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail="This environment is restricted to Unify AI members only.",
)


def not_found(item):
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"{item} not found.",
    )


# TODO: Test this
def server_error_with_digest(text: str):
    digest = hashlib.shake_256(text.encode()).digest(4).hex()
    return (
        HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal Server Error. Digest: {digest}",
        ),
        digest,
    )
