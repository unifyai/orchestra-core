"""No-op stubs for the platform-only endpoints unify SDK calls.

The unify SDK assumes a hosted multi-tenant Orchestra and calls a small
handful of account/billing endpoints opportunistically. To avoid forcing
unify to branch on which backend it's talking to, orchestra-core exposes
the same paths and returns sensible single-tenant defaults.
"""

from orchestra_core.web.api.local_stubs.views import router

__all__ = ["router"]
