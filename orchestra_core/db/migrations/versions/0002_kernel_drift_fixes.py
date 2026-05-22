"""Reconcile kernel-table constraint drift between historical platform DBs and the model.

Two classes of drift exist between the schema produced by orchestra-core's
`0001_core_initial` (which mirrors the model) and the schema present on
production databases that originally got their kernel tables from
orchestra-platform's pre-split migrations:

1. **Renamed primary key on `context_counter`.** Production has the
   migration-set name `pk_context_counter`; the model (and therefore
   `meta.create_all`) produces the postgres default `context_counter_pkey`.
   This migration renames the production constraint to match.

2. **Missing CHECK constraints on `description` columns.** The kernel
   model declares `ck_<table>_description_len` CHECKs limiting
   `char_length(description) <= 256` on `context`, `field_type`, and
   `project`. The pre-split platform migrations that originally created
   these tables did not include the CHECKs, so production runs without
   them today. Fresh databases (which run `0001_core_initial`) get the
   CHECKs from the model. This migration backfills them on existing
   DBs.

Every operation is idempotent — guarded by a `pg_constraint` lookup —
so this migration is safe to run on:

- Fresh DBs that already have the new names + CHECKs from
  `0001_core_initial` (no-op).
- Production DBs that have the old PK name and no CHECKs (renames +
  adds).
- Partially-converged DBs (idempotent on the operations already
  applied).

Revision ID: 0002_kernel_drift_fixes
Revises: 0001_core_initial
Create Date: 2026-05-22 23:00:00.000000
"""

from __future__ import annotations

from alembic import op

revision = "0002_kernel_drift_fixes"
down_revision = "0001_core_initial"
branch_labels = None
depends_on = None


def _rename_constraint(table: str, old: str, new: str) -> None:
    """Rename a constraint only if the old name still exists.

    Wrapped in a DO block so the ALTER is skipped on databases that have
    already been converged or were created fresh under the model.
    """
    op.execute(f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = '{old}'
                  AND conrelid = 'public.{table}'::regclass
            ) THEN
                ALTER TABLE public.{table}
                    RENAME CONSTRAINT {old} TO {new};
            END IF;
        END$$;
        """)


def _add_description_len_check(table: str) -> None:
    """Add `char_length(description) <= 256` CHECK if not already present.

    The constraint name follows the kernel model's `ck_<table>_description_len`
    convention. Skipped on DBs that already have it (fresh-from-model).
    """
    name = f"ck_{table}_description_len"
    op.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = '{name}'
                  AND conrelid = 'public.{table}'::regclass
            ) THEN
                ALTER TABLE public.{table}
                    ADD CONSTRAINT {name}
                    CHECK (char_length(description) <= 256);
            END IF;
        END$$;
        """)


def upgrade() -> None:
    _rename_constraint("context_counter", "pk_context_counter", "context_counter_pkey")
    for table in ("context", "field_type", "project"):
        _add_description_len_check(table)


def downgrade() -> None:
    """Reverse the renames + drop the backfilled CHECKs.

    Idempotent in the same fashion as `upgrade()` — only acts if the
    converged-state name/constraint is actually present.
    """
    for table in ("context", "field_type", "project"):
        op.execute(f"""
            ALTER TABLE public.{table}
                DROP CONSTRAINT IF EXISTS ck_{table}_description_len;
            """)

    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'context_counter_pkey'
                  AND conrelid = 'public.context_counter'::regclass
            ) THEN
                ALTER TABLE public.context_counter
                    RENAME CONSTRAINT context_counter_pkey TO pk_context_counter;
            END IF;
        END$$;
        """)
