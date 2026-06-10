# type: ignore
"""Update versioning triggers: group operations within the same DB transaction

Previously the trigger procedure always created a new ``transaction`` row for
every statement-level trigger invocation.  This produced one transaction record
per DML statement even when many statements were part of the same database
transaction, causing the ``transaction`` table to grow very quickly.

The new procedure uses a *transaction-local* PostgreSQL session variable
(``ixmp4.current_tx_id``, set with ``is_local = true``) so that all trigger
invocations that share the same database transaction reuse a single
``transaction`` row.  The variable is automatically reset when the database
transaction commits or rolls back.

This migration recreates all triggers so the changes to the procedure are applied.

Revision ID: 566ff46148d0
Revises: 87987b875e9b
Create Date: 2026-05-20 23:01:38.446861
"""

from alembic import op

# Revision identifiers, used by Alembic.
revision = "566ff46148d0"
down_revision = "87987b875e9b"
branch_labels = None
depends_on = None


def upgrade():
    op.sync_version_triggers(
        "iamc_datapoint_universal", "iamc_datapoint_universal_version"
    )
    op.sync_version_triggers("iamc_measurand", "iamc_measurand_version")
    op.sync_version_triggers("iamc_timeseries", "iamc_timeseries_version")
    op.sync_version_triggers("iamc_variable", "iamc_variable_version")
    op.sync_version_triggers("model", "model_version")
    op.sync_version_triggers("opt_equ", "opt_equ_version")
    op.sync_version_triggers(
        "opt_equ_idx_association", "opt_equ_idx_association_version"
    )
    op.sync_version_triggers("opt_idx", "opt_idx_version")
    op.sync_version_triggers("opt_idx_data", "opt_idx_data_version")
    op.sync_version_triggers("opt_par", "opt_par_version")
    op.sync_version_triggers(
        "opt_par_idx_association", "opt_par_idx_association_version"
    )
    op.sync_version_triggers("opt_sca", "opt_sca_version")
    op.sync_version_triggers("opt_tab", "opt_tab_version")
    op.sync_version_triggers(
        "opt_tab_idx_association", "opt_tab_idx_association_version"
    )
    op.sync_version_triggers("opt_var", "opt_var_version")
    op.sync_version_triggers(
        "opt_var_idx_association", "opt_var_idx_association_version"
    )
    op.sync_version_triggers("region", "region_version")
    op.sync_version_triggers("runmetaentry", "runmetaentry_version")
    op.sync_version_triggers("scenario", "scenario_version")
    op.sync_version_triggers("run", "run_version")
    op.sync_version_triggers("unit", "unit_version")


def downgrade():
    op.sync_version_triggers("unit", "unit_version")
    op.sync_version_triggers("run", "run_version")
    op.sync_version_triggers("scenario", "scenario_version")
    op.sync_version_triggers("runmetaentry", "runmetaentry_version")
    op.sync_version_triggers("region", "region_version")
    op.sync_version_triggers(
        "opt_var_idx_association", "opt_var_idx_association_version"
    )
    op.sync_version_triggers("opt_var", "opt_var_version")
    op.sync_version_triggers(
        "opt_tab_idx_association", "opt_tab_idx_association_version"
    )
    op.sync_version_triggers("opt_tab", "opt_tab_version")
    op.sync_version_triggers("opt_sca", "opt_sca_version")
    op.sync_version_triggers(
        "opt_par_idx_association", "opt_par_idx_association_version"
    )
    op.sync_version_triggers("opt_par", "opt_par_version")
    op.sync_version_triggers("opt_idx_data", "opt_idx_data_version")
    op.sync_version_triggers("opt_idx", "opt_idx_version")
    op.sync_version_triggers(
        "opt_equ_idx_association", "opt_equ_idx_association_version"
    )
    op.sync_version_triggers("opt_equ", "opt_equ_version")
    op.sync_version_triggers("model", "model_version")
    op.sync_version_triggers("iamc_variable", "iamc_variable_version")
    op.sync_version_triggers("iamc_timeseries", "iamc_timeseries_version")
    op.sync_version_triggers("iamc_measurand", "iamc_measurand_version")
    op.sync_version_triggers(
        "iamc_datapoint_universal", "iamc_datapoint_universal_version"
    )
