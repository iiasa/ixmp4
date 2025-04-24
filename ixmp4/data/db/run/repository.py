from typing import TYPE_CHECKING, cast

import pandas as pd
from sqlalchemy.exc import NoResultFound

# TODO Adapt import when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

from ixmp4 import db
from ixmp4.core.exceptions import Forbidden, IxmpError, NoDefaultRunVersion
from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard
from ixmp4.db import utils

from .. import base
from ..model import Model, ModelRepository
from ..scenario import Scenario, ScenarioRepository
from .model import Run


class CreateKwargs(TypedDict, total=False):
    scenario_name: str


class RunRepository(
    base.Creator[Run],
    base.Retriever[Run],
    base.Enumerator[Run],
    base.VersionManager[Run],
    abstract.RunRepository,
):
    model_class = Run

    models: ModelRepository
    scenarios: ScenarioRepository

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        self.models = ModelRepository(*args)
        self.scenarios = ScenarioRepository(*args)

        from .filter import RunFilter

        self.filter_class = RunFilter
        super().__init__(*args)

    def join_auth(self, exc: db.sql.Select[tuple[Run]]) -> db.sql.Select[tuple[Run]]:
        if not utils.is_joined(exc, Model):
            exc = exc.join(Model, Run.model)
        return exc

    def add(self, model_name: str, scenario_name: str) -> Run:
        # Get or create model
        try:
            exc_model = self.models.select(name=model_name)
            model = self.session.execute(exc_model).scalar_one()
        except NoResultFound:
            model = Model(name=model_name)
            self.session.add(model)

        # Get or create scenario
        try:
            exc_scenario = self.scenarios.select(name=scenario_name)
            scenario = self.session.execute(exc_scenario).scalar_one()
        except NoResultFound:
            scenario = Scenario(name=scenario_name)
            self.session.add(scenario)

        (version,) = (
            # Aggregate MAX over run.version where the run's
            # model and scenario match.
            self.session.query(db.func.max(Run.version))
            .filter(Run.model.has(name=model_name))
            .filter(Run.scenario.has(name=scenario_name))
            .one()
        )
        if version is None:
            version = 0
        version += 1

        run = Run(model=model, scenario=scenario, version=version)
        self.session.add(run)
        return run

    @guard("edit")
    def create(
        self, model_name: str, *args: str, **kwargs: Unpack[CreateKwargs]
    ) -> Run:
        if self.backend.auth_context is not None:
            if not self.backend.auth_context.check_access("edit", model_name):
                raise Forbidden(f"Access to model '{model_name}' denied.")
        run = super().create(model_name, *args, **kwargs)
        return run

    @guard("view")
    def get(self, model_name: str, scenario_name: str, version: int) -> Run:
        exc = self.select(
            model={"name": model_name},
            scenario={"name": scenario_name},
            version=version,
            default_only=False,
        )

        try:
            # TODO clean up unnecessary cast such as this
            run: Run = self.session.execute(exc).scalar_one()
            return run
        except NoResultFound:
            raise Run.NotFound(
                model=model_name,
                scenario=scenario_name,
                version=version,
            )

    @guard("view")
    def get_by_id(self, id: int, _access_type: str = "view") -> Run:
        exc = self.select(_access_type=_access_type, _skip_filter=True).where(
            self.model_class.id == id
        )

        try:
            obj: Run = self.session.execute(exc).scalar_one()
        except NoResultFound:
            raise Run.NotFound(id=id)

        return obj

    @guard("view")
    def get_default_version(self, model_name: str, scenario_name: str) -> Run:
        exc = self.select(
            model={"name": model_name},
            scenario={"name": scenario_name},
        )

        try:
            run: Run = self.session.execute(exc).scalar_one()
            return run
        except NoResultFound:
            raise NoDefaultRunVersion

    @guard("view")
    def tabulate(self, **kwargs: Unpack[abstract.run.EnumerateKwargs]) -> pd.DataFrame:
        return super().tabulate(**kwargs)

    @guard("view")
    def list(self, **kwargs: Unpack[abstract.run.EnumerateKwargs]) -> list[Run]:
        return super().list(**kwargs)

    @guard("edit")
    def set_as_default_version(self, id: int) -> None:
        run = self.get_by_id(id, _access_type="edit")

        exc = (
            db.update(Run)
            .where(
                Run.model__id == run.model__id,
                Run.scenario__id == run.scenario__id,
                Run.is_default,
            )
            .values(is_default=False)
        )

        with self.backend.event_handler.pause():
            # we dont want to trigger the
            # updated_at fields for this operation.
            self.session.execute(exc)
            self.session.commit()

        run.is_default = True
        self.session.commit()

    @guard("edit")
    def unset_as_default_version(self, id: int) -> None:
        run = self.get_by_id(id, _access_type="edit")

        if not run.is_default:
            raise IxmpError(f"Run with id={id} is not set as the default version.")

        run.is_default = False
        self.session.commit()

    def _get_or_create_ts(self, run__id: int, df: pd.DataFrame) -> pd.DataFrame:
        df["run__id"] = run__id
        id_cols = ["region", "variable", "unit", "run__id"]
        # create set of unqiue timeseries (if missing)
        ts_df = df[id_cols].drop_duplicates()
        self.backend.iamc.timeseries.bulk_upsert(ts_df, create_related=True)

        # retrieve them again to get database ids
        ts_df = self.backend.iamc.timeseries.tabulate(
            join_parameters=True,
            run={"id": run__id, "default_only": False},
        )
        ts_df = ts_df.rename(columns={"id": "time_series__id"})

        # merge on the identity columns
        return pd.merge(
            df,
            ts_df,
            how="left",
            on=id_cols,
            suffixes=(None, "_y"),
        )

    def revert_iamc_data(self, run: Run, transaction__id: int) -> None:
        current_dps = self.backend.iamc.datapoints.tabulate(
            run={"id": run.id, "default_only": False}
        )

        if not current_dps.empty:
            self.backend.iamc.datapoints.bulk_delete(current_dps)  # type: ignore[arg-type]

        version_dps = self.backend.iamc.datapoints.tabulate_versions(
            transaction__id=transaction__id
        )
        if version_dps.empty:
            return

        version_dps = version_dps.drop(
            columns=[
                "id",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
                "time_series__id",
            ]
        )
        version_dps = self._get_or_create_ts(run.id, version_dps).dropna(
            how="all", axis="columns"
        )
        self.backend.iamc.datapoints.bulk_upsert(version_dps)  # type: ignore[arg-type]

    def revert_meta(self, run: Run, transaction__id: int) -> None:
        current_meta = self.backend.meta.tabulate(
            run={"id": run.id, "default_only": False}
        ).drop(columns=["id", "value", "dtype"])

        if not current_meta.empty:
            self.backend.meta.bulk_delete(current_meta)  # type: ignore[arg-type]

        version_meta = self.backend.meta.tabulate_versions(
            transaction__id=transaction__id
        )
        if version_meta.empty:
            return

        # remove the transaction_id column
        version_meta = version_meta.drop(
            columns=["transaction_id", "end_transaction_id", "operation_type"]
        )
        version_meta = self.backend.meta.merge_value_columns(version_meta)
        version_meta = version_meta.drop(columns=["dtype", "id"])
        self.backend.meta.bulk_upsert(version_meta)  # type: ignore[arg-type]

    @guard("edit")
    def revert(self, id: int, transaction__id: int) -> None:
        run = self.get_by_id(id, _access_type="edit")

        if self.get_latest_transaction().id == transaction__id:
            # we are already at the right transaction
            return

        self.revert_iamc_data(run, transaction__id)
        self.revert_meta(run, transaction__id)

    @guard("view")
    def tabulate_transactions(
        self, /, **kwargs: Unpack[base.TabulateTransactionsKwargs]
    ) -> pd.DataFrame:
        return super().tabulate_transactions(**kwargs)

    @guard("view")
    def tabulate_versions(
        self, /, **kwargs: Unpack[base.TabulateVersionsKwargs]
    ) -> pd.DataFrame:
        return super().tabulate_versions(**kwargs)

    def get_latest_transaction(self) -> base.TransactionProtocol:
        exc = self.select_transactions()
        exc = exc.order_by(self.transaction_class.issued_at.desc())
        transaction = cast(
            base.TransactionProtocol, self.session.execute(exc).scalars().first()
        )
        return transaction

    @guard("edit")
    def lock(self, id: int) -> Run:
        try:
            run = self.get_by_id(id, _access_type="edit")
        except Run.NotFound:
            self.get_by_id(id, _access_type="view")
            raise Forbidden("You may not lock this run.")

        if run.lock_transaction is not None:
            raise Run.IsLocked()
        run.lock_transaction = self.get_latest_transaction().id
        self.session.commit()
        return run

    @guard("edit")
    def unlock(self, id: int) -> Run:
        run = self.get_by_id(id, _access_type="edit")
        run.lock_transaction = None
        self.session.commit()
        return run
