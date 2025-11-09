from contextlib import suppress
from typing import Any, List

from toolkit import db
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance
from typing_extensions import Unpack

from ixmp4.rewrite.data.dataframe import SerializableDataFrame
from ixmp4.rewrite.data.model.repositories import ItemRepository as ModelRepository
from ixmp4.rewrite.data.model.repositories import ModelNotUnique
from ixmp4.rewrite.data.pagination import PaginatedResult, Pagination
from ixmp4.rewrite.data.scenario.repositories import (
    ItemRepository as ScenarioRepository,
)
from ixmp4.rewrite.data.scenario.repositories import ScenarioNotUnique
from ixmp4.rewrite.data.versions.transaction import TransactionRepository
from ixmp4.rewrite.exceptions import Forbidden
from ixmp4.rewrite.services import (
    Service,
    paginated_procedure,
    procedure,
)
from ixmp4.rewrite.transport import DirectTransport

from .dto import Run
from .filter import RunFilter
from .repositories import (
    ItemRepository,
    NoDefaultRunVersion,
    PandasRepository,
    PandasVersionRepository,
    RunIsLocked,
    RunNotFound,
)


class RunService(Service):
    router_prefix = "/runs"
    router_tags = ["runs"]

    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository
    models: ModelRepository
    scenarios: ScenarioRepository
    transactions: TransactionRepository

    default_columns = [
        "id",
        "model",
        "scenario",
        "model__id",
        "scenario__id",
        "version",
        "lock_transaction",
        "is_default",
        "created_by",
        "created_at",
        "updated_by",
        "updated_at",
    ]

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)
        self.pandas_versions = PandasVersionRepository(self.executor)
        self.models = ModelRepository(self.executor)
        self.scenarios = ScenarioRepository(self.executor)
        self.transactions = TransactionRepository(self.executor)

    @procedure(methods=["POST"])
    def create(self, model_name: str, scenario_name: str) -> Run:
        """Creates a run with an incremented version number or version=1 if no versions
        exist. Will automatically create the models and scenarios if they don't exist
        yet.

        Parameters
        ----------
        model_name : str
            The name of a model.
        scenario_name : str
            The name of a scenario.

        Returns
        -------
        :class:`Run`:
            The created run.
        """
        creation_info = self.get_creation_info()
        with suppress(ModelNotUnique):
            self.models.create({"name": model_name, **creation_info})
        model = self.models.get({"name": model_name})

        with suppress(ScenarioNotUnique):
            self.scenarios.create({"name": scenario_name, **creation_info})
        scenario = self.scenarios.get({"name": scenario_name})

        id_ = self.items.create(model.id, scenario.id, values=creation_info)
        return Run.model_validate(self.items.get_by_pk({"id": id_}))

    @create.auth_check()
    def create_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        model_name: str,
        scenario_name: str,
    ) -> None:
        auth_ctx.has_edit_permission(platform, models=[model_name], raise_exc=Forbidden)

    @procedure(path="/{id}/", methods=["DELETE"])
    def delete_by_id(self, id: int) -> None:
        """Deletes a run and **all associated iamc, optimization and meta data**.

        Parameters
        ----------
        id : int
            The unique integer id of the run.

        Raises
        ------
        :class:`RunNotFound`:
            If the run with `id` does not exist.
        """

        self.items.delete_by_pk({"id": id})

    @delete_by_id.auth_check()
    def delete_by_id_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_management_permission(platform, raise_exc=Forbidden)

    @procedure(methods=["POST"])
    def get(self, model_name: str, scenario_name: str, version: int) -> Run:
        """Retrieves a run.

        Parameters
        ----------
        model_name : str
            The name of a model.
        scenario_name : str
            The name of a scenario.
        version : int
            The version number of this run.

        Raises
        ------
        :class:`RunNotFound`:
            If the run with `model_name`, `scenario_name` and `version` does not exist.

        Returns
        -------
        :class:`Run`:
            The retrieved run.
        """
        result = self.items.get(
            {
                "version": version,
                "scenario": {"name": scenario_name},
                "model": {"name": model_name},
            }
        )
        return Run.model_validate(result)

    @get.auth_check()
    def get_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        model_name: str,
        scenario_name: str,
    ) -> None:
        auth_ctx.has_view_permission(platform, models=[model_name], raise_exc=Forbidden)

    def get_or_create(self, model_name: str, scenario_name: str) -> Run:
        """Tries to retrieve a run's default version
        and creates it if it was not found.

        Parameters
        ----------
        model_name : str
            The name of a model.
        scenario_name : str
            The name of a scenario.

        Returns
        -------
        :class:`Run`:
            The retrieved or created run.
        """

        try:
            return self.get_default_version(model_name, scenario_name)
        except NoDefaultRunVersion:
            return self.create(model_name, scenario_name)

    @procedure(methods=["POST"])
    def get_default_version(self, model_name: str, scenario_name: str) -> Run:
        """Retrieves a run's default version.

        Parameters
        ----------
        model_name : str
            The name of a model.
        scenario_name : str
            The name of a scenario.

        Raises
        ------
        :class:`NoDefaultRunVersion`:
            If no runs with `model_name`, `scenario_name` and `is_default=True` exist.

        Returns
        -------
        :class:`Run`:
            The retrieved run.
        """
        try:
            result = self.items.get(
                {
                    "is_default": True,
                    "scenario": {"name": scenario_name},
                    "model": {"name": model_name},
                }
            )
        except RunNotFound as e:
            raise NoDefaultRunVersion() from e

        return Run.model_validate(result)

    @get_default_version.auth_check()
    def get_default_version_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        model_name: str,
        scenario_name: str,
    ) -> None:
        auth_ctx.has_view_permission(platform, models=[model_name], raise_exc=Forbidden)

    @procedure(path="/{id}/", methods=["GET"])
    def get_by_id(self, id: int) -> Run:
        """Retrieves a Run by its id.

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`RunNotFound`.
            If the Run with `id` does not exist.

        Returns
        -------
        :class:`Run`:
            The retrieved Run.
        """
        result = self.items.get_by_pk({"id": id})
        return Run.model_validate(result)

    @get_by_id.auth_check()
    def get_by_id_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @paginated_procedure(methods=["PATCH"])
    def list(self, **kwargs: Unpack[RunFilter]) -> list[Run]:
        r"""Lists runs by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `RunFilter`.

        Returns
        -------
        Iterable[:class:`Run`]:
            List of runs.
        """
        return [Run.model_validate(i) for i in self.items.list(values=kwargs)]

    @list.auth_check()
    def list_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[RunFilter]
    ) -> PaginatedResult[List[Run]]:
        return PaginatedResult(
            results=[
                Run.model_validate(i)
                for i in self.items.list(
                    values=kwargs, limit=pagination.limit, offset=pagination.offset
                )
            ],
            total=self.items.count(values=kwargs),
            pagination=pagination,
        )

    @paginated_procedure(methods=["PATCH"])
    def tabulate(self, **kwargs: Unpack[RunFilter]) -> SerializableDataFrame:
        r"""Tabulate runs by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `RunFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - model__id
                - scenario__id
        """
        return self.pandas.tabulate(values=kwargs, columns=self.default_columns)

    @tabulate.auth_check()
    def tabulate_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @tabulate.paginated()
    def paginated_tabulate(
        self, pagination: Pagination, **kwargs: Unpack[RunFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs,
                limit=pagination.limit,
                offset=pagination.offset,
                columns=self.default_columns,
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )

    @procedure(methods=["POST"])
    def set_as_default_version(self, id: int) -> None:
        """Sets a run as the default version for a (model, scenario) combination.

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`RunNotFound`:
            If no run with the `id` exists.

        """
        self.items.set_as_default_version(id)

    @set_as_default_version.auth_check()
    def set_as_default_version_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        run = self.items.get_by_pk({"id": id})
        auth_ctx.has_edit_permission(
            platform, models=[run.model.name], raise_exc=Forbidden
        )

    @procedure(methods=["POST"])
    def unset_as_default_version(self, id: int) -> None:
        """Unsets a run as the default version leaving no
        default version for a (model, scenario) combination.

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`RunNotFound`:
            If no run with the `id` exists.
        :class:`ixmp4.core.exceptions.IxmpError`:
            If the run is not set as a default version.

        """
        self.items.unset_as_default_version(id)

    @unset_as_default_version.auth_check()
    def unset_as_default_version_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        run = self.items.get_by_pk({"id": id})
        auth_ctx.has_edit_permission(
            platform, models=[run.model.name], raise_exc=Forbidden
        )

    @procedure(methods=["POST"])
    def revert(
        self, id: int, transaction__id: int, revert_platform: bool = False
    ) -> None:
        """Reverts run data to a specific `transaction__id`.

        Parameters
        ----------
        id : int
            Unique integer id.
        transaction__id : int
            Id of the transaction to revert to.
        revert_platform : bool, optional
            Whether to revert the units defined on the platform, too. Default `False`.

        Raises
        ------
        :class:`RunNotFound`:
            If no run with the `id` exists.
        """
        ...

    @revert.auth_check()
    def revert_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        id: int,
        transaction__id: int,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        run = self.items.get_by_pk({"id": id})
        auth_ctx.has_edit_permission(
            platform, models=[run.model.name], raise_exc=Forbidden
        )

    @procedure(methods=["POST"])
    def lock(self, id: int) -> Run:
        """Locks a run at the current transaction (via `transaction__id`).

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`RunNotFound`:
            If no run with the `id` exists.
        :class:`RunIsLocked`:
            If the run is already locked.

        """
        run = self.items.get_by_pk({"id": id})

        if run.lock_transaction is not None:
            raise RunIsLocked()

        latest_transaction = self.transactions.latest()
        self.items.update_by_pk({"id": id, "lock_transaction": latest_transaction.id})
        return Run.model_validate(self.items.get_by_pk({"id": id}))

    @lock.auth_check()
    def lock_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        id: int,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        run = self.items.get_by_pk({"id": id})
        auth_ctx.has_edit_permission(
            platform, models=[run.model.name], raise_exc=Forbidden
        )

    @procedure(methods=["POST"])
    def unlock(self, id: int) -> Run:
        """Locks a run at the current transaction (via `transaction__id`).

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`RunNotFound`:
            If no run with the `id` exists.
        """
        self.items.update_by_pk({"id": id, "lock_transaction": None})
        return Run.model_validate(self.items.get_by_pk({"id": id}))

    @unlock.auth_check()
    def unlock_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        id: int,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        run = self.items.get_by_pk({"id": id})
        auth_ctx.has_edit_permission(
            platform, models=[run.model.name], raise_exc=Forbidden
        )

    @procedure(methods=["POST"])
    def clone(
        self,
        run_id: int,
        model_name: str | None = None,
        scenario_name: str | None = None,
        keep_solution: bool = True,
    ) -> Run:
        """Clone all data from one run to a new one.

        Parameters
        ----------
        run_id: int
            The unique integer id of the base run.
        model_name: str | None
            The new name of the model used in the new run, optional.
        scenario_name: str | None
            The new name of the scenario used in the new run, optional.
        keep_solution: bool
            Whether to keep the solution data from the base run. Optional, defaults to
            `True`.

        Returns
        -------
        :class:`Run`:
            The clone of the base run.
        """
        # TODO
        raise NotImplementedError

    @clone.auth_check()
    def clone_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        run_id: int,
        model_name: str | None = None,
        scenario_name: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        run = self.items.get_by_pk({"id": run_id})
        auth_ctx.has_view_permission(
            platform, models=[run.model.name], raise_exc=Forbidden
        )
        auth_ctx.has_edit_permission(platform, models=[model_name], raise_exc=Forbidden)
