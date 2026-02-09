from typing import Any

from litestar import get
from litestar.di import Provide

from ixmp4.data.backend import Backend
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.data.services.controller import ServiceController

from .dto import Docs


async def get_docs_params(
    dimension_id: int | None = None,
    dimension_id__in: list[int] | None = None,
) -> dict[str, Any]:
    docs_params: dict[str, Any] = {}
    if dimension_id is not None:
        docs_params["dimension__id"] = dimension_id
    if dimension_id__in is not None:
        docs_params["dimension__id__in"] = dimension_id__in
    return docs_params


class DocsCompatibilityController(ServiceController[Any]):
    path = "/docs/"
    dependencies = {
        "docs_params": Provide(get_docs_params),
        **ServiceController.dependencies,
    }
    tags = ["docs"]

    @get(path="/models/", description="Use /models/docs/ instead.", deprecated=True)
    async def list_model_docs(
        self,
        backend: Backend,
        pagination: Pagination,
        docs_params: dict[str, Any],
    ) -> PaginatedResult[list[Docs]]:
        return backend.models.paginated_list_docs(pagination, **docs_params)

    @get(path="/regions/", description="Use /regions/docs/ instead.", deprecated=True)
    async def list_region_docs(
        self,
        backend: Backend,
        pagination: Pagination,
        docs_params: dict[str, Any],
    ) -> PaginatedResult[list[Docs]]:
        return backend.regions.paginated_list_docs(pagination, **docs_params)

    @get(
        path="/scenarios/", description="Use /scenarios/docs/ instead.", deprecated=True
    )
    async def list_scenario_docs(
        self,
        backend: Backend,
        pagination: Pagination,
        docs_params: dict[str, Any],
    ) -> PaginatedResult[list[Docs]]:
        return backend.scenarios.paginated_list_docs(pagination, **docs_params)

    @get(path="/units/", description="Use /units/docs/ instead.", deprecated=True)
    async def list_unit_docs(
        self,
        backend: Backend,
        pagination: Pagination,
        docs_params: dict[str, Any],
    ) -> PaginatedResult[list[Docs]]:
        return backend.units.paginated_list_docs(pagination, **docs_params)

    @get(
        path="/iamc/variables/",
        description="Use /iamc/variables/docs/ instead.",
        deprecated=True,
    )
    async def list_iamc_variable_docs(
        self,
        backend: Backend,
        pagination: Pagination,
        docs_params: dict[str, Any],
    ) -> PaginatedResult[list[Docs]]:
        return backend.iamc.variables.paginated_list_docs(pagination, **docs_params)
