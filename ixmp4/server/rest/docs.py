from typing import cast

from fastapi import APIRouter, Depends, Path, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.docs import AbstractDocs

from . import deps
from .base import BaseModel, EnumerationOutput, Pagination

router: APIRouter = APIRouter(
    prefix="/docs",
    tags=["docs"],
)


class DocsInput(BaseModel):
    dimension_id: int
    description: str


@router.get("/models/", response_model=EnumerationOutput[api.Docs])
def list_models(
    dimension_id: int | None = Query(None),
    dimension_id__in: list[int] | None = Query(None),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[AbstractDocs]:
    return EnumerationOutput(
        results=backend.models.docs.list(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        total=backend.models.docs.count(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        pagination=pagination,
    )


@router.post("/models/", response_model=api.Docs)
def set_models(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
) -> api.Docs:
    return cast(api.Docs, backend.models.docs.set(**docs.model_dump()))


@router.delete("/models/{dimension_id}/")
def delete_models(
    dimension_id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.models.docs.delete(dimension_id)


@router.get("/regions/", response_model=EnumerationOutput[api.Docs])
def list_regions(
    dimension_id: int | None = Query(None),
    dimension_id__in: list[int] | None = Query(None),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[AbstractDocs]:
    return EnumerationOutput(
        results=backend.regions.docs.list(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        total=backend.regions.docs.count(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        pagination=pagination,
    )


@router.post("/regions/", response_model=api.Docs)
def set_regions(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
) -> api.Docs:
    return cast(api.Docs, backend.regions.docs.set(**docs.model_dump()))


@router.delete("/regions/{dimension_id}/")
def delete_regions(
    dimension_id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.regions.docs.delete(dimension_id)


@router.get("/scenarios/", response_model=EnumerationOutput[api.Docs])
def list_scenarios(
    dimension_id: int | None = Query(None),
    dimension_id__in: list[int] | None = Query(None),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[AbstractDocs]:
    return EnumerationOutput(
        results=backend.scenarios.docs.list(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        total=backend.scenarios.docs.count(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        pagination=pagination,
    )


@router.post("/scenarios/", response_model=api.Docs)
def set_scenarios(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
) -> api.Docs:
    return cast(api.Docs, backend.scenarios.docs.set(**docs.model_dump()))


@router.delete("/scenarios/{dimension_id}/")
def delete_scenarios(
    dimension_id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.scenarios.docs.delete(dimension_id)


@router.get("/units/", response_model=EnumerationOutput[api.Docs])
def list_units(
    dimension_id: int | None = Query(None),
    dimension_id__in: list[int] | None = Query(None),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[AbstractDocs]:
    return EnumerationOutput(
        results=backend.units.docs.list(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        total=backend.units.docs.count(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        pagination=pagination,
    )


@router.post("/units/", response_model=api.Docs)
def set_units(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
) -> api.Docs:
    return cast(api.Docs, backend.units.docs.set(**docs.model_dump()))


@router.delete("/units/{dimension_id}/")
def delete_units(
    dimension_id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.units.docs.delete(dimension_id)


@router.get("/iamc/variables/", response_model=EnumerationOutput[api.Docs])
def list_variables(
    dimension_id: int | None = Query(None),
    dimension_id__in: list[int] | None = Query(None),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[AbstractDocs]:
    return EnumerationOutput(
        results=backend.iamc.variables.docs.list(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        total=backend.iamc.variables.docs.count(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        pagination=pagination,
    )


@router.post("/iamc/variables/", response_model=api.Docs)
def set_variables(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
) -> api.Docs:
    return cast(api.Docs, backend.iamc.variables.docs.set(**docs.model_dump()))


@router.delete("/iamc/variables/{dimension_id}/")
def delete_variables(
    dimension_id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.iamc.variables.docs.delete(dimension_id)


@router.get("/optimization/indexsets/", response_model=EnumerationOutput[api.Docs])
def list_indexsets(
    dimension_id: int | None = Query(None),
    dimension_id__in: list[int] | None = Query(None),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[AbstractDocs]:
    return EnumerationOutput(
        results=backend.optimization.indexsets.docs.list(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        total=backend.optimization.indexsets.docs.count(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        pagination=pagination,
    )


@router.post("/optimization/indexsets/", response_model=api.Docs)
def set_indexsets(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
) -> api.Docs:
    return cast(api.Docs, backend.optimization.indexsets.docs.set(**docs.model_dump()))


@router.delete("/optimization/indexsets/{dimension_id}/")
def delete_indexsets(
    dimension_id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.indexsets.docs.delete(dimension_id)


@router.get("/optimization/scalars/", response_model=EnumerationOutput[api.Docs])
def list_scalars(
    dimension_id: int | None = Query(None),
    dimension_id__in: list[int] | None = Query(None),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[AbstractDocs]:
    return EnumerationOutput(
        results=backend.optimization.scalars.docs.list(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        total=backend.optimization.scalars.docs.count(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        pagination=pagination,
    )


@router.post("/optimization/scalars/", response_model=api.Docs)
def set_scalars(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
) -> api.Docs:
    return cast(api.Docs, backend.optimization.scalars.docs.set(**docs.model_dump()))


@router.delete("/optimization/scalars/{dimension_id}/")
def delete_scalars(
    dimension_id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.scalars.docs.delete(dimension_id)


@router.get("/optimization/tables/", response_model=EnumerationOutput[api.Docs])
def list_tables(
    dimension_id: int | None = Query(None),
    dimension_id__in: list[int] | None = Query(None),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[AbstractDocs]:
    return EnumerationOutput(
        results=backend.optimization.tables.docs.list(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        total=backend.optimization.tables.docs.count(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        pagination=pagination,
    )


@router.post("/optimization/tables/", response_model=api.Docs)
def set_tables(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
) -> api.Docs:
    return cast(api.Docs, backend.optimization.tables.docs.set(**docs.model_dump()))


@router.delete("/optimization/tables/{dimension_id}/")
def delete_tables(
    dimension_id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.tables.docs.delete(dimension_id)


@router.get("/optimization/parameters/", response_model=EnumerationOutput[api.Docs])
def list_parameters(
    dimension_id: int | None = Query(None),
    dimension_id__in: list[int] | None = Query(None),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[AbstractDocs]:
    return EnumerationOutput(
        results=backend.optimization.parameters.docs.list(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        total=backend.optimization.parameters.docs.count(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        pagination=pagination,
    )


@router.post("/optimization/parameters/", response_model=api.Docs)
def set_parameters(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
) -> api.Docs:
    return cast(api.Docs, backend.optimization.parameters.docs.set(**docs.model_dump()))


@router.delete("/optimization/parameters/{dimension_id}/")
def delete_parameters(
    dimension_id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.parameters.docs.delete(dimension_id)


@router.get("/optimization/variables/", response_model=EnumerationOutput[api.Docs])
def list_optimization_variables(
    dimension_id: int | None = Query(None),
    dimension_id__in: list[int] | None = Query(None),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[AbstractDocs]:
    return EnumerationOutput(
        results=backend.optimization.variables.docs.list(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        total=backend.optimization.variables.docs.count(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        pagination=pagination,
    )


@router.post("/optimization/variables/", response_model=api.Docs)
def set_optimization_variables(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
) -> api.Docs:
    return cast(api.Docs, backend.optimization.variables.docs.set(**docs.model_dump()))


@router.delete("/optimization/variables/{dimension_id}/")
def delete_optimization_variables(
    dimension_id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.variables.docs.delete(dimension_id)


@router.get("/optimization/equations/", response_model=EnumerationOutput[api.Docs])
def list_equations(
    dimension_id: int | None = Query(None),
    dimension_id__in: list[int] | None = Query(None),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
) -> EnumerationOutput[AbstractDocs]:
    return EnumerationOutput(
        results=backend.optimization.equations.docs.list(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        total=backend.optimization.equations.docs.count(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        ),
        pagination=pagination,
    )


@router.post("/optimization/equations/", response_model=api.Docs)
def set_equations(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
) -> api.Docs:
    return cast(api.Docs, backend.optimization.equations.docs.set(**docs.model_dump()))


@router.delete("/optimization/equations/{dimension_id}/")
def delete_equations(
    dimension_id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
) -> None:
    backend.optimization.equations.docs.delete(dimension_id)
