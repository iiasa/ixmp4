from fastapi import APIRouter, Depends, Path, Query

from ixmp4.data import api
from ixmp4.data.backend.base import Backend

from . import deps
from .base import BaseModel

router: APIRouter = APIRouter(
    prefix="/docs",
    tags=["docs"],
)


class DocsInput(BaseModel):
    dimension_id: int
    description: str


@router.get("/models/", response_model=list[api.Docs])
def list_models(
    dimension_id: int | None = Query(None),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.models.docs.list(dimension_id=dimension_id)


@router.post("/models/", response_model=api.Docs)
def set_models(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.models.docs.set(**docs.dict())


@router.delete("/models/{dimension_id}/")
def delete_models(
    dimension_id: int = Path(None),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.models.docs.delete(dimension_id)


@router.get("/regions/", response_model=list[api.Docs])
def list_regions(
    dimension_id: int | None = Query(None),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.regions.docs.list(dimension_id=dimension_id)


@router.post("/regions/", response_model=api.Docs)
def set_regions(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.regions.docs.set(**docs.dict())


@router.delete("/regions/{dimension_id}/")
def delete_regions(
    dimension_id: int = Path(None),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.regions.docs.delete(dimension_id)


@router.get("/scenarios/", response_model=list[api.Docs])
def list_scenarios(
    dimension_id: int | None = Query(None),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.scenarios.docs.list(dimension_id=dimension_id)


@router.post("/scenarios/", response_model=api.Docs)
def set_scenarios(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.scenarios.docs.set(**docs.dict())


@router.delete("/scenarios/{dimension_id}/")
def delete_scenarios(
    dimension_id: int = Path(None),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.scenarios.docs.delete(dimension_id)


@router.get("/units/", response_model=list[api.Docs])
def list_units(
    dimension_id: int | None = Query(None),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.units.docs.list(dimension_id=dimension_id)


@router.post("/units/", response_model=api.Docs)
def set_units(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.units.docs.set(**docs.dict())


@router.delete("/units/{dimension_id}/")
def delete_units(
    dimension_id: int = Path(None),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.units.docs.delete(dimension_id)


@router.get("/iamc/variables/", response_model=list[api.Docs])
def list_variables(
    dimension_id: int | None = Query(None),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.iamc.variables.docs.list(dimension_id=dimension_id)


@router.post("/iamc/variables/", response_model=api.Docs)
def set_variables(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.iamc.variables.docs.set(**docs.dict())


@router.delete("/iamc/variables/{dimension_id}/")
def delete_variables(
    dimension_id: int = Path(None),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.iamc.variables.docs.delete(dimension_id)


@router.get("/optimization/indexsets/", response_model=list[api.Docs])
def list_indexsets(
    dimension_id: int | None = Query(None),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.indexsets.docs.list(dimension_id=dimension_id)


@router.post("/optimization/indexsets/", response_model=api.Docs)
def set_indexsets(
    docs: DocsInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.indexsets.docs.set(**docs.dict())


@router.delete("/optimization/indexsets/{dimension_id}/")
def delete_indexsets(
    dimension_id: int = Path(None),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.optimization.indexsets.docs.delete(dimension_id)
