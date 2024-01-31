from typing import ClassVar, Type

from ixmp4.data import abstract

from . import base


class Docs(base.BaseModel):
    NotFound: ClassVar = abstract.Docs.NotFound
    NotUnique: ClassVar = abstract.Docs.NotUnique
    DeletionPrevented: ClassVar = abstract.Docs.DeletionPrevented

    id: int
    description: str
    dimension__id: int


class DocsRepository(
    base.Creator[Docs],
    base.Retriever[Docs],
    base.Deleter[Docs],
    base.Enumerator[Docs],
    abstract.DocsRepository,
):
    model_class: Type[Docs]
    enumeration_method = "GET"

    def get(self, dimension_id: int) -> Docs:
        return super().get(dimension_id=dimension_id)

    def set(self, dimension_id: int, description: str) -> Docs:
        res = self._create(
            self.prefix,
            json={
                "dimension_id": dimension_id,
                "description": description,
            },
        )
        return Docs(**res)

    def list(self, *, dimension_id: int | None = None) -> list[Docs]:
        return super()._list(params={"dimension_id": dimension_id})

    def delete(self, dimension_id: int) -> None:
        super().delete(dimension_id)
