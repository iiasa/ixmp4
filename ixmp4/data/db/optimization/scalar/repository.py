from collections.abc import Iterable
from functools import partial, reduce
from typing import TYPE_CHECKING, Any, Literal

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4 import db
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.abstract.annotations import HasUnitIdFilter
from ixmp4.data.auth.decorators import guard
from ixmp4.data.db import versions
from ixmp4.data.db.unit.model import Unit, UnitVersion
from ixmp4.data.db.utils import map_existing
from ixmp4.db import utils

from .. import base
from .docs import ScalarDocsRepository
from .model import Scalar, ScalarVersion

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend


class EnumerateKwargs(base.EnumerateKwargs, HasUnitIdFilter, total=False): ...


class ScalarVersionRepository(
    versions.VersionRepository[ScalarVersion], base.BulkUpserter[ScalarVersion]
):
    model_class = ScalarVersion

    def select(
        self,
        transaction__id: int | None = None,
        run__id: int | None = None,
        valid: Literal["at_transaction", "after_transaction"] = "at_transaction",
        **kwargs: Any,
    ) -> db.sql.Select[Any]:
        exc = db.select(self.bundle, UnitVersion.name.label("unit")).select_from(
            self.model_class
        )

        exc = exc.join(UnitVersion, onclause=ScalarVersion.unit__id == UnitVersion.id)

        apply_transaction__id = partial(
            self._apply_transaction__id, transaction__id=transaction__id, valid=valid
        )

        exc = reduce(apply_transaction__id, {self.model_class, UnitVersion}, exc)

        if run__id is not None:
            exc = exc.where(ScalarVersion.run__id == run__id)

        exc = self.where_matches_kwargs(exc, **kwargs)
        return exc.distinct()


class ScalarRepository(
    base.Creator[Scalar],
    base.Deleter[Scalar],
    base.Retriever[Scalar],
    base.Enumerator[Scalar],
    base.Reverter[Scalar],
    abstract.ScalarRepository,
):
    model_class = Scalar
    versions: ScalarVersionRepository

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)
        self.docs = ScalarDocsRepository(*args)

        from .filter import OptimizationScalarFilter

        self.filter_class = OptimizationScalarFilter

        self.versions = ScalarVersionRepository(*args)

    def add(self, name: str, value: float | int, unit_name: str, run_id: int) -> Scalar:
        unit_id = self.backend.units.get(unit_name).id
        scalar = Scalar(
            name=name,
            value=value,
            unit__id=unit_id,
            run__id=run_id,
        )
        self.session.add(scalar)
        return scalar

    @guard("edit")
    def delete(self, id: int) -> None:
        return super().delete(id=id)

    @guard("view")
    def get(self, run_id: int, name: str) -> Scalar:
        exc = db.select(Scalar).where(
            (Scalar.name == name) & (Scalar.run__id == run_id)
        )
        try:
            return self.session.execute(exc).scalar_one()
        except db.NoResultFound:
            raise Scalar.NotFound

    @guard("view")
    def get_by_id(self, id: int) -> Scalar:
        obj = self.session.get(self.model_class, id)

        if obj is None:
            raise Scalar.NotFound(id=id)

        return obj

    @guard("edit")
    def create(self, name: str, value: float, unit_name: str, run_id: int) -> Scalar:
        return super().create(
            name=name, value=value, unit_name=unit_name, run_id=run_id
        )

    @guard("edit")
    def update(
        self, id: int, value: float | None = None, unit_id: int | None = None
    ) -> Scalar:
        scalar = self.get_by_id(id)

        if value is not None:
            scalar.value = value
        if unit_id is not None:
            scalar.unit__id = unit_id

        self.session.commit()

        return scalar

    @guard("view")
    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> Iterable[Scalar]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        return super().tabulate(**kwargs)

    def _map_units_on_revert(self, df: pd.DataFrame) -> pd.DataFrame:
        """Maps foreign-key-unit-ids to the ids of restored Units."""
        # NOTE This df has two unit__id columns: one containing current ids and one ids
        # from before the rollback (called _old). Extra columns like this are ignored by
        # bulk_insert(), it seems.
        df, missing = map_existing(
            df,
            existing_df=self.backend.units.tabulate(name__in=df["unit"]),
            join_on=("name", "unit"),
            map=("id", "unit__id"),
            suffixes=("_old", None),
        )
        if len(missing) > 0:
            raise Unit.NotFound(", ".join(missing))

        return df

    @guard("edit")
    def revert(
        self, transaction__id: int, run__id: int, revert_platform: bool = False
    ) -> None:
        correct_versions: pd.DataFrame | None = None
        if revert_platform:
            old_unit_subquery = self.backend.units.versions._select_for_id_map(
                transaction__id=transaction__id
            ).subquery()
            new_unit_subquery = self.backend.units._select_for_id_map().subquery()

            unit_map_subquery = utils.create_id_map_subquery(
                old_exc=old_unit_subquery, new_exc=new_unit_subquery
            )

            columns = utils.collect_columns_to_select(
                columns=utils.get_columns(self.versions.model_class),
                exclude={"unit__id"},
            )

            select_correct_versions = (
                db.select(
                    unit_map_subquery.c.new_id.label("unit__id"), *columns.values()
                )
                .select_from(self.versions.model_class)
                .join(
                    unit_map_subquery,
                    self.versions.model_class.unit__id == unit_map_subquery.c.old_id,
                )
            )

            select_correct_versions = self.versions._apply_transaction__id(
                exc=select_correct_versions,
                vclass=self.versions.model_class,
                transaction__id=transaction__id,
            ).order_by(self.versions.model_class.id.asc())

            correct_versions = self.tabulate_query(select_correct_versions)

        return super().revert(
            transaction__id=transaction__id,
            run__id=run__id,
            correct_versions=correct_versions,
        )
