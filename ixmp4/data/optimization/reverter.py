from typing import Any

import sqlalchemy as sa
from toolkit import db

from ixmp4.data.optimization.equation.db import (
    Equation,
    EquationIndexsetAssociation,
    EquationIndexsetAssociationVersion,
    EquationVersion,
)
from ixmp4.data.optimization.indexset.db import (
    IndexSet,
    IndexSetData,
    IndexSetDataVersion,
    IndexSetVersion,
)
from ixmp4.data.optimization.parameter.db import (
    Parameter,
    ParameterIndexsetAssociation,
    ParameterIndexsetAssociationVersion,
    ParameterVersion,
)
from ixmp4.data.optimization.scalar.db import Scalar, ScalarVersion
from ixmp4.data.optimization.table.db import (
    Table,
    TableIndexsetAssociation,
    TableIndexsetAssociationVersion,
    TableVersion,
)
from ixmp4.data.optimization.variable.db import (
    Variable,
    VariableIndexsetAssociation,
    VariableIndexsetAssociationVersion,
    VariableVersion,
)
from ixmp4.data.versions.reverter import Reverter, ReverterRepository


class IndexSetReverterRepository(ReverterRepository):
    target = db.r.ModelTarget(IndexSet)
    version_target = db.r.ModelTarget(IndexSetVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(IndexSetVersion).where(IndexSetVersion.run__id == run__id)


class IndexSetDataReverterRepository(ReverterRepository):
    target = db.r.ModelTarget(IndexSetData)
    version_target = db.r.ModelTarget(IndexSetDataVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(IndexSetDataVersion).where(
            IndexSetDataVersion.indexset.has(IndexSetVersion.run__id == run__id)
        )


class EquationReverterRepository(ReverterRepository):
    target = db.r.ModelTarget(Equation)
    version_target = db.r.ModelTarget(EquationVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(EquationVersion).where(EquationVersion.run__id == run__id)


class EquationIndexsetAssociationReverterRepository(ReverterRepository):
    target = db.r.ModelTarget(EquationIndexsetAssociation)
    version_target = db.r.ModelTarget(EquationIndexsetAssociationVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(EquationIndexsetAssociationVersion).where(
            EquationIndexsetAssociationVersion.equation.has(
                EquationVersion.run__id == run__id
            )
        )


class ParameterReverterRepository(ReverterRepository):
    target = db.r.ModelTarget(Parameter)
    version_target = db.r.ModelTarget(ParameterVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(ParameterVersion).where(ParameterVersion.run__id == run__id)


class ParameterIndexsetAssociationReverterRepository(ReverterRepository):
    target = db.r.ModelTarget(ParameterIndexsetAssociation)
    version_target = db.r.ModelTarget(ParameterIndexsetAssociationVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(ParameterIndexsetAssociationVersion).where(
            ParameterIndexsetAssociationVersion.parameter.has(
                ParameterVersion.run__id == run__id
            )
        )


class TableReverterRepository(ReverterRepository):
    target = db.r.ModelTarget(Table)
    version_target = db.r.ModelTarget(TableVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(TableVersion).where(TableVersion.run__id == run__id)


class TableIndexsetAssociationReverterRepository(ReverterRepository):
    target = db.r.ModelTarget(TableIndexsetAssociation)
    version_target = db.r.ModelTarget(TableIndexsetAssociationVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(TableIndexsetAssociationVersion).where(
            TableIndexsetAssociationVersion.table.has(TableVersion.run__id == run__id)
        )


class VariableReverterRepository(ReverterRepository):
    target = db.r.ModelTarget(Variable)
    version_target = db.r.ModelTarget(VariableVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(VariableVersion).where(VariableVersion.run__id == run__id)


class VariableIndexsetAssociationReverterRepository(ReverterRepository):
    target = db.r.ModelTarget(VariableIndexsetAssociation)
    version_target = db.r.ModelTarget(VariableIndexsetAssociationVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(VariableIndexsetAssociationVersion).where(
            VariableIndexsetAssociationVersion.variable.has(
                VariableVersion.run__id == run__id
            )
        )


class ScalarReverterRepository(ReverterRepository):
    target = db.r.ModelTarget(Scalar)
    version_target = db.r.ModelTarget(ScalarVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(ScalarVersion).where(ScalarVersion.run__id == run__id)


run_reverter = Reverter(
    targets=[
        IndexSetReverterRepository,
        IndexSetDataReverterRepository,
        EquationReverterRepository,
        EquationIndexsetAssociationReverterRepository,
        ParameterReverterRepository,
        ParameterIndexsetAssociationReverterRepository,
        TableReverterRepository,
        TableIndexsetAssociationReverterRepository,
        VariableReverterRepository,
        VariableIndexsetAssociationReverterRepository,
        ScalarReverterRepository,
    ]
)
