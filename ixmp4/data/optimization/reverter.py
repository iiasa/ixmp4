from typing import Any

import sqlalchemy as sa
from toolkit.db.target import ModelTarget

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


class IndexSetReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(IndexSet)
    version_target = ModelTarget(IndexSetVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(IndexSetVersion).where(IndexSetVersion.run__id == run__id)


class IndexSetDataReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(IndexSetData)
    version_target = ModelTarget(IndexSetDataVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(IndexSetDataVersion).where(
            IndexSetDataVersion.indexset.has(IndexSetVersion.run__id == run__id)
        )


class EquationReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(Equation)
    version_target = ModelTarget(EquationVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(EquationVersion).where(EquationVersion.run__id == run__id)


class EquationIndexsetAssociationReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(EquationIndexsetAssociation)
    version_target = ModelTarget(EquationIndexsetAssociationVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(EquationIndexsetAssociationVersion).where(
            EquationIndexsetAssociationVersion.equation.has(
                EquationVersion.run__id == run__id
            )
        )


class ParameterReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(Parameter)
    version_target = ModelTarget(ParameterVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(ParameterVersion).where(ParameterVersion.run__id == run__id)


class ParameterIndexsetAssociationReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(ParameterIndexsetAssociation)
    version_target = ModelTarget(ParameterIndexsetAssociationVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(ParameterIndexsetAssociationVersion).where(
            ParameterIndexsetAssociationVersion.parameter.has(
                ParameterVersion.run__id == run__id
            )
        )


class TableReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(Table)
    version_target = ModelTarget(TableVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(TableVersion).where(TableVersion.run__id == run__id)


class TableIndexsetAssociationReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(TableIndexsetAssociation)
    version_target = ModelTarget(TableIndexsetAssociationVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(TableIndexsetAssociationVersion).where(
            TableIndexsetAssociationVersion.table.has(TableVersion.run__id == run__id)
        )


class VariableReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(Variable)
    version_target = ModelTarget(VariableVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(VariableVersion).where(VariableVersion.run__id == run__id)


class VariableIndexsetAssociationReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(VariableIndexsetAssociation)
    version_target = ModelTarget(VariableIndexsetAssociationVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(VariableIndexsetAssociationVersion).where(
            VariableIndexsetAssociationVersion.variable.has(
                VariableVersion.run__id == run__id
            )
        )


class ScalarReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(Scalar)
    version_target = ModelTarget(ScalarVersion)

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
