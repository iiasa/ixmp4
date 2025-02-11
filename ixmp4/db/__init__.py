"""
This module is responsible for everything database related.
Interfacing, migrating and more is all done here.

It uses `sqlalchemy <https://www.sqlalchemy.org/>`__ and
`alembic <https://alembic.sqlalchemy.org/en/latest/>`__ for database
management.

Migrations
----------

There is a development database at ``run/db.sqlite`` which is used for
generating migrations, nothing else. It can be manipulated with alembic
directly using these commands:

.. code:: bash

   # run all migrations until the current state is reached
   alembic upgrade head

   # run one migration forward
   alembic upgrade +1

   # run one migration backward
   alembic downgrade -1

   # autogenerate new migration (please choose a descriptive change message)
   alembic revision -m "<message>" --autogenerate

You will have to run all migrations before being able to create new ones
in the development database. Be sure to run ``ruff`` on newly created
migrations before committing them!

"""

from typing import Annotated

from sqlalchemy import (
    BinaryExpression,
    BindParameter,
    ColumnExpressionArgument,
    ForeignKey,
    Index,
    Label,
    Sequence,
    UniqueConstraint,
    and_,
    delete,
    exists,
    false,
    func,
    insert,
    null,
    or_,
    select,
    sql,
    update,
)
from sqlalchemy import Column as typing_column
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError, MultipleResultsFound
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import (
    Bundle,
    MappedColumn,
    Relationship,
    Session,
    aliased,
    backref,
    mapped_column,
    relationship,
    validates,
)
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.types import *

from . import utils

Column = mapped_column
EquationIdType = Annotated[
    int,
    Column(
        Integer,
        ForeignKey("optimization_equation.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    ),
]
# NOTE By using two IndexSetIdTypes, one with ondelete="CASCADE" and one without, we
# enable deletion even when the IndexSet is used in IndexSetData, but prevent it when
# it's used anywhere else
IndexSetIdType = Annotated[
    int,
    Column(Integer, ForeignKey("optimization_indexset.id"), nullable=False, index=True),
]
IndexSet__IdType = Annotated[
    int,
    Column(
        Integer,
        ForeignKey("optimization_indexset.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    ),
]
JsonType = JSON()
# NOTE sqlalchemy's JSON is untyped, but we may not need it if we redesign the opt DB
# model
JsonType = JsonType.with_variant(JSONB(), "postgresql")  # type:ignore[no-untyped-call]
NameType = Annotated[str, Column(String(255), nullable=False, unique=False)]
OptimizationVariableIdType = Annotated[
    int,
    Column(
        Integer,
        ForeignKey("optimization_optimizationvariable.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    ),
]
ParameterIdType = Annotated[
    int,
    Column(
        Integer,
        ForeignKey("optimization_parameter.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    ),
]
RunIdType = Annotated[
    int,
    Column(Integer, ForeignKey("run.id"), nullable=False, index=True),
]
TableIdType = Annotated[
    int,
    Column(
        Integer,
        ForeignKey("optimization_table.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    ),
]
UniqueNameType = Annotated[str, Column(String(255), nullable=False, unique=True)]
UsernameType = Annotated[str, Column(String(255), nullable=True)]
