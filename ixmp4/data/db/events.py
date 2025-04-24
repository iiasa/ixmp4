import logging
from collections.abc import Callable, Generator, Mapping, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import Connection, Result, event, sql
from sqlalchemy.orm import Mapper, ORMExecuteState

from ixmp4 import db
from ixmp4.core.exceptions import ProgrammingError
from ixmp4.data.db import base, mixins

if TYPE_CHECKING:
    from ..backend.db import SqlAlchemyBackend


ParametersT = Sequence[Mapping[str, Any]] | Mapping[str, Any]


class SqlaEventHandler(object):
    backend: "SqlAlchemyBackend"

    def __init__(self, backend: "SqlAlchemyBackend") -> None:
        self.backend = backend
        self.listeners: list[
            tuple[tuple[Any, str, Callable[..., None]], dict[str, bool]]
        ] = [
            ((backend.session, "do_orm_execute", self.receive_do_orm_execute), {}),
            (
                (base.BaseModel, "before_insert", self.receive_before_insert),
                {"propagate": True},
            ),
            (
                (base.BaseModel, "before_update", self.receive_before_update),
                {"propagate": True},
            ),
        ]
        self.add_listeners()

    def add_listeners(self) -> None:
        # Somehow, mypy knows how long args is, but tries to insert kwargs as argument
        # 2 and 3
        for args, kwargs in self.listeners:
            if not event.contains(*args):
                event.listen(*args, **kwargs)  # type: ignore[arg-type]

    def remove_listeners(self) -> None:
        for args, kwargs in self.listeners:
            if event.contains(*args):
                event.remove(*args)

    @contextmanager
    def pause(self) -> Generator[None, Any, None]:
        """Temporarily removes all event listeners for the enclosed scope."""
        self.remove_listeners()
        yield
        self.add_listeners()

    def set_logger(self, state: tuple[int, int, int] | ORMExecuteState) -> None:
        self.logger = logging.getLogger(__name__ + "." + str(id(state)))

    def receive_before_insert(
        self, mapper: Mapper[Any], connection: Connection, target: base.BaseModel
    ) -> None:
        """Handles the insert event when creating data like this:
        ```
        model = Model(**kwargs)
        session.add(model)
        session.commit()
        ```
        """
        self.set_logger((id(mapper), id(connection), id(target)))
        if connection.engine is not self.backend.engine:
            self.logger.debug("Event dispatched from another backend, ignoring.")
            return

        if isinstance(target, mixins.HasCreationInfo):
            self.logger.debug(f"Setting creation info for: {target}")
            target.set_creation_info(self.backend.auth_context)

    def receive_before_update(
        self, mapper: Mapper[Any], connection: Connection, target: base.BaseModel
    ) -> None:
        """Handles the update event when changing data like this:
        ```
        model = query_model()
        model.attribute = "foo"
        session.commit()
        ```
        """
        self.set_logger((id(mapper), id(connection), id(target)))
        if connection.engine is not self.backend.engine:
            self.logger.debug("Event dispatched from another backend, ignoring.")
            return

        if isinstance(target, mixins.HasUpdateInfo):
            self.logger.debug(f"Setting update info for: {target}")
            target.set_update_info(self.backend.auth_context)

    def receive_do_orm_execute(self, orm_execute_state: ORMExecuteState) -> None:
        """Handles ORM execution events like:
        ```
        exc = select/update/delete(Model)
        exc = exc.where(<expression>)
        result = session.execute(exc)
        # or
        exc = insert/update(Model)
        result = session.execute(exc, [{"attribute": "foo", ...}, ...])
        ```
        """
        self.set_logger(orm_execute_state)
        self.logger.debug("Received 'do_orm_execute' event.")
        if orm_execute_state.is_select:
            self.receive_select(orm_execute_state)
        else:
            if orm_execute_state.is_insert:
                self.logger.debug("Operation: 'insert'")
                self.receive_insert(orm_execute_state)
            if orm_execute_state.is_update:
                self.logger.debug("Operation: 'update'")
                self.receive_update(orm_execute_state)
            if orm_execute_state.is_delete:
                self.logger.debug("Operation: 'delete'")
                self.receive_delete(orm_execute_state)
            else:
                self.logger.debug(f"Ignoring operation: {orm_execute_state}")

    def receive_select(self, oes: ORMExecuteState) -> None:
        # select = cast(sql.Select, oes.statement)
        pass

    def receive_insert(self, oes: ORMExecuteState) -> Result[Any] | None:
        insert = cast(sql.Insert, oes.statement)
        entity = insert.entity_description
        type_ = entity["type"]
        self.logger.debug(f"Entity: '{entity['name']}'")

        if issubclass(type_, mixins.HasCreationInfo):
            creation_info = {
                "created_by": type_.get_username(self.backend.auth_context),
                "created_at": type_.get_timestamp(),
            }

            assert oes.parameters is not None
            return oes.invoke_statement(
                params=self.get_extra_params(oes.parameters, creation_info)
            )
        return None

    def receive_update(self, oes: ORMExecuteState) -> Result[Any] | None:
        update = cast(sql.Update, oes.statement)
        entity = update.entity_description
        type_ = entity["type"]
        self.logger.debug(f"Entity: '{entity['name']}'")

        if issubclass(type_, mixins.HasUpdateInfo):
            update_info = {
                "updated_by": type_.get_username(self.backend.auth_context),
                "updated_at": type_.get_timestamp(),
            }
            if oes.parameters is not None:
                return oes.invoke_statement(
                    params=self.get_extra_params(oes.parameters, update_info)
                )
            elif update.whereclause is not None:
                new_statement = update.values(**update_info)
                return oes.invoke_statement(statement=new_statement)
            else:
                raise ProgrammingError(f"Cannot handle update statement: {update}")
        return None

    def receive_delete(self, oes: ORMExecuteState) -> None:
        # delete = cast(sql.Delete, oes.statement)
        pass

    # Can't find any reference use cases
    def select_affected(self, statement: sql.Delete | sql.Update) -> sql.Select[Any]:
        entity = statement.entity_description
        wc = statement.whereclause
        exc = db.select(entity["entity"])
        if wc is not None:
            exc.where(wc.expression)
        return exc

    def get_extra_params(
        self,
        params: Sequence[Mapping[str, Any]] | Mapping[str, Any],
        extra: dict[str, Any],
    ) -> list[dict[str, Any]] | dict[str, Any]:
        return [extra] * len(params) if isinstance(params, Sequence) else extra
