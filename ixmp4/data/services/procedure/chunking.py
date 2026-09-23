from typing import TYPE_CHECKING, Annotated, Any, get_args, get_origin

import pandas as pd

from ixmp4.base_exceptions import ProgrammingError
from ixmp4.data.dataframe import SerializableDataFrame

if TYPE_CHECKING:
    from . import Procedure


def is_chunkable_annotation(annotation: Any) -> bool:
    """Return ``True`` if *annotation* denotes a chunkable payload.

    Both the ``SerializableDataFrame`` alias and plain ``pandas.DataFrame``
    annotations, as well as ``list`` annotations, are accepted (including
    ``Annotated`` wrappers around them).
    """
    if annotation is SerializableDataFrame or annotation is pd.DataFrame:
        return True

    if annotation is list or get_origin(annotation) is list:
        return True

    if get_origin(annotation) is Annotated:
        return is_chunkable_annotation(get_args(annotation)[0])

    return False


class ProcedureChunking:
    """Metadata describing a procedure's client-side chunking behavior.

    Chunking is declared on the base procedure through the ``chunked``
    argument of :func:`~ixmp4.data.services.procedure.procedure`::

        @procedure(Http(methods=("POST",)), chunked=True)  # auto-detect
        def bulk_upsert(self, df: SerializableDataFrame) -> None: ...


        @procedure(Http(methods=("POST",)), chunked="items")  # explicit
        def store(self, items: list[int]) -> None: ...

    ``chunked=True`` auto-detects the single chunkable argument, which must
    be annotated either as a ``SerializableDataFrame`` or as a ``list``.
    Passing a string selects the chunkable argument by name. This is
    required when the chunkable argument cannot be detected automatically,
    for example when several arguments are chunkable annotations.

    Unlike pagination, chunking is entirely client-side. The
    procedure's HTTP endpoint remains unchanged and keeps receiving a
    single chunk per request. The
    :class:`~ixmp4.data.services.procedure.client.ProcedureClient` splits
    the payload into chunks of
    :attr:`~ixmp4.conf.settings.ClientSettings.default_upload_chunk_size`
    and dispatches one request per chunk.

    No attempt is made to make the chunkable procedure atomic or rollback
    previous chunks when an error occurs. Use with care.
    """

    procedure: "Procedure[Any, Any, Any]"
    has_chunking: bool
    chunk_arg_name: str | None

    def __init__(
        self, procedure: "Procedure[Any, Any, Any]", chunked: bool | str = False
    ) -> None:
        self.procedure = procedure
        self.has_chunking = chunked is not False
        self.chunk_arg_name = None

        if not self.has_chunking:
            return

        if isinstance(chunked, str):
            if chunked not in procedure.signature.parameters:
                raise ProgrammingError(
                    f"Chunkable argument '{chunked}' does not exist in function "
                    f"definition for `{procedure.func.__name__}`."
                )
            self.chunk_arg_name = chunked
        else:
            self.chunk_arg_name = self.detect_chunk_arg()

    def detect_chunk_arg(self) -> str:
        candidates = [
            name
            for name, param in self.procedure.signature.parameters.items()
            if is_chunkable_annotation(param.annotation)
        ]

        if len(candidates) != 1:
            raise ProgrammingError(
                f"Could not determine the chunkable argument for "
                f"`{self.procedure.func.__name__}` automatically. Expected exactly "
                f"one argument annotated as `SerializableDataFrame` or `list`, "
                f"found {len(candidates)}. Specify it explicitly via "
                f'`chunked="<name>"`.'
            )

        return candidates[0]
