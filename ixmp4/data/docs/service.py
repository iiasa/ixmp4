from typing import Any, List

from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.services import Http, Service, procedure
from ixmp4.transport import DirectTransport

from .db import AbstractDocs
from .dto import Docs
from .filter import DocsFilter
from .repository import ItemRepository as DocsRepository


class DocsService(Service):
    docs_executor: db.r.SessionExecutor
    docs: DocsRepository
    docs_model: type[AbstractDocs]

    def __init_direct__(
        self, transport: DirectTransport, docs_model: type[AbstractDocs] | None = None
    ) -> None:
        if docs_model is not None:
            self.docs_model = docs_model

        self.docs_executor = db.r.SessionExecutor(transport.session)
        self.docs = DocsRepository(
            self.docs_executor,
            target=db.r.ModelTarget(self.docs_model),
            filter=db.r.Filter(DocsFilter, self.docs_model),
        )

    @procedure(Http(path="/{dimension__id}/docs/", methods=["GET"]))
    def get_docs(self, dimension__id: int) -> Docs:
        """Retrieves a docs entry.

        Parameters
        ----------
        dimension__id : int
            The id of the related row.

        Raises
        ------
        :class:`DocsNotFound`:
            If the row with `dimension__id` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.
        """

        return Docs.model_validate(self.docs.get({"dimension__id": dimension__id}))

    # TODO: check run permission
    @get_docs.auth_check()
    def get_docs_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{dimension__id}/docs/", methods=["POST"]))
    def set_docs(self, dimension__id: int, description: str) -> Docs:
        """Updates a docs entry.

        Parameters
        ----------
        dimension__id : int
            The id of the related row.
        description: str
            The updated doc string.

        Raises
        ------
        :class:`DocsNotFound`:
            If the row with `dimension__id` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.
        """
        row_id: int
        try:
            row_id = self.docs.get({"dimension__id": dimension__id}).id
            self.docs.update_by_pk({"id": row_id, "description": description})
        except self.docs.NotFound:
            result = self.docs.create(
                {"dimension__id": dimension__id, "description": description}
            )
            row_id = result.inserted_primary_key.id

        return Docs.model_validate(self.docs.get_by_pk({"id": row_id}))

    # NOTE: Any edit permission suffices to delete any docs row, is this intended?
    @set_docs.auth_check()
    def set_docs_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        # self.auth_ctx.has_management_permission(platform, raise_exc=Forbidden)
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{dimension__id}/docs/", methods=["DELETE"]))
    def delete_docs(self, dimension__id: int) -> None:
        """Deletes a docs entry.

        Parameters
        ----------
        dimension__id : int
            The id of the related row.

        Raises
        ------
        :class:`DocsNotFound`:
            If the row with `dimension__id` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.
        """

        docs = self.docs.get({"dimension__id": dimension__id})
        self.docs.delete_by_pk({"id": docs.id})

    # NOTE: Any edit permission suffices to delete any docs row, is this intended?
    @delete_docs.auth_check()
    def delete_docs_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/docs/list/", methods=["PATCH"]))
    def list_docs(self, **kwargs: Unpack[DocsFilter]) -> list[Docs]:
        r"""Lists docs entries by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `DocsFilter`.

        Raises
        ------
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        list[:class:`Docs`]:
            List of docs entries.
        """

        return [Docs.model_validate(i) for i in self.docs.list(values=kwargs)]

    @list_docs.auth_check()
    def list_docs_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @list_docs.paginated()
    def paginated_list_docs(
        self, pagination: Pagination, **kwargs: Unpack[DocsFilter]
    ) -> PaginatedResult[List[Docs]]:
        return PaginatedResult(
            results=[
                Docs.model_validate(i)
                for i in self.docs.list(
                    values=kwargs, limit=pagination.limit, offset=pagination.offset
                )
            ],
            total=self.docs.count(values=kwargs),
            pagination=pagination,
        )
