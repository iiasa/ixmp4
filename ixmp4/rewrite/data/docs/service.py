from typing import List

from toolkit import db
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance
from typing_extensions import Unpack

from ixmp4.rewrite.data.pagination import PaginatedResult, Pagination
from ixmp4.rewrite.exceptions import Forbidden
from ixmp4.rewrite.services import Service, paginated_procedure, procedure
from ixmp4.rewrite.transport import DirectTransport

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

    @procedure(methods=["POST"])
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

        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

        return Docs.model_validate(self.docs.get({"dimension__id": dimension__id}))

    @procedure(methods=["POST"])
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

        # self.auth_ctx.has_management_permission(platform,             raise_exc=Forbidden)
        # NOTE: Any edit permission suffices to delete any docs row, is this intended?
        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

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

    @procedure(path="/{dimension__id}/", methods=["DELETE"])
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

        # self.auth_ctx.has_management_permission(platform,             raise_exc=Forbidden)
        # NOTE: Any edit permission suffices to delete any docs row, is this intended?
        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

        docs = self.docs.get({"dimension__id": dimension__id})
        self.docs.delete_by_pk({"id": docs.id})

    @paginated_procedure(methods=["PATCH"])
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

        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

        return [Docs.model_validate(i) for i in self.docs.list(values=kwargs)]

    @list_docs.paginated()
    def paginated_list_docs(
        self, pagination: Pagination, **kwargs: Unpack[DocsFilter]
    ) -> PaginatedResult[List[Docs]]:
        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

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
