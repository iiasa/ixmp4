"""
This package provides the base service abstraction and the ``procedure``
mechanism that lets service methods be exposed either as direct Python
callables or as HTTP endpoints. The module-level exports make the most
commonly used building blocks available for users of the data layer.

Basic Facilities
----------------

* :class:`~ixmp4.data.services.base.Service`
  abstract base class for data services; provides transport
  helpers and common utilities.

* :func:`~ixmp4.data.services.procedure.procedure`
  decorator used to mark service methods as procedures.

* :class:`Http <ixmp4.data.services.procedure.endpoint.ProcedureHttpConfig>`
  convenient alias for ``ProcedureHttpConfig`` used when
  decorating service methods for HTTP exposure.

Base Classes
------------

* :class:`~ixmp4.data.services.base.GetByIdService`
  specialized ``Service`` interface for types that can be
  retrieved by integer id.

* :class:`~ixmp4.data.services.controller.ServiceController`
  Litestar controller that dispatches to service procedures.

Procedure Internals
-------------------

* :class:`~ixmp4.data.services.procedure.Procedure`:
  internal representation of a service procedure; manages
  adapters for direct and HTTP invocation (see
  :mod:`ixmp4.data.services.procedure`).

* :class:`~ixmp4.data.services.procedure.descriptor.ProcedureDescriptor`:
  descriptor returned by the `procedure``
  decorator; binds a procedure to instances and to transports.

* :class:`~ixmp4.data.services.procedure.endpoint.ProcedureRouteHandler` \
/ :class:`~ixmp4.data.services.procedure.endpoint.ProcedureHttpConfig`:
  HTTP adapter and
  configuration used to expose procedures as HTTP routes.

* :class:`~ixmp4.data.services.procedure.client.ProcedureClient`:
  HTTP client adapter used when a service is backed
  by an :class:`ixmp4.transport.HttpxTransport`.

* :class:`~ixmp4.data.services.procedure.auth.ProcedureAuthCheck` \
/  :class:`~ixmp4.data.services.procedure.pagination.ProcedurePagination`:
  optional wrappers to add authorization checks and pagination support
  to procedures.


Transports re-exported here for convenience:
* ``DirectTransport``: direct (same-process) transport implementation.
* ``HttpxTransport``: HTTP transport using ``httpx``.
* ``Transport``: abstract transport base class.

"""

from ixmp4.transport import DirectTransport as DirectTransport
from ixmp4.transport import HttpxTransport as HttpxTransport
from ixmp4.transport import Transport as Transport

from .base import GetByIdService as GetByIdService
from .base import Service as Service
from .procedure import ProcedureHttpConfig as Http
from .procedure import procedure as procedure

__all__ = [
    "DirectTransport",
    "HttpxTransport",
    "Transport",
    "GetByIdService",
    "Service",
    "Http",
    "procedure",
]
