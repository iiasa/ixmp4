"""
Configuration centers around the :class:`ixmp4.conf.settings.Settings` class.
When instantiated, this class will read environment variables and `.env` files to
populate the object. Any value can be overridden by passing the appropriate
constructor argument:

.. code:: python

    from ixmp4.conf.settings import Settings
    from ixmp4 import Platform

    settings = Settings(manager_url="https://dev.manager.ece.iiasa.ac.at")

    # use custom manager url for a single platform
    mp = Platform("dev-public", settings=settings)

Two nested settings classes :class:`ixmp4.conf.settings.ClientSettings` and
:class:`ixmp4.conf.settings.ServerSettings` are used to configure the
:class:`ixmp4.transport.HttpxTransport` and :class:`ixmp4.server.Ixmp4Server`
classes respectively,

For the convenience, a local `.env` file can be used to configure the settings object:

.. code:: bash

    IXMP4_MODE=development
    IXMP4_STORAGE_DIRECTORY=~/.local/share/ixmp4/
    IXMP4_MANAGER_URL=https://api.manager.ece.iiasa.ac.at/v1/
    IXMP4_CHECK_ALEMBIC_VERSION=true

    # Server Settings
    IXMP4_SERVER__MANAGER_URL=https://api.manager.ece.iiasa.ac.at/v1/
    IXMP4_SERVER__TOML_PLATFORMS=/custom/path/to/platforms.toml
    IXMP4_SERVER__SECRET_HS256=None
    IXMP4_SERVER__MAX_PAGE_SIZE=10000
    IXMP4_SERVER__DEFAULT_PAGE_SIZE=5000
    IXMP4_SERVER__LOG_EXCEPTIONS=always

    # Client Settings
    IXMP4_CLIENT__DEFAULT_UPLOAD_CHUNK_SIZE=10000
    IXMP4_CLIENT__CONCURRENCY=2
    IXMP4_CLIENT__RETRIES=3
    IXMP4_CLIENT__TIMEOUT=30
    IXMP4_CLIENT__SECRET_HS256=None

"""
