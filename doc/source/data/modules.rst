Data Layer (ixmp4.data)
=======================

.. automodule:: ixmp4.data
   :members:
   :undoc-members:
   :show-inheritance:

The :mod:`ixmp4.data` module organizes each datatype into a few files for consistency:

- **db.py**: sqlalchemy database models and other database definitions
- **dto.py**: a data transfer class for item serialization
- **exceptions.py**: exceptions specific to the datatype (NotFound, NotUnique, etc.)
- **filter.py**: filter definitions for use in repositories
- **repositories.py**: repository classes responsible for interacting with the database
- **service.py**: service class as the main interface for the datatype which combines all of the above

The service classes are instantiated together via a :class:`ixmp4.data.backend.Backend` object 
which can be used by other code to perform operations in the database or on a remote ixmp4 
http server. This construct and its classes can be referred to as the "data layer".


.. toctree::
   :maxdepth: 1
   
   common
   iamc
   optimization

   pagination


Backend
-------

.. automodule:: ixmp4.data.backend
   :members:
   :undoc-members:
   :show-inheritance:

