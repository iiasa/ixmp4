Docs
====

Select data types can be associated with a string for
documentation purposes.

.. code:: python

    obj = platform.<object>s.get("") 
    obj.docs = "More information about this object."

The ``docs`` property is an instance of :class:`~ixmp4.core.docs.DocsDescriptor`
that supports ``__get__``, ``__set__`` and ``__delete__``.

.. autoclass:: ixmp4.core.docs.DocsDescriptor
    :members:
    :special-members: __get__ ,__set__, __delete__
    
    