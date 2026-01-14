Exceptions
==========

Failure cases can result in a wide range of exceptions. 
They can be roughly categorized into:

* `Base Exceptions`_: 
   Low-level failures like web-server or authentication errors
   and base classes for more specific errors.
* `Data Exceptions`_: 
   Failures as results of operations on data in a 
   platform like ``NotFound`` and ``NotUnique`` variants.

All subclasses of :class:`ixmp4.base_exceptions.Ixmp4Error` will be 
serialized to a JSON error response if raised via the http API.
The resulting JSON object will have a standardized structure:

.. code:: json
   
   {
      "name": "<exception name>",
      "message": "<exception message>",
      "http_status_code": 200 /* ... 599 */,
      "data": { /* extra exception data */ },
   }

The exception name can be used to look up the appropriate
exception class in the client exception registry and re-raise
the right exceptions.

.. toctree::
   :hidden:
   :maxdepth: 3
   

Base Exceptions
---------------

.. automodule:: ixmp4.base_exceptions
   :members:
   :undoc-members:
   :show-inheritance:


Data Exceptions
---------------

Run
^^^

.. automodule:: ixmp4.data.run.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Model
^^^^^

.. automodule:: ixmp4.data.model.exceptions
   :members:
   :undoc-members:
   :show-inheritance:
   
Scenario
^^^^^^^^

.. automodule:: ixmp4.data.scenario.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Meta Indicator
^^^^^^^^^^^^^^

.. automodule:: ixmp4.data.meta.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Region
^^^^^^

.. automodule:: ixmp4.data.region.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Unit
^^^^

.. automodule:: ixmp4.data.unit.exceptions
   :members:
   :undoc-members:
   :show-inheritance:


IAMC
^^^^

DataPoint
"""""""""

.. automodule:: ixmp4.data.iamc.datapoint.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Measurand
"""""""""

.. automodule:: ixmp4.data.iamc.measurand.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

TimeSeries
""""""""""

.. automodule:: ixmp4.data.iamc.timeseries.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Variable
""""""""
.. automodule:: ixmp4.data.iamc.variable.exceptions
   :members:
   :undoc-members:
   :show-inheritance:


Optimization
^^^^^^^^^^^^

IndexSet
""""""""

.. automodule:: ixmp4.data.optimization.indexset.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Scalar
""""""

.. automodule:: ixmp4.data.optimization.scalar.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Equation
""""""""

.. automodule:: ixmp4.data.optimization.equation.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Parameter
"""""""""

.. automodule:: ixmp4.data.optimization.parameter.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Table
"""""

.. automodule:: ixmp4.data.optimization.table.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Variable
""""""""

.. automodule:: ixmp4.data.optimization.variable.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

