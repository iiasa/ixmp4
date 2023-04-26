Tests
=====

Run tests with the CLI for a default configuration:

.. code:: bash

   ixmp4 test [--with-backend] [--with-benchmarks]

Unfortunately, since you are using ixmp4 to execute the tests, global statements are not
included in the coverage calculations. To circumvent this, use the ``--dry`` parameter.

.. code:: bash

   ixmp4 test --with-backend --dry
   # -> pytest --cov-report xml:.coverage.xml --cov-report term --cov=ixmp4 -rsx --benchmark-skip

   eval $(ixmp4 test --with-backend --dry)
   # -> executes pytest

Alternatively, use ``pytest`` directly:

.. code:: bash

   py.test

Running tests with PostgreSQL and ORACLE
----------------------------------------

In order to run the local tests with PostgreSQL or ORACLE you'll need to have a local
instance of this database running. The easiest way to do this is using a docker
container. 

The docker container of the database needs to be started first and then the tests can be
run normally using pytest. If everything is working correctly, the tests for ORACLE or
PostgreSQL should not be skipped.


For PostgreSQL using the official `postgres <https://hub.docker.com/_/postgres>`_ image
is recommended. Get the latest version on your local machine using (having docker
installed):

.. code:: bash

   docker pull postgres

and run the container with:

.. code:: bash

   docker run -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=test -p 5432:5432 -d postgres

for ORACLE you can use the `gvenzl/oracle-xe`
<https://hub.docker.com/r/gvenzl/oracle-xe>`_ image:

.. code:: bash

   docker pull gvenzl/oracle-xe
   docker run -e ORACLE_RANDOM_PASSWORD=true -e APP_USER=ixmp4_test -e APP_USER_PASSWORD=ixmp4_test -p 1521:1521 -d gvenzl/oracle-xe

please note that you'll have to wait for a few seconds for the databases to be up and
running.

In case there are any error messages during the start up of the container along those lines:

.. code:: bash

   ... Error response from daemon: driver failed programming external connectivity on
   endpoint ...
   Error starting userland proxy: listen tcp4 0.0.0.0:5432: bind: address already in
   use.

you have to find the process running on the port in question (in the above case 5432)
and kill it:

.. code:: bash

   sudo ss -lptn 'sport = :5432'
   sudo kill <pid>

Profiling
---------

Some tests will output profiler information to the ``.profiles/``
directory (using the ``profiled`` fixture). You can analyze these using
``snakeviz``. For example:

.. code:: bash

   snakeviz .profiles/test_add_datapoints_full_benchmark.prof
