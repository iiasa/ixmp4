Docker Image
============

You may build a docker image from source using the ``Dockerfile``
in the root folder:

.. code:: bash

    docker build -t ixmp4:latest .

Optionally, supply POETRY_OPTS:

.. code:: bash

    docker build --build-arg POETRY_OPTS="--with docs,dev" -t ixmp4-docs:latest .

Use the image like this in a docker-compose file:

.. code:: yaml

    version: "3"

    services:
    ixmp4_server:
        image: registry.iiasa.ac.at/ixmp4/ixmp4-server:latest
        # To change the amount of workers in a single container
        # override the ixmp4 cli command:
        command:
        - ixmp4
        - server
        - start
        - --host=0.0.0.0
        - --port=9000
        - --workers=2
        volumes:
        - ./run:/opt/ixmp4/run
        env_file:
        - ./.env
        deploy:
        mode: replicated
        replicas: 2
        ports:
        - 9000-9001:9000

This configurations spawns two containers at ports `9000` and `9001` with 2 workers each.
For a list of environment variables used for configuration, refer to the 
:doc:`configuration section <configuration>`.