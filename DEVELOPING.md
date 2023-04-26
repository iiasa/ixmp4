# Developing

Add yourself to the "authors" section in the `pyproject.toml` file to ensure proper documentation.

## Folder Structure

```bash
.
├── ixmp4
│   ├── cli                 # cli
│   ├── conf                # configuration module, loads settings etc.
│   ├── core                # contains the facade layer for the python API
│   ├── data
│   │   ├── abstract        # ABCs for data source models and repositories
│   │   ├── api             # data source implementation for the web api
│   │   ├── backend         # data source backends
│   │   └── db              # data source implementation for databases (sqlalchemy)
│   ├── db                  # database management
│   ├── server              # web application server
│       └── rest            # REST endpoints
├── run                     # runtime artifacts
└── tests                   # tests
```

## Architecture

When using ixmp4 via the python API we would traverse this diagram from left to right.

```
         Platform         Backend                    Server         Backend
   │  ┌────────────┐   ┌───────────┐    ┌─    │   ┌──────────┐   ┌───────────┐  ─┐      │  ┌─┐
 P │  │            │   │           │    │     │   │          │   │           │   │    S │  │ │
 y │  │ ┌────────┐ │   │ ┌───────┐ │    │   R │   │ ┌──────┐ │   │ ┌───────┐ │   │    Q │  │D│
 t │  │ │        │ │   │ │       │ │  ┌─┘   E │   │ │Endp. │ │   │ │       │ │   └─┐  L │  │a│
 h │  │ │Facade  │ │   │ │Model  │ │  │     S │   │ └──────┘ │   │ │Model  │ │     │  A │  │t│
 o │  │ └────────┘ │   │ ├───────┤ │  │     T │   │          │   │ ├───────┤ │     │  l │  │a│
 n │  │            │   │ ├───────┤ │  │       │   │ ┌──────┐ │   │ ├───────┤ │     │  c │  │b│
   │  │    ...     │   │ │       │ │  │     A │   │ │Endp. │ │   │ │       │ │     │  h │  │a│
 A │  │            │   │ │Repo.  │ │  └─┐   P │   │ └──────┘ │   │ │Repo.  │ │   ┌─┘  e │  │s│
 P │  │            │   │ └───────┘ │    │   I │   │          │   │ └───────┘ │   │    m │  │e│
 I │  │            │   │    ...    │    │     │   │   ...    │   │    ...    │   │    y │  │ │
   │  └────────────┘   └───────────┘    └─    │   └──────────┘   └───────────┘  ─┘      │  └─┘

        ixmp4.core        ixmp4.data                ixmp4.server      ixmp4.data
```

Note that the bracketed part of the diagram is only in use when using a web-based platform (using ixmp4 over the REST API). Note also that a REST SDK in another programming language would have to implement only the components before the bracketed part of the diagram (`ixmp4.data.api` + optionally a facade layer).

Overall both the "facade" layer and the "data source" layer are split into "models" (representing a row in a database or a json object) and "repositories" (representing a database table or a collection of REST endpoints) which manage these models.

### ixmp4.core

Contains the user-facing python API ("facade layer") and user documentation.

### ixmp4.data

The module `ixmp4.data.abstract` contains an abstract definition of all data and functionality used by the facade layer of ixmp4. This module also contains internal documentation.

The modules `ixmp4.data.db` and `ixmp4.data.api` contain data source implementations for the abstract "interface". The database implementation uses `sqlalchemy` for database interaction and the API implementation talks to `ixmp4.server` via a web API.

### ixmp4.server

Uses `fastapi` as a micro-framework for the REST API.

## System Dependencies for Ubuntu

```bash
# Install the following package
sudo apt-get install build-essential
sudo apt install python3-dev

```

## Setup

```bash
# Install Poetry, minimum version >=1.2 required
curl -sSL https://install.python-poetry.org | python -

# You may have to reinitialize your shell at this point.
source ~/.bashrc

# Activate in-project virtualenvs
poetry config virtualenvs.in-project true

# Install dependencies
# (using "--with docs" if docs dependencies should be installed as well)
poetry install --with docs,server,dev

# Activate virtual environment
poetry shell

# Copy the template environment configuration
cp template.env .env
```

## Update poetry

Developing ixmp4 requires poetry `>= 1.2`.

If you already have a previous version of poetry installed you will need to update. The
first step is removing the old poetry version:

```bash
curl -sSL https://install.python-poetry.org | python3 - --uninstall
```

after that, the latest poetry version can be installed using:

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

details can be found here in the poetry docs:
<https://python-poetry.org/docs/#installation>.

## Resolve conflicts in poetry.lock

When updating dependencies it can happen that a conflict between the current and the
target poetry.lock file occurs. In this case the following steps should be taken to
resolve the conflict.

1. Do not attempt to manually resolve in the GitHub web interface.
2. Instead checkout the target branch locally and merge into your branch:

```console
git checkout main
git pull origin main
git checkout my-branch
git merge main
```

3. After the last step you'll have a merge conflict in poetry.lock.
4. Instead of resolving the conflict, directly checkout the one from main and rewrite
   it:

```console
# Get poetry.lock to look like it does in master
git checkout main poetry.lock
# Rewrite the lock file
poetry lock --no-update
```

5. After that simply add poetry.lock to mark the conflict as resolved and commit to
   finalize the merge:

```console
git add poetry.lock
git commit

# and most likely needed
poetry install
```

(Taken from <https://www.peterbe.com/plog/how-to-resolve-a-git-conflict-in-poetry.lock>)

## Database(s)

This project uses [sqlalchemy](https://www.sqlalchemy.org/) and [alembic](https://alembic.sqlalchemy.org/en/latest/) for database management. As of now only SQLite databases are supported.

### CLI

ixmp4 itself provides a CLI for interacting with the platforms (i.e. databases) it uses:

```bash
# list all existing databases
ixmp4 platforms list

# run all migrations on all databases in your platforms.toml file
ixmp4 platforms upgrade

# create a new sqlite database
ixmp4 platforms add <platform-name>

# delete a platform
ixmp4 platforms delete <database-name>

```

In development mode additional commands are available:

```bash
# set the revision hash of a database without running migrations
ixmp4 platforms stamp <revision-hash>
```

### Developing Migrations

There is a development database at `run/db.sqlite` which is used for generating migrations, nothing else.
It can be manipulated with alembic directly using these commands:

```bash
# run all migrations until the current state is reached
alembic upgrade head

# run one migration forward
alembic upgrade +1

# run one migration backward
alembic downgrade -1

# autogenerate new migration (please choose a descriptive change message)
alembic revision -m "<message>" --autogenerate
```

You will have to run all migrations before being able to create new ones in the development database.
Be sure to run `black` on newly created migrations!

## Tests

Run tests with the CLI for a default `pytest` configuration (coverage report, etc.):

```bash
ixmp4 test
```

Or use `pytest` directly:

```bash
py.test
```

### Running tests with PostgreSQL and ORACLE

In order to run the local tests with PostgreSQL or ORACLE you'll need to have a local
instance of this database running.
The easiest way to do this using a docker container.

For PostgreSQL using the official [`postgres`](https://hub.docker.com/_/postgres) image
is recommended. Get the latest version on you local machine using (having docker
installed):

```console
docker pull postgres
```

and run the container with:

```console
docker run -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=test -p 5432:5432 -d postgres
```

for ORACLE you can use the the [`gvenzl/oracle-xe`](https://hub.docker.com/r/gvenzl/oracle-xe) image:

```console
docker pull gvenzl/oracle-xe
docker run -e ORACLE_RANDOM_PASSWORD=true -e APP_USER=ixmp4_test -e APP_USER_PASSWORD=ixmp4_test -p 1521:1521 -d gvenzl/oracle-xe
```

please note that you'll have to wait for a few seconds for the databases to be up and
running.

### Profiling

Some tests will output profiler information to the `.profiles/` directory (using the `profiled` fixture). You can analyze these using `snakeviz`. For example:

```bash
snakeviz .profiles/test_add_datapoints_full_benchmark.prof
```

## Web API

Run the web api with:

```bash
ixmp4 server start
```

This will start ixmp4's asgi server. Check `http://127.0.0.1:9000/v1/<platform>/docs`.

## Docker Image

To build:

```bash
docker build -t ixmp4:latest .
```

Optionally, supply POETRY_OPTS:

```bash
docker build --build-arg POETRY_OPTS="--with docs,dev" -t ixmp4-docs:latest .
```
