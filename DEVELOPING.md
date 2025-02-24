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

# Add dynamic versioning plugin
poetry self add "poetry-dynamic-versioning[plugin]"

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
Be sure to run `ruff` on newly created migrations!

## Tests

Run tests with the CLI for a default `pytest` configuration (coverage report, etc.):

```bash
ixmp4 test
```

Or use `pytest` directly:

```bash
py.test
```

### Running tests with PostgreSQL

In order to run the local tests with PostgreSQL you'll need to have a local instance
of this database running. The easiest way to do this using a docker container.

Using the official [`postgres`](https://hub.docker.com/_/postgres) image is recommended.
Get the latest version on you local machine using (having docker installed):

```console
docker pull postgres
```

and run the container with:

```console
docker run -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=test -p 5432:5432 -d postgres
```

Please note that you'll have to wait for a few seconds for the databases to be up and
running.

The `tests/docker-compose.yml` file might help you accomplish all of this with a single
command for your convenience.

```console
docker-compose -f tests/docker-compose.yml up
```

### Benchmarks

The tests contain two types of benchmarks:

- `test_benchmarks.py`
  Simple throughput benchmarks for a
  few operations and pre-conditions.
  Check the test docstrings for more information.
  These tests require a long-format iamc file
  at `./tests/test-data/iamc-test-data_annual_big.csv`

- `test_benchmark_filters.py`
  Benchmarks the retrieval of iamc data with different filters.
  Primarily used for testing query speed.

### Profiling

Some tests will output profiler information to the `.profiles/` directory (using the
`profiled` fixture). You can analyze these using `snakeviz`. For example:

```bash
snakeviz .profiles/test_add_datapoints_full_benchmark\[test_api_pgsql_mp\].prof
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

On release, new images will be built akin to this:

```bash
docker build --build-arg POETRY_OPTS="--with server" -t ixmp4-server:latest .
```

Use the image like this in a docker-compose file:

```yml
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
```

This configurations spawns two containers at ports `9000` and `9001` with 2 workers each.

## Version number

This package uses the poetry-dynamic-versioning plugin to generate a version number
either out of a tag or a current revision.

For this reason the version number is _intentionally_ set to 0.0.0 in `pyproject.toml`.

It is overwritten on the fly by the poetry-dynamic-versioning plugin.

## Release procedure

1. Before releasing, check that the "pytest" GitHub action on the current "main" branch
   passes. Address any failures before releasing.
1. Test on your local machine if the build runs by running `python -m build --sdist
--wheel --outdir dist/`. Fix any packaging issues or errors by creating a PR.

1. Tag the release candidate (RC) version on the main branch as v<release version>rc<N>
   and push to upstream:

```console
git tag v<release version>rc<N>>
git push upstream v<release version>rc<N>
```

1. Check that the GitHub action "Publish ixmp4" was executed correctly and that the
   release candidate was successfully uploaded to TestPyPI. The address will be
   https://test.pypi.org/project/ixmp4/<release version>rc<N>. E.g.:
   <https://test.pypi.org/project/ixmp4/0.2.0rc1/>
1. Visit https://github.com/iiasa/ixmp4/releases and mark the new release by creating
   the tag and release simultaneously. The name of the tag is v<release version>
   (without the rc<N>).
1. Check that the "Publish to PyPI and TestPyPI" GitHub action passed and that the
   distributions are published on https://pypi.org/project/ixmp4/ .
1. Update on [conda-forge](https://github.com/conda-forge/ixmp4-feedstock). 
   A PR should automatically be opened by a bot after the GitHub release (sometimes this 
   takes from 30 minutes to several hours).

   1. Confirm that any new dependencies are added. The minimum versions in meta.yaml 
      should match the versions in pyproject.toml.
   1. Ensure that tests pass and complete any other checklist items.
   1. Merge the PR.
   1. Check that the new package version appears on conda-forge. This may take up to 
      several hours.

## Contributing

Contributions to the code are always welcome! Please make sure your code follows our
code style so that the style is consistent. Each PR will be checked by a Code Quality
test that examines compliance with ruff and mypy.

### Running pre-commit locally

We use [pre-commit](https://pre-commit.com/) to check the code style. You can install
pre-commit locally by installing ixmp4 with the optional `dev` group. Running

```bash
pre-commit install
```

will set pre-commit up to run on every `git commit`. Per default, pre-commit will run
on changed files, but if you want to run it on all files, you can run

```bash
pre-commit run --all-files
```

If you only want certain hooks to run, choose from `ruff` and `mypy` as
`hook-ids` and run

```bash
pre-commit run <hook-ids> --all-files
```

### Ensuring compliance

Whether you run pre-commit locally or see it on your PR for the first time, the checks
are the same. You can, of course, run the code style tools manually. From within the
ixmp4 directory, this would look similar to this:

```bash
mypy .
ruff check .
ruff format .

# Or to enable ruff's automic fixes
ruff check --fix .
```

However, it is easy to forget running these commands manually. Therefore, we recommend
setting your editor up to run at least [ruff](https://docs.astral.sh/ruff/usage/#vs-code)
automatically whenever you hit `save`. A few minutes of configuration will save you time
and nerves later on.
