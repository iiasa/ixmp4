# Developing

Add yourself to the "authors" section in the `pyproject.toml` file to ensure proper documentation.

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

## Tests

Refer to the [test module docstring](tests/__init__.py) for more information on testing ixmp4.

## Docker Image

Refer to the [docker image documentation](doc/docker.rst) for more information on the ixmp4 docker image.

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
