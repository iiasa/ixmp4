# The ixmp4 package for scenario data management

Copyright (c) 2023 IIASA - Energy, Climate, and Environment Program (ECE)

[![license: MIT](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://github.com/iiasa/ixmp4/blob/main/LICENSE)
[![python](https://img.shields.io/badge/python-3.10_|_3.11-blue?logo=python&logoColor=white)](https://github.com/iiasa/ixmp4)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Overview

The **ixmp4** package is a data warehouse for high-powered scenario analysis
in the domain of integrated assessment of climate change and energy systems modeling.

## License

The **ixmp4** package is released under the [MIT license](https://github.com/iiasa/ixmp4/blob/main/LICENSE).

## Install from pypi

You can install ixmp4 using pip:

```console
pip install ixmp4
```

## Install from GitHub

For installing the latest version directly from GitHub do the following.

### Requirements

This project requires Python 3.10 (or higher) and poetry (>= 1.2).

### Setup

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
# (using "--with dev,docs,server" if dev and docs dependencies should be installed as well)
poetry install --with dev,docs,server

# Activate virtual environment
poetry shell

# Copy the template environment configuration
cp template.env .env

# Add a test platform
ixmp4 platforms add test

# Start the asgi server
ixmp4 server start
```

## CLI

```bash
ixmp4 --help
```

## Docs

Check [doc/README.md](doc/README.md) on how to build and serve dev documentation locally.

## Docker Image

Check [docker/README.md](docker/README.md) on how to build and publish docker images.

## Developing

See [DEVELOPING.md](DEVELOPING.md) for guidance. When contributing to this project via
a Pull Request, add your name to the "authors" section in the `pyproject.toml` file.

## Funding ackownledgement

<img src="./doc/source/_static/ECEMF-logo.png" width="264" height="100"
alt="ECEMF logo" />
<img src="./doc/source/_static/openENTRANCE-logo.png" width="187" height="120"
alt="openENTRANCE logo" />
<img src="./doc/source/_static/ariadne-bmbf-logo.png" width="353" height="100"
alt="Kopernikus project ARIADNE logo" />

The development of the **ixmp4** package was funded from the EU Horizon 2020 projects
[openENTRANCE](https://openentrance.eu) and [ECEMF](https://ecemf.eu)
as well as the BMBF Kopernikus project [ARIADNE](https://ariadneprojekt.de)
(FKZ 03SFK5A by the German Federal Ministry of Education and Research).

<img src="./doc/source/_static/EU-logo-300x201.jpg" width="80" height="54" align="left"
alt="EU logo" /> This project has received funding from the European Union’s Horizon
2020 research and innovation programme under grant agreement No. 835896 and 101022622.
