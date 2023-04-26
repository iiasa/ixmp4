# Building the docs

We use Sphinx and restructured text (rst) for building the documentation pages.

## Writing in Restructured Text

There are a number of guides to get started, for example
on [sourceforge](https://docutils.sourceforge.io/docs/user/rst/quickref.html).

## Building the documentation pages

On \*nix, from the command line, run:

    make html

On Windows, from the command line, run:

    ./make.bat

The rendered html pages will be located in `doc/build/html/index.html`.

To make multiversion docs, use:

    make multiversion

Before re-building, remove the previous build results:

    make clean

To quickly serve them:

    python3 -m http.server 9000 -d ./build

More detailed development docs will be available at:
[localhost:9000/html/devs/modules.html](http://localhost:9000/html/devs/modules.html)

## Publishing the documentation pages

Create an `ixmp4-docs` docker image.

```bash

cd docker
make docs

```

On your webserver, run these commands:

```bash

docker pull "$IMAGE"
docker run \
	--mount type=bind,src=$PUBLIC_DOCS_DIR,\
    dst=/opt/ixmp4/doc/build \
	"$IMAGE" bash -c \
    "ixmp4 server dump-schema -o doc/source/openapi-v1.json && \
    sphinx-multiversion doc/source doc/build"

```
