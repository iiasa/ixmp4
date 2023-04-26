# Docker Images

To build and publish:

```bash
cd docker
make
```

Make other variants:

```bash
make docs
make server
```

Or your own:

```bash
make DOCKER_IMAGE_LABEL=ixmp4-custom DOCKER_TAG=v1.2.3 IXMP4_EXTRAS=server,dev,custom
```
