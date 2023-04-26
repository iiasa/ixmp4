DOCKER_TAG ?=latest
DOCKER_OPTS ?=
DOCKER_REGISTRY ?=ene-docker.iiasa.ac.at
DOCKER_IMAGE_LABEL ?=ixmp4
DOCKER_IMAGE="${DOCKER_REGISTRY}/${DOCKER_IMAGE_LABEL}:${DOCKER_TAG}"
IXMP4_EXTRAS?=docs,dev

.DEFAULT_GOAL := image
.PHONY: image

docs:
	make DOCKER_IMAGE_LABEL=ixmp4-docs IXMP4_EXTRAS?=docs,server,dev

server:
	make DOCKER_IMAGE_LABEL=ixmp4-server IXMP4_EXTRAS?=server,dev

image: Makefile 
	docker build ${DOCKER_OPTS} --build-arg POETRY_OPTS="--with ${IXMP4_EXTRAS}" -t ${DOCKER_IMAGE} ..
	docker login ${DOCKER_REGISTRY}
	docker push ${DOCKER_IMAGE}
	