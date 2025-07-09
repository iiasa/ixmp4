FROM python:3.12

ARG POETRY_OPTS
ARG POETRY_VERSION="1.8.3"

ENV POETRY_OPTS=${POETRY_OPTS} \
    POETRY_VERSION=${POETRY_VERSION} \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

ENV \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 

ENV \
    PIP_NO_CACHE_DIR=off \
    PIP_PROGRESS_BAR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 

RUN pip install poetry

ENV PATH="$POETRY_HOME/bin:$PATH"

RUN poetry config virtualenvs.create false

COPY . /opt/ixmp4

WORKDIR /opt/ixmp4


RUN mkdir -p run/logs && \
    touch .env

RUN poetry self add "poetry-dynamic-versioning[plugin]"  && \
    poetry dynamic-versioning
RUN poetry build --format wheel && \
    poetry export ${POETRY_OPTS} --format requirements.txt --output constraints.txt --without-hashes
RUN pip install ./dist/*.whl  && \
    pip install -r constraints.txt 


EXPOSE 9000
CMD [ "ixmp4", "server", "start", "--host=0.0.0.0", "--port=9000" ]
