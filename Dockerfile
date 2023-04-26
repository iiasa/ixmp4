FROM python:3.10

ARG POETRY_OPTS

ENV POETRY_OPTS=${POETRY_OPTS} \
    POETRY_VERSION=$POETRY_VERSION \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

ENV \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 

ENV \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 

RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python

ENV PATH="$POETRY_HOME/bin:$PATH"

RUN poetry config virtualenvs.create false

ADD . /opt/ixmp4

WORKDIR /opt/ixmp4


RUN mkdir -p run/logs && \
    touch .env

RUN poetry build --format wheel && \
    poetry export ${POETRY_OPTS} --format requirements.txt --output constraints.txt --without-hashes  && \
    pip install ./dist/*.whl  && \
    pip install -r constraints.txt 


CMD [ "/opt/ixmp4/docker/start.sh" ]
