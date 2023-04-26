#!/bin/bash

NAME="ixmp4-server"
DIR="${IXMP4_SERVER_DIR:-/opt/ixmp4/run}"
SOCKFILE=$DIR/gunicorn.sock
ACCESS_LOGFILE=$DIR/logs/gunicorn-access.log
ERROR_LOGFILE=$DIR/logs/gunicorn-error.log
NUM_WORKERS="${NUM_WORKERS:-4}"
NUM_THREADS="${NUM_THREADS:-4}"
ASGI_MODULE=ixmp4.server:app

echo "Starting $NAME as `whoami`"

# Create the run directories
test -d $DIR || mkdir -p $DIR
test -d $DIR/logs || mkdir -p $DIR/logs

# Start Gunicorn
exec gunicorn ${ASGI_MODULE} \
  --name $NAME \
  --workers $NUM_WORKERS \
  --threads $NUM_THREADS \
  --worker-class ixmp4.server.workers.UvicornWorker \
  --error-logfile $ERROR_LOGFILE \
  --access-logfile $ACCESS_LOGFILE \
  --bind=unix:$SOCKFILE
