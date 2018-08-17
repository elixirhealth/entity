#!/usr/bin/env bash

set -eou pipefail
#set -x  # useful for debugging

docker_cleanup() {
    echo "cleaning up existing network and containers..."
    CONTAINERS='entity'
    docker ps | grep -E ${CONTAINERS} | awk '{print $1}' | xargs -I {} docker stop {} || true
    docker ps -a | grep -E ${CONTAINERS} | awk '{print $1}' | xargs -I {} docker rm {} || true
    docker network list | grep ${CONTAINERS} | awk '{print $2}' | xargs -I {} docker network rm {} || true
}

# optional settings (generally defaults should be fine, but sometimes useful for debugging)
ENTITY_LOG_LEVEL="${ENTITY_LOG_LEVEL:-INFO}"  # or DEBUG
ENTITY_TIMEOUT="${ENTITY_TIMEOUT:-5}"  # 10, or 20 for really sketchy network

# local and filesystem constants
LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# container command constants
ENTITY_IMAGE="gcr.io/elixir-core-prod/entity:snapshot" # develop

echo
echo "cleaning up from previous runs..."
docker_cleanup

echo
echo "creating entity docker network..."
docker network create entity

# TODO start and healthcheck dependency services if necessary

echo
echo "starting entity..."
port=10100
name="entity-0"
docker run --name "${name}" --net=entity -d -p ${port}:${port} ${ENTITY_IMAGE} \
    start \
    --logLevel "${ENTITY_LOG_LEVEL}" \
    --serverPort ${port}
    # TODO add other relevant args if necessary
entity_addrs="${name}:${port}"
entity_containers="${name}"

echo
echo "testing entity health..."
docker run --rm --net=entity ${ENTITY_IMAGE} test health \
    --addresses "${entity_addrs}" \
    --logLevel "${ENTITY_LOG_LEVEL}"

echo
echo "testing entity ..."
docker run --rm --net=entity ${ENTITY_IMAGE} test io \
    --addresses "${entity_addrs}" \
    --logLevel "${ENTITY_LOG_LEVEL}"
    # TODO add other relevant args if necessary

echo
echo "cleaning up..."
docker_cleanup

echo
echo "All tests passed."
