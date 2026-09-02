#!/usr/bin/env bash

cantidad_clientes="$1"

server_port="5678"
server_host="server"
dockerfile_name="Dockerfile"
build_context="./services/client"
batch_size="10"

cat > docker-compose.yaml <<EOF
services:
  server:
    build:
      context: ./services/server
      dockerfile: Dockerfile
    container_name: server
    environment:
      - PYTHONUNBUFFERED=1
      - SERVER_HOST=$server_host
      - SERVER_PORT=$server_port
      - AGENCY_QUORUM_MIN=$cantidad_clientes
      - BETS_FILE=/output/bets.csv
    ports:
      - "127.0.0.1:5678:5678"
    volumes:
      - ./output:/output

EOF

{
  for ((i = 0; i <= cantidad_clientes - 1; i++)); do
  cat <<EOF
  client_$i:
    build:
      context: ./services/client
      dockerfile: Dockerfile
    container_name: client_$i
    depends_on:
      - server
    environment:
      - AGENCY_ID=$i
      - SERVER_HOST=$server_host
      - SERVER_PORT=$server_port
      - INPUT_FILE=/input/input-$i.csv
      - OUTPUT_FILE=/output/output-$i.csv
      - BATCH_SIZE=$batch_size
    volumes:
      - ./input:/input:ro
      - ./output:/output

EOF
  done
} >> docker-compose.yaml

sed -i '$d' docker-compose.yaml
