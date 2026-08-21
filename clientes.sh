#!/usr/bin/env bash

cantidad_clientes="$1"

server_port="5678"
server_host="server"
dockerfile_name="Dockerfile"
build_context="./services/client"

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

EOF

{
  for ((i = 1; i <= cantidad_clientes; i++)); do
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

EOF
  done
} >> docker-compose.yaml

sed -i '$d' docker-compose.yaml