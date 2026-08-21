#!/bin/sh

echo "prueba" | docker run -i \
  --network=tp-nivelador_default \
  --name validar_server \
  --rm \
  itsthenetwork/alpine-ncat \
  -i 1 server 5678 | grep -q "prueba" \
  && echo "El servidor funciona correctamente" \
  || echo "Error: El servidor no responde correctamente"