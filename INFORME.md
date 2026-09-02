Redactar un breve informe en donde se detallen los aspectos más importantes de la solución provista, como ser el protocolo de comunicación implementado y los mecanismos para sincronizar la ejecución concurrente.

problemas:
- mandaba en dos llamadas distintas el header del mensaje y luego el contenido. Eso rompia las pruebas de batch
- el maxretry era muy agresivo
- problemas de memoria (copiaba muchas veces)
- un circo hacer la salida graceful
  - Dentro de esto, tambien está el tema de bloqueos por sockets
  - Encima al final lo tuve que cambiar
- implementar los ACK (dios q paja)
- Herramientas de concurrencia:
  - Servidor:
    - Threads + Join
      Por conexion de cliente
    - Lock
      Para acceso al archivo compartido
    - Condiciones (wait + notify_all)
      Para esperar que haya AGENCY_QUORUM_MIN agencias listas
    - Señales
      Para tomar SIGTERM
    - Timeout
      Para evitar tener sockets colgados
  - Cliente
    - Señales
      Para tomar SIGTERM
      Contexto: propaga la cancelacion de SIGTERM
      Callback: fuerza al socket a "terminar" (deadline) en ese instante para poder desbloquear el thread
      Canales: espera que corra el callback de cancelacion y restaurar el deadline de socket
      
    