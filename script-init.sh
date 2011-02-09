#!/bin/bash

CHEMIN=/opt/pytransfert/src/
NAME=Pytransfert
DAEMON=pytransfert.py

start() {

        echo -n "Starting $DESC "
        start-stop-daemon --start --quiet -m --pidfile /var/run/pytransfert.pid \
        --chdir $CHEMIN -b --exec /opt/pytransfert/src/$DAEMON 1>&2
        echo "$NAME."
}
stop() {
      echo -n "Stopping $DESC "
      start-stop-daemon --stop --quiet --pidfile /var/run/pytransfert.pid \
      --chdir $CHEMIN --exec /usr/local/bin/python
      echo "$NAME."
}

case "$1" in
    start)
      start
  ;;
    stop)
      stop
  ;;
    restart)
      stop
      start
  ;;
    *)
      echo "Usage: pytransfert {start|stop|restart}"
      exit 1
  ;;
esac
exit 0
