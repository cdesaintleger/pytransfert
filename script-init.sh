#!/bin/bash

CHEMIN=/media/psf/www/pytransfert/src/

start() {
      echo -n "Starting Pytransfert: "
      cd $CHEMIN
      pwd 
      ./pytransfert.py &
}
stop() {
      echo -n "Stopping Pytransfert: "
      killall -9 /usr/bin/python
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
      echo "Usage: php-fpm {start|stop|restart}"
      exit 1
  ;;
esac
exit 0

