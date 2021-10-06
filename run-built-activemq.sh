#!/bin/sh

VERSION=5.17.0-SNAPSHOT
command=${1:-restart}

set -e

copy_and_start() {
  rm -rf apache-activemq-${VERSION}/bin

  cd assembly/target/
  tar xf apache-activemq-${VERSION}-bin.tar.gz

  rm -rf broker1 broker2
  mv apache-activemq-${VERSION} broker1
  cp -R broker1 broker2

  cp ../../my-config.xml ./broker1/conf/activemq.xml
  cp ../../my-log4j.properties ./broker1/conf/log4j.properties

  cp ../../my-config2.xml ./broker2/conf/activemq.xml
  cp ../../my-console2.xml ./broker2/conf/jetty.xml
  cp ../../my-log4j.properties ./broker2/conf/log4j.properties

  ./broker1/bin/activemq start
  ./broker2/bin/activemq start

  tail -f ./broker1/data/activemq.log ./broker2/data/activemq.log
}

stop() {
  set +e
  cd assembly/target/

  ./broker1/bin/activemq stop
  ./broker2/bin/activemq stop
}

kill_mq() {
  kill $(ps aux | grep activemq | awk '{print $2}')
}

case "$command" in
  restart)
    stop & copy_and_start
    ;;
  start)
    copy_and_start
    ;;
  stop)
    stop
    ;;
  kill)
    kill_mq
    ;;
  *)
    echo "Unknown command $command"
    exit $?
esac
