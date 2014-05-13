#!/bin/bash

java -cp $(mvn dependency:build-classpath | grep -v INFO):target/zookeeper-play-1.0-SNAPSHOT.jar play.curator.lock.LockClient 127.0.0.1:2181 $@;

