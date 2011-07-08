#!/bin/bash
cp -R *.s4latin ~/s4/build/s4-image/s4-apps/s4-latin/
sh ~/s4/build/s4-image/scripts/start-s4.sh  > target/cluster.log &
sh ~/s4/build/s4-image/scripts/run-adapter.sh -x -u ~/dev/msc/latin/target/s4-latin-1.0-SNAPSHOT.jar -d target/classes/adapter-conf.xml > target/adapter.log &

