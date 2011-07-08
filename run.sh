#!/bin/bash
rm -rf tmp*
mvn clean package
rm -rf ~/s4/build/s4-image/s4-apps/s4-latin/
mkdir ~/s4/build/s4-image/s4-apps/s4-latin/
#cp -R target/classes/*.xml ~/s4/build/s4-image/s4-apps/s4-latin/
cp -R *.s4latin ~/s4/build/s4-image/s4-apps/s4-latin/
#mkdir ~/s4/build/s4-image/s4-apps/s4-latin/lib/
#cp ~/dev/msc/s4-latin/target/*.jar ~/s4/build/s4-image/s4-apps/s4-latin/lib/
cp -R target/dist/ ~/s4/build/s4-image/s4-apps/s4-latin/
#sh ~/s4/build/s4-image/scripts/start-s4.sh  > target/cluster.log &
#sh ~/s4/build/s4-image/scripts/run-adapter.sh -x -u ~/dev/msc/s4-latin/target/s4-latin-1.0-SNAPSHOT.jar -d target/classes/adapter-conf.xml > target/adapter.log &
sh start_app.sh 

