#!/bin/bash
cp -R *.s4latin ~/s4/build/s4-image/s4-apps/s4-latin/

echo "#######################################################################################"
echo "Starting cluster............"
echo "#######################################################################################"
echo ""
sh ~/s4/build/s4-image/scripts/start-s4.sh  > target/cluster.log &
## THIS IS IMPORTANT. IF THE INPUT ADAPTER ARE LOADED BEFORE THE CLUSTER IT WILL FAIL SILENTLY TO SEND ETC
echo "#######################################################################################"
echo "ATTENTION:... sleeping for 5 seconds to give cluster time to startup"
echo "#######################################################################################"
echo ""
sleep 5
echo ""
echo "#######################################################################################"
echo "SLEEPING DONE!"
echo "#######################################################################################"
sh ~/s4/build/s4-image/scripts/run-adapter.sh -x -u ~/dev/msc/latin/target/s4-latin-1.0-SNAPSHOT.jar -d src/main/resources/adapter-conf.xml > target/adapter.log &

