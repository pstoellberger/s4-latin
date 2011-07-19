#!/bin/bash
cp -R src/main/resources/*.s4latin $S4_IMAGE/s4-apps/s4-latin/


echo "#######################################################################################"
echo "Starting cluster............"
echo "#######################################################################################"
echo ""
sh $S4_IMAGE/scripts/start-s4.sh  > target/cluster.log &
## THIS IS IMPORTANT. IF THE INPUT ADAPTER ARE LOADED BEFORE THE CLUSTER IT WILL FAIL SILENTLY TO SEND ETC
echo "#######################################################################################"
echo "ATTENTION:... sleeping for 5 seconds to give cluster time to startup"
echo "#######################################################################################"
echo ""
sleep 16
echo ""
echo "#######################################################################################"
echo "SLEEPING DONE!"
echo "#######################################################################################"
sh $S4_IMAGE/scripts/run-adapter.sh -x -u ~/dev/msc/latin/target/s4-latin-1.0-SNAPSHOT.jar -d src/main/resources/adapter-conf.xml > target/adapter.log &
echo "#######################################################################################"
echo "Cluster and adapter started!"
echo "#######################################################################################"

