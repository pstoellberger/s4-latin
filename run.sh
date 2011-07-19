#!/bin/bash
if [ -d "$S4_IMAGE" ]; then
	echo "---------------------------------------------------"
	echo "S4_IMAGE: $S4_IMAGE"
	echo "---------------------------------------------------"
	echo ""
	rm -rf tmp*
	mvn clean package
	rm -rf $S4_IMAGE/s4-apps/s4-latin
	mkdir $S4_IMAGE/s4-apps/s4-latin/
	cp -R src/main/resources/*.s4latin $S4_IMAGE/s4-apps/s4-latin/
	cp -R target/dist/ $S4_IMAGE/s4-apps/s4-latin/
	sh start_app.sh 
else
   echo "No S4 image found."
   echo "You need to set the environment variable S4_IMAGE to an existing S4 Image!"
fi


