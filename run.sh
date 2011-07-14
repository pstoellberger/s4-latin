#!/bin/bash
rm -rf tmp*
mvn clean package
rm -rf ~/s4/build/s4-image/s4-apps/s4-latin/
mkdir ~/s4/build/s4-image/s4-apps/s4-latin/
cp -R *.s4latin ~/s4/build/s4-image/s4-apps/s4-latin/
cp -R target/dist/ ~/s4/build/s4-image/s4-apps/s4-latin/
sh start_app.sh 

