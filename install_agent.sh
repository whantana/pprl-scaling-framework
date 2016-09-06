#!/usr/bin/env bash
mkdir agent
cd agent
wget http://javamex.com/classmexer/classmexer-0_03.zip
unzip classmexer-0_03.zip
mvn install:install-file -Dfile=classmexer.jar -DgroupId=com.javamex.classmexer -DartifactId=classmexer -Dversion=0.0.3 -Dpackaging=jar
cd ..
rm -rf agent