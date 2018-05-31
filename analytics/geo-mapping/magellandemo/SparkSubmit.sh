#! /bin/bash

spark-submit --class com.o2.uds.MagellanDemo --master local[1] --executor-memory 2G --jars /home/osboxes/.m2/repository/harsha2010/magellan/1.0.5-s_2.11/magellan-1.0.5-s_2.11.jar ./target/magellandemo-1.0-SNAPSHOT.jar ./config.properties

