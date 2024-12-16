# IOT_p

## lancement de kafka
cd Téléchargements/kafka_2.12-3.9.0/

bin/kafka-storage.sh random-uuid

bin/kafka-storage.sh format -t <uid> -c config/kraft/server.properties

bin/kafka-server-start.sh config/kraft/server.properties


## lancement hadoop

start-dfs.sh

## lancement de kassandra

cd apache-cassandra-4.1.7
bin/cassndra



## vérification 

hdfs dfs -cat /data_lake/parkinson_data/GaCo04_01.csv
