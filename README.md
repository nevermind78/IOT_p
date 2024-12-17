# IOT_p

## lancement de kafka
cd ~/Téléchargements/kafka_2.12-3.9.0/

bin/kafka-storage.sh random-uuid

bin/kafka-storage.sh format -t 8i8O1MBaT7qAzQlrNDGghg -c config/kraft/server.properties

bin/kafka-server-start.sh config/kraft/server.properties




## lancement hadoop

start-dfs.sh

## lancement de kassandra

cd ~/apache-cassandra-4.1.7 && bin/cassandra

## exécution

cd /home/nevermind/Documents/p
python sensors_data_receiver.py 
python sensors_data_sender.py

python hdfs_read.py

python data_preprocess.py

## vérification 

hdfs dfs -cat /data_lake/parkinson_data/GaCo04_01.csv
