#!/bin/bash
cd ~

cp -r ~/Desktop/code/dda-master/hadoop-dist/target/hadoop-2.6.2 ~/Desktop/hadoop/
cp ~/Desktop/code/dda-master/settings/config/dis/* ~/Desktop/hadoop/hadoop-2.6.2/etc/hadoop/

cd ~/Desktop/hadoop/hadoop-2.6.2/
mkdir tmp
mkdir hdfs
mkdir hdfs/name
mkdir hdfs/data
bin/hdfs namenode -format
cd ~

scp -r ~/Desktop/hadoop/hadoop-2.6.2 slave1:~/Desktop/hadoop/

cd ~/Desktop/hadoop/hadoop-2.6.2/
./sbin/start-all.sh
./sbin/hadoop-daemon.sh start datanode
bin/hdfs dfs -mkdir /input
bin/hdfs dfs -put ~/Desktop/jin.txt.segmented /input
sleep 3
echo HDFS-SETTING-DONE
#bin/hdfs dfs -put ~/Desktop/jin2.txt.segmented /input

#echo "jyb" | sudo -S wondershaper eth0 80000 8000
ssh slave1 << remotessh
cd ~/Desktop/hadoop/hadoop-2.6.2/
hadoop jar ~/Desktop/InvertedIndex.jar /input/. /out1

sleep 10

hadoop jar ~/Desktop/InvertedIndex.jar /input/. /out2
exit
remotessh
cd ~
#echo "jyb" | sudo -S wondershaper clear eth0

echo All-Done
