./hadoop/hadoop-2.6.2/sbin/stop-all.sh
rm -rf ~/Desktop/hadoop/hadoop-2.6.2/

cp -r ~/Desktop/code/hadoop-2.6.2-src/hadoop-dist/target/hadoop-2.6.2 ~/Desktop/hadoop/
cp ~/Desktop/config/settings/* ~/Desktop/hadoop/hadoop-2.6.2/etc/hadoop/

cd hadoop/hadoop-2.6.2/
mkdir tmp
mkdir hdfs
mkdir hdfs/name
mkdir hdfs/data
bin/hdfs namenode -format
./sbin/start-all.sh
bin/hdfs dfs -mkdir /input
bin/hdfs dfs -put ~/Desktop/jin1.txt.segmented /input
bin/hdfs dfs -put ~/Desktop/jin2.txt.segmented /input
hadoop jar ~/Desktop/InvertedIndex.jar /input/. /out
cd ../..

echo All-Done
