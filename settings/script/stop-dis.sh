#!/bin/bash
./Desktop/hadoop/hadoop-2.6.2/sbin/stop-all.sh
./Desktop/hadoop/hadoop-2.6.2/sbin/hadoop-daemon.sh stop datanode
echo ALL-STOP
