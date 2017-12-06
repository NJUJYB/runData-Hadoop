#!/bin/bash
cd ~
./Desktop/hadoop/hadoop-2.6.2/sbin/stop-all.sh
rm -rf ~/Desktop/hadoop/hadoop-2.6.2/
ssh slave1 << remotessh
rm -rf ~/Desktop/hadoop/hadoop-2.6.2/
exit
remotessh
