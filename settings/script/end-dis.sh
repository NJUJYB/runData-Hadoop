#!/bin/bash
cd ~
rm -rf ~/Desktop/hadoop/hadoop-2.6.2/
ssh slave1 << remotessh
rm -rf ~/Desktop/hadoop/hadoop-2.6.2/
exit
remotessh
echo ALL-CLEAN-DONE
