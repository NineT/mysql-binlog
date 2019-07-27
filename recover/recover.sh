#!/bin/bash

pid=`ps -ef |grep recover | grep dbreg | awk '{print $2}'`
echo "kill pid $pid"
kill -9 $pid

echo "结束{$pName}进程.."
sleep 1
echo "开始recover 进程"
nohup ./recover -base=/mysql_backup -clusterid=1000 -user=root -password=secret -dbreg=* -tbreg=* -time="2099-12-31 23:59:59" -level=debug > recover.log 2>&1 &
