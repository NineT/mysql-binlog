#!/bin/bash

pid=`ps -ef |grep backup | grep mode | awk '{print $2}'`
echo "kill pid $pid"
kill -9 $pid

echo "结束{$pName}进程.."
sleep 1
echo "开始backup 进程"
nohup ./backup -port=8888 -mode=separated -level=debug -cfspath=/mysql_backup > bk.log 2>&1 &