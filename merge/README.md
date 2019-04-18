# backup

backup project 提供实时备份数据，合并压缩binlog 等功能

## 前提
基于binlog row模式

## 使用说明　
```txt
make 
./merge -dumphost=127.0.0.1 -dumpuser=root -dumppasswd=secret -dumpport=3306 -startblogfile="mysql-bin.000001", -startblogpos=4 -starttime="2018-12-31 23:59:59" -stopblogfile=mysql-bin.000001 -stopblogpos=25374 -stoptime="2019-03-13 17:14:08" -targetfile=/export/backup/out.log -tmppath=/export/backup -level=debug -compress=false
```

## 测试命令如下  
```$xslt
nohup ./merge -dumphost=127.0.0.1 -dumpuser=root -dumppasswd=secret -dumpport=3306 -startblogfile="mysql-bin.000001", -startblogpos=4 -starttime="2018-12-31 23:59:59" -stopblogfile=mysql-bin.000001 -stopblogpos=25374 -stoptime="2019-03-13 17:14:08" -targetfile=/export/backup/out.log -tmppath=/export/backup -level=debug -compress=false &
```

## 权限说明  
dump用户名以及密码需要有如下权限
```txt
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'binlake'@'%' identified by '********'; FLUSH PRIVILEGES;
```

## 启动参数说明

* 参数-dumphost  
    dump binlog的MySQL 实例host 域名或IP  

* 参数-dumppasswd  
    dump binlog的MySQL 密码  

* 参数-dumpport  
    dump binlog的MySQL 实例端口  
    
* 参数-dumpuser  
    dump binlog的MySQL 实例用户名  

* 参数-starttime  
    启动开始合并binlog 的时间戳

* 参数-startblogfile  
    启动开始合并binlog文件名

* 参数-startblogpos  
    启动开始合并binlog position

* 参数-startgtid  
    启动开始合并的gtid

* 参数-stoptime  
    结束合并binlog 的时间戳

* 参数-stopblogfile  
    结束合并binlog文件名

* 参数-stopblogpos  
    结束合并binlog position

* 参数-stopgtid  
    结束合并binlog的gtid

* 参数-tmppath  
    dump 的数据文件临时存储路径
    
* 参数-compress
    是否压缩 true表示压缩， false表示不进行压缩
    
* 参数-targetfile  
    合并生成的文件名全称包含路径
    
* 参数-level  
    日志级别 log-level

## Attention   
注意 由于有文件之间的移动 最好临时文件是不跨device

