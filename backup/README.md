# backup

backup project 提供实时备份数据，合并压缩binlog 等功能

## 前提
基于binlog row模式

## 使用说明　
```txt
make 
./backup -dumphost=127.0.0.1 -dumpport=3358 -dumppasswd=secret -dumpuser=root -level=debug -metadb=binlog_backup -metapasswd=secret
```

## 测试命令如下  
```$xslt
nohup ./backup -dumphost=192.168.200.151 -dumpport=3358 -dumppasswd=passwd -dumpuser=user -level=debug -metadb=test -metapasswd=metapasswd -metahost=192.168.200.152 -metaport=3358 -metadb=test -period=sec -tmppath=/export/backup -cfspath=/export/backup -compress=false > logs 2>&1 &
```

## 权限说明  
dump用户名以及密码需要有如下权限
```txt
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%' identified by '********'; FLUSH PRIVILEGES;
```

## 启动参数说明

* 参数-period   
    备份周期， 包括 day, hour, min, sec

值 | 参数说明 | 影响
:---:|:---:|---
day | 备份到天 | 恢复时无法恢复到小时、分、秒
hour | 备份到小时 | 恢复时无法恢复到分、秒
min | 备份到分钟 | 恢复时无法恢复到秒
sec | 备份到秒 | 最小备份周期单位

* 参数-cfspath  
    cfs存储路径也即数据的最终远程存储路径 

* 参数-dumphost  
    dump binlog的MySQL 实例host 域名或IP  

* 参数-dumppasswd  
    dump binlog的MySQL 密码  

* 参数-dumpport  
    dump binlog的MySQL 实例端口  
    
* 参数-dumpuser  
    dump binlog的MySQL 实例用户名  

* 参数-dumpmode  
    dump mode 选择dump模式 分成local, remote 两种模式, local模式直接读取本地binlog文件, remote 模式采用远程dump binlog方式

* 参数-metadb  
    元数据用户名

* 参数-metahost  
    元数据MySQL host

* 参数-metapasswd  
    元数据MySQL 用户名

* 参数-metaport  
    元数据MySQl 端口  

* 参数-metauser
    元数据MySQL 用户名 

* 参数-stoptime  
    dump binlog结束时间 表示dump结束

* 参数-tmppath  
    dump 的数据文件临时存储路径
    
* 参数-compress
    是否压缩 true表示压缩， false表示不进行压缩
    
* 参数-level  
    日志级别 log-level

## Attention   
注意 由于有文件之间的移动 最好临时文件是不跨device

