# recover

recover data using binlog file on cfs storage   

## Introduction 

* dump file from jss/hdfs/cfs  
    dump file using [down](./res/resume.go) located on line: 513 
    
* recover in order  
    using go channel send file on sorted order and parse and assemble base64 code into statement then execute 

* save the right offset in case of the kill  
    save offset on [conf](./conf) directory

## QuickStart  

* run    
```tx
make 
nohup ./recover -cfspath=/export/backup/store/127.0.0.1 -dbreg="sysbench" -tbreg="sbtest1" -tmppath=/export/tmp/ -user=user -password=password -host=10.191.251.189 -port=3358  -compress=false -stoptime="2019-04-09 11:00:01" -level=debug > logs 2>&1 &
```

## Args Intruction  

