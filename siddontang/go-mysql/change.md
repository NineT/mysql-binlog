# Project Introduction


## Come from
This project is from the source of [github](!https://github.com/siddontang), 
but cannot get the right version then using the timestamp{2018年03月08日} to locate the version instead.  


## Purpose
Why using the code from github from siddontang?   
The answer is that we have to promote the operation for BinlogEvents, 
method **dump** and **decode** cannot content to us for binlog base64 decode.  


## Modifications
* add encode event  
	add [encode_event.go](./replication/encode_event.go) to encode event into bytes like FormatDescEvent, QueryEvent, RowsLogEvent etc.  

* add rows event header  
	add rows event header to locate rows event header flag, mainly for the statement end flag.  


	
