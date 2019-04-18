#!/bin/sh

if [ $# != 3 ] ; then
    echo "no enough parameter for backup.sh script"
    exit 1
fi

####
backup_file=$1
host=$2
end=$3

echo "backup.file $backup_file host $host end $end"

DIR=`pwd`

if [ ! -f "backup" ]; then
    echo "backup binary file not exist!!!!!"
    exit 1
fi

chmod +x ./backup
