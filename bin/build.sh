#!/bin/bash

EMAIL="aaron@archive.org"

if [ -z "${1}" ]; then
    echo "$0 <basedir>"
    exit 1;
fi

c="${1}"
tstamp=`date "+%Y%m%d%H%M%S"`
logdir="log/${c}"
logfile="${logdir}/${tstamp}.txt"

mkdir -p "${logdir}"

export JAVA_HOME=/usr
export HADOOP_BIN=/home/webcrawl/hadoop/bin/hadoop

{
    /home/webcrawl/pig/bin/pig -l /dev/null -v ./pig/build.py ${c} 
    
    retval=$?
    
} &> ${logfile}

if [ $retval -ne 0 ]; then
    cat ${logfile} | mail -s "Build failed: ${c}" ${EMAIL}
fi

exit 0
