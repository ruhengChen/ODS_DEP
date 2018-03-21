#! /bin/bash

# example: sh execute_ap.sh 20170624

export LANG=zh_CN.utf-8
date=$1

python /etl/etldata/script/python_script/execute_ap.py ${date}

[ $? -ne 0 ] && echo "execute error" && exit -1
exit 0
