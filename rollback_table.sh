#! /bin/bash

# example: sh rollback_table.sh 20170624

export LANG=zh_CN.utf-8
date=$1

python /etl/etldata/script/python_script/rollback_table.py ${date}

[ $? -ne 0 ] && echo "rollback error" && exit -1
exit 0
