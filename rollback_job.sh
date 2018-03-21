#! /bin/bash

# example: sh rollback_job.sh 20170624

export LANG=zh_CN.utf-8
date=$1

sh /etl/etldata/script/yatop_update/${date}/rollback_job.sh
[ $? -ne 0 ] && echo "rollback error" && exit -1
exit 0
