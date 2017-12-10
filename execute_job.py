# coding: utf-8

import os
import config
import pyodbc
# import ibm_db
import datetime
import sys
import shutil
import re
import json
import traceback
import commands


# conn = ibm_db.connect("DATABASE={DATABASE};HOSTNAME={HOSTNAME};PORT={PORT};PROTOCOL={PROTOCOL};UID={UID};PWD={PWD};".format(DATABASE=config.DATABASE, HOSTNAME=config.HOSTNAME, PORT=config.PORT, PROTOCOL=config.PROTOCOL, UID=config.UID, PWD=config.PWD), "", "")

software_version = 2.7
conn = pyodbc.connect('DSN=%s' %config.dwmm_dsn)
dwmm_cursur = conn.cursor()

global execute_logFile

def validate_date(date):
    #"""判断确认此日期大于job_log的最大批量日期"""
    sql = "SELECT count(1) FROM ETL.JOB_LOG"
    dwmm_cursur.execute(sql)
    rows = dwmm_cursur.fetchall()

    log_date = rows[0][0]
    if log_date == 0:
        print("ETL.JOB_LOG is empty, continue")
        log_date = "00000000"
        return 0
    # cursor_dw.execute(sql)
    # rows = cursor_dw.fetchone()

    sql = "SELECT MAX(DATA_PRD) DATA_PRD FROM ETL.JOB_LOG"
    dwmm_cursur.execute(sql)
    rows = dwmm_cursur.fetchall()
    # stmt = ibm_db.exec_immediate(conn, sql);
    # rows = ibm_db.fetch_tuple(stmt)
    # cursor_dw.execute(sql)
    # rows = cursor_dw.fetchone()

    log_date = rows[0][0]

    log_date = datetime.datetime.strftime(log_date, '%Y%m%d')

    print("log_date:%s" %log_date)

    if not date > log_date:
        print("input_date is not valid, system exit...")
        return -1

    return 0


def execute(date, file_name):
    print("execute %s" %file_name)
    execute_logFile.write("execute %s" %file_name)
    cmd = "db2 -stvf /etl/etldata/script/yatop_update/"+date+"/"+file_name
    print(cmd)
    execute_logFile.write(cmd)

    status, output = commands.getstatusoutput(cmd)

    print(status)

    if not (status == 0 or status == 256):
        execute_logFile.write(str(status)+'\n')

        execute_logFile.write("execute %s error, cat /etl/etldata/script/yatop_update/%s/%s.error to see detail\n" % (file_name, date, file_name))
        print("execute %s error, cat /etl/etldata/script/yatop_update/%s/%s.error to see detail" % (file_name, date, file_name))

        with open("/etl/etldata/script/yatop_update/"+date+'execute_job.error', 'w') as f:
            f.write(output)
            execute_logFile.write(output)
        return -1, output
    return 0, output


def main(input_date):

    return_dict = {}

    global execute_logFile
    execute_logFile = open(config.execute_job_logFile.format(date=input_date),'w')

    print("input_date:%s" %input_date)

    ## 判断是否已经执行过
    if os.path.exists(config.etl_path.format(date=input_date)+'jobHandFile'):
        print(u"job_schedule error! 请勿重复执行\n请先回滚后重新执行")
        return_dict["returnCode"] = 400
        return_dict["returnMsg"] = u'请勿重复执行\n 请先回滚后重新执行'

        return json.dumps(return_dict, ensure_ascii=False)

    # 创建握手文件
    jobHandFilePath = '/etl/etldata/script/yatop_update/{date}/jobHandFile'.format(date=input_date)
    with open(jobHandFilePath, 'w') as f:
        f.write('waiting')

    execute_logFile.write(input_date)

    ret = validate_date(input_date)
    return_dict["typeName"]='execute_job'
    if ret:
        return_dict["returnCode"] = 400
        return_dict["returnMsg"] = "the date is < max(date) in job_log"
        execute_logFile.write(return_dict["returnMsg"]+'\n')
        return json.dumps(return_dict)

    if not os.path.exists(config.backup_path.format(date=input_date)):
        os.makedirs(config.backup_path.format(date=input_date))

    execute_list = ["job_schedule.SQL"]

    for file in execute_list:
        ret, output = execute(input_date, file)
        if ret:
            return_dict["returnCode"] = 400
            return_dict["returnMsg"] = "执行 {0} 失败!\n".format(file)
            execute_logFile.write(return_dict["returnMsg"]+'\n')
            with open('/etl/etldata/script/yatop_update/{date}/execute_job.error'.format(date=input_date), 'w') as f:
                f.write(output)
            return json.dumps(return_dict)

    execute_logFile.close()
    # with open(config.etl_path.format(date=input_date)+'execute_job.ok','w') as f:
    #     f.write('ok')
    return json.dumps({"returnCode":200, "returnMsg":u"执行成功"})


if __name__ == "__main__":
    input_date = raw_input('input_date:')
    resp = main(input_date)
    print(resp)

