# coding: utf-8

import os
import config
import pyodbc
#import ibm_db
import datetime
import sys
import shutil
import re
import json
import traceback
import commands

software_version = 2.7
conn = pyodbc.connect('DSN=%s' %config.dwmm_dsn)
dwmm_cursur = conn.cursor()

#conn = ibm_db.connect("DATABASE={DATABASE};HOSTNAME={HOSTNAME};PORT={PORT};PROTOCOL={PROTOCOL};UID={UID};PWD={PWD};".format(DATABASE=config.DATABASE, HOSTNAME=config.HOSTNAME, PORT=config.PORT, PROTOCOL=config.PROTOCOL, UID=config.UID, PWD=config.PWD), "", "")

global execute_logFile

def validate_date(date):
    sql = "SELECT count(1) FROM ETL.JOB_LOG"
    dwmm_cursur.execute(sql)
    rows = dwmm_cursur.fetchall()

    log_date = rows[0][0]
    if log_date == 0:
        print("ETL.JOB_LOG is empty, continue")
        log_date = "00000000"
        return 0

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


def execute_ap(input_date):
    return_dict = {}
    return_dict["returnCode"] = 200
    return_dict["returnMsg"] = ""
    return_dict["typeName"]= 'execute_ap'
    APlist = os.listdir(config.apsql_path.format(date=input_date, APNAME=""))
    print(APlist)

    for AP in APlist:
        try:
            print("execute AP %s" %AP)
            execute_logFile.write("execute AP %s\n" %AP)

            shutil.copy(config.apsql_path.format(date=input_date, APNAME=AP), config.apsql_ods_path.format(APNAME=""))

        except Exception:
            return_dict["returnCode"] = 400
            exc_type, exc_value, exc_traceback = sys.exc_info()
            return_dict["returnMsg"] = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))

            with open('/etl/etldata/script/yatop_update/{date}/execute_ap.error'.format(date=input_date), 'w') as f:
                f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

            execute_logFile.write(return_dict["returnMsg"]+'\n')

            return_dict["returnMsg"] = u"执行 AP 失败"

            return return_dict

    return return_dict

def mainaaa(input_date):

    ## 删除错误日志
    if os.path.exists('/etl/etldata/script/yatop_update/{date}/execute_ap.error'.format(date=input_date)):
        os.remove('/etl/etldata/script/yatop_update/{date}/execute_ap.error'.format(date=input_date))

    return_dict = {}

    global execute_logFile
    execute_logFile = open(config.execute_ap_logFile.format(date=input_date),'w')

    print("input_date:%s" %input_date)

    # 判断是否已经执行过
    if os.path.exists(config.etl_path.format(date=input_date)+'apHandFile'):
        print(u"请勿重复执行\n请先回滚后重新执行")
        return_dict["returnCode"] = 400
        return_dict["returnMsg"] = u'请勿重复执行\n 请先回滚后重新执行'

        return json.dumps(return_dict, ensure_ascii=False)

    ## 创建握手文件
    apHandFilePath = '/etl/etldata/script/yatop_update/{date}/apHandFile'.format(date=input_date)

    with open(apHandFilePath, 'w') as f:
        f.write('waiting')

    execute_logFile.write("input_date:%s\n" %input_date)
    return_dict["typeName"] = 'execute_ap'
    ret = validate_date(input_date)
    if ret:
        return_dict["returnCode"] = 400
        return_dict["returnMsg"] = "the date is < max(date) in job_log"
        execute_logFile.write(return_dict["returnMsg"]+'\n')
        return json.dumps(return_dict)

    if not os.path.exists(config.backup_path.format(date=input_date)):
        os.makedirs(config.backup_path.format(date=input_date))

    return_dict = execute_ap(input_date)   # 执行AP
    if return_dict.get("returnCode") != 200:
        return json.dumps(return_dict)

    execute_logFile.close()

    # with open(config.etl_path.format(date=input_date)+'execute_ap.ok','w') as f:
    #     f.write('ok')
    return json.dumps({"returnCode":200, "returnMsg":u"执行成功","typeName":"execute_ap"})


if __name__ == "__main__":
    input_date = raw_input('input_date:')
    resp = mainaaa(input_date)
    print(resp)

