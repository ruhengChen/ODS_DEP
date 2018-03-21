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
import codecs
reload(sys)
sys.setdefaultencoding('utf-8')

software_version = 2.8
print("start...")

# conn = ibm_db.connect("DATABASE={DATABASE};HOSTNAME={HOSTNAME};PORT={PORT};PROTOCOL={PROTOCOL};UID={UID};PWD={PWD};".format(DATABASE=config.DATABASE, HOSTNAME=config.HOSTNAME, PORT=config.PORT, PROTOCOL=config.PROTOCOL, UID=config.UID, PWD=config.PWD), "", "")

global con_edw
global con_dwmm
global cursor_edw
global cursor_dwmm
global execute_logFile

def validate_date(date):
    #"""判断确认此日期大于job_log的最大批量日期"""
    sql = "SELECT count(1) FROM ETL.JOB_LOG"
    cursor_dwmm.execute(sql)
    rows = cursor_dwmm.fetchone()
    
    if rows[0] == 0:
        return 0
    
    sql = "SELECT MAX(DATA_PRD)DATA_PRD FROM ETL.JOB_LOG"
    cursor_dwmm.execute(sql)
    rows = cursor_dwmm.fetchone()
    # cursor_dw.execute(sql)
    # rows = cursor_dw.fetchone()

    log_date = rows[0]

    if str(log_date) == "0000-00-00":
        print("ETL.JOB_LOG is empty, continue")
        log_date = "00000000"
    else:
        log_date = datetime.datetime.strftime(log_date, '%Y%m%d')

    print("log_date:%s" %log_date)

    if not date > log_date:
        print("input_date is not valid, system exit...")
        return -1

    return 0


def execute(date, file_name, guid):
    sql = ""
    logSql = ""
    file = codecs.open('/etl/etldata/script/yatop_update/{date}/{file_name}'.format(date=date, file_name= file_name), 'r', encoding='gb18030')
    data = file.read()

    if file_name != "alter_table.sql":

        data = re.sub('DROP TABLE \w+\.\w+;','',data,flags=re.M)    ## 删除所有DROP TABLE 的行
        # print(data)

        #### CREATE 表 ####
        tableList = data.split('CREATE TABLE ')
        tableList = tableList[1:]
        # print(tableList[0])

        tablenmReg = re.compile(r'CREATE TABLE (\w+\.\w+)')

        for table in tableList:
            try:
                table = 'CREATE TABLE ' + table

                tablenm = re.findall(tablenmReg, table)[0]
                syscode, tablename = tablenm.split('.')
                # print(tablenm)
                sqlList = table.split(';\n')

                for sql in sqlList:
                    if sql.strip():
                        sql = sql +';\n'
                        cursor_edw.execute(sql)
                        execute_logFile.write(sql+'\n')

                logSql = "INSERT INTO DSA.AUTOMATIC_LOG(SMY_DT, OPERATION, NOW_TABNM, STATUS, PPN_TSTAMP, GUID) VALUES('{date}', 'CREATE', '{tablenm}', 'EXECUTED', CURRENT TIMESTAMP, '{guid}')".format(date=date, tablenm=tablenm, guid=guid)
                execute_logFile.write(logSql+'\n')

                cursor_dwmm.execute(logSql)

                uptTableSql = "UPDATE DSA.ORGIN_TABLE_DETAIL_T SET IS_DW_T_F = '1' WHERE CHANGE_DATE = '{date}' AND SRC_STM_ID='{syscode}' AND TAB_CODE='{tablename}'".format(date=date, syscode=syscode, tablename=tablename)

                cursor_dwmm.execute(uptTableSql)

                cursor_edw.commit()    ## 需要都没有问题后,一起提交,不然就回滚
                cursor_dwmm.commit()
                print("CREATE TABLE {tablename} success".format(tablename=tablenm))

            except Exception:
                print(sql)

                exc_type, exc_value, exc_traceback = sys.exc_info()
                with open('/etl/etldata/script/yatop_update/{date}/execute_table.error'.format(date=date), 'w') as f:
                    f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback))+'\n')
                    f.write(sql)

                print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

                return -1

    else:

        data = re.sub('^-.*\n', '', data, flags=re.M)    ## 去掉所有的注释行

        #### CREATE 表 ####
        createReg = re.compile(r'CREATE TABLE (\w+\.\w+) LIKE (\w+\.\w+);')
        createList = re.findall(createReg , data)
        print(createList)
        for table_update, table in createList:
            sql = "CREATE TABLE {table_update} LIKE {table};".format(table_update=table_update, table=table)
            print(sql)

            cursor_edw.execute(sql)

            logSql = "INSERT INTO DSA.AUTOMATIC_LOG(SMY_DT, OPERATION, NOW_TABNM, STATUS, PPN_TSTAMP, GUID) VALUES('{date}', 'CREATE', '{table_update}', 'EXECUTED', CURRENT TIMESTAMP, '{guid}')".format(
                date=date, table_update=table_update, guid=guid)
            execute_logFile.write(logSql + '\n')

            cursor_dwmm.execute(logSql)

            cursor_edw.commit()  ## 需要都没有问题后,一起提交,不然就回滚
            cursor_dwmm.commit()


        #### add column ####
        addColumnReg = re.compile(r'alter table (\w+.\.\w+) add column (\w+)\t(.*);')
        addColumnList = re.findall(addColumnReg , data)

        for tableName, columnName, columnType in addColumnList:
            try:
                syscode, table_name = tableName.split('.')

                sql = "alter table {tableName} add column {columnName}	{columnType};".format(tableName=tableName, columnName=columnName, columnType=columnType)
                execute_logFile.write(sql+'\n')
                cursor_edw.execute(sql)

                sql = "CALL SYSPROC.ADMIN_CMD('reorg table {tableName}')".format(tableName=tableName)
                execute_logFile.write(sql+'\n')
                cursor_edw.execute(sql)

                logSql = "INSERT INTO DSA.AUTOMATIC_LOG(SMY_DT, OPERATION, NOW_TABNM, NOW_COLNM, NOW_COLTP, STATUS, PPN_TSTAMP, GUID) VALUES('{date}', 'ADDCOL', '{tableName}', '{columnName}', '{columnType}', 'EXECUTED', CURRENT TIMESTAMP, '{guid}')".format(date=date, tableName=tableName, columnName=columnName, columnType=columnType, guid=guid)
                execute_logFile.write(logSql+'\n')
                cursor_dwmm.execute(logSql)

                uptTableSql = "UPDATE DSA.ORGIN_TABLE_DETAIL_T SET IS_DW_T_F = '1' WHERE CHANGE_DATE = '{date}' AND SRC_STM_ID='{syscode}' AND TAB_CODE='{tablename}'".format(date=date, syscode=syscode, tablename=table_name)

                cursor_dwmm.execute(uptTableSql)

                cursor_edw.commit()    ## 需要都没有问题后,一起提交,不然就回滚
                cursor_dwmm.commit()

                print("alter table {tableName} add column {columnName}	{columnType} success".format(tableName=tableName, columnName=columnName, columnType=columnType))

            except Exception:
                print(sql)
                print(logSql)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                with open('/etl/etldata/script/yatop_update/{date}/execute_table.error'.format(date=date), 'w') as f:
                    f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback))+'\n')
                    f.write(sql)
                print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                return -1

        #### update column ####
        uptColumnReg = re.compile(r'alter table (\w+\.\w+) alter column (\w+) set data type (.*);--old type:(.*)')
        uptColumnList = re.findall(uptColumnReg, data)

        for tableName, columnName, columnType, columnOldType in uptColumnList:
            try:
                syscode, table_name = tableName.split('.')

                sql = "alter table {tableName} alter column {columnName} set data type {columnType};".format(tableName=tableName, columnName=columnName, columnType=columnType)
                execute_logFile.write(sql+'\n')
                cursor_edw.execute(sql)

                sql = "CALL SYSPROC.ADMIN_CMD('reorg table {tableName}')".format(tableName=tableName)
                execute_logFile.write(sql+'\n')
                cursor_edw.execute(sql)

                logSql = "INSERT INTO DSA.AUTOMATIC_LOG(SMY_DT, OPERATION, NOW_TABNM, NOW_COLNM, LST_COLTP, NOW_COLTP, STATUS, PPN_TSTAMP, GUID) VALUES('{date}', 'UPTCOL', '{tableName}', '{columnName}', '{columnOldType}', '{columnType}', 'EXECUTED', CURRENT TIMESTAMP, '{guid}')".format(date=date, tableName=tableName, columnName=columnName, columnOldType=columnOldType, columnType=columnType, guid=guid)
                execute_logFile.write(logSql+'\n')
                cursor_dwmm.execute(logSql)

                uptTableSql = "UPDATE DSA.ORGIN_TABLE_DETAIL_T SET IS_DW_T_F = '1' WHERE CHANGE_DATE = '{date}' AND SRC_STM_ID='{syscode}' AND TAB_CODE='{tablename}'".format(date=date, syscode=syscode, tablename=table_name)

                cursor_dwmm.execute(uptTableSql)

                cursor_edw.commit()
                cursor_dwmm.commit()

                print("alter table {tableName} alter column {columnName} set data type {columnType} success".format(tableName=tableName, columnName=columnName, columnType=columnType))

            except Exception:
                print(sql)
                print(logSql)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                with open('/etl/etldata/script/yatop_update/{date}/execute_table.error'.format(date=date), 'w') as f:
                    f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback))+'\n')
                    f.write(sql)
                print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                return -1

        ##### rename 表 #####
        renameReg = re.compile(r'rename table (\w+\.\w+) to (\w+);')
        renameList = re.findall(renameReg, data)

        for lstTableName, nowTableName in renameList:
            try:

                syscode, tablename = lstTableName.split('.')

                sql = "rename table {lstTableName} to {nowTableName};".format(lstTableName=lstTableName,
                                                                              nowTableName=nowTableName)
                execute_logFile.write(sql + '\n')
                cursor_edw.execute(sql)

                logSql = "INSERT INTO DSA.AUTOMATIC_LOG(SMY_DT, OPERATION, LST_TABNM, NOW_TABNM, STATUS, PPN_TSTAMP, GUID) VALUES('{date}', 'RENAME', '{lstTableName}', '{nowTableName}', 'EXECUTED', CURRENT TIMESTAMP, '{guid}')".format(
                    date=date, lstTableName=lstTableName, nowTableName=nowTableName, guid=guid)
                execute_logFile.write(logSql + '\n')
                cursor_dwmm.execute(logSql)

                uptTableSql = "UPDATE DSA.ORGIN_TABLE_DETAIL_T SET IS_DW_T_F = '1' WHERE CHANGE_DATE = '{date}' AND SRC_STM_ID='{syscode}' AND TAB_CODE='{tablename}'".format(
                    date=date, syscode=syscode, tablename=tablename)

                cursor_dwmm.execute(uptTableSql)

                cursor_edw.commit()  ## 需要都没有问题后,一起提交,不然就回滚
                cursor_dwmm.commit()

                print("rename table {lstTableName} to {nowTableName} success".format(lstTableName=lstTableName,
                                                                                     nowTableName=nowTableName))

            except Exception:
                print(sql)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                with open('/etl/etldata/script/yatop_update/{date}/execute_table.error'.format(date=date),
                          'w') as f:
                    f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)) + '\n')
                    f.write(sql)

                print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                return -1

        #### DROP 表 ####
        dropReg = re.compile(r'DROP TABLE (\w+\.\w+);')  ## 找出所有要Drop的表
        dropList = re.findall(dropReg, data)
        print(dropList)

        for table in dropList:
            try:
                syscode, tablename = table.split('.')

                sql = "DROP TABLE {tablenm};".format(tablenm=table)
                execute_logFile.write(sql + '\n')

                cursor_edw.execute(sql)

                logSql = "INSERT INTO DSA.AUTOMATIC_LOG(SMY_DT, OPERATION, LST_TABNM, STATUS, PPN_TSTAMP, GUID) VALUES('{date}', 'DROP', '{tablenm}', 'EXECUTED', CURRENT TIMESTAMP, '{guid}')".format(
                    date=date, tablenm=table, guid=guid)
                cursor_dwmm.execute(logSql)
                execute_logFile.write(logSql + '\n')

                uptTableSql = "UPDATE DSA.ORGIN_TABLE_DETAIL_T SET IS_DW_T_F = '1' WHERE CHANGE_DATE = '{date}' AND SRC_STM_ID='{syscode}' AND TAB_CODE='{tablename}'".format(
                    date=date, syscode=syscode, tablename=tablename)

                cursor_dwmm.execute(uptTableSql)

                cursor_edw.commit()  ## 需要都没有问题后,一起提交,不然就回滚
                cursor_dwmm.commit()
                print("DROP TABLE {tablenm} success".format(tablenm=table))

            except Exception:
                print(sql)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                with open('/etl/etldata/script/yatop_update/{date}/execute_table.error'.format(date=date),
                          'w') as f:
                    f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)) + '\n')
                    f.write(sql)

                print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                return -1

    return 0


def main(input_date, guid):

    return_dict = {}
    return_dict["typeName"]="execute_table"

    global con_edw
    global con_dwmm
    global cursor_edw
    global cursor_dwmm
    global execute_logFile

    try:
        if __name__ != "__main__":
            if not guid:
                raise Exception("guid is empty...")
        print("guid: %s" %guid)
        con_edw = pyodbc.connect('DSN={edw_dsn}'.format(edw_dsn=config.edw_dsn))
        cursor_edw = con_edw.cursor()

        con_dwmm = pyodbc.connect('DSN={dwmm_dsn}'.format(dwmm_dsn=config.dwmm_dsn))
        cursor_dwmm = con_dwmm.cursor()

        execute_logFile = open(config.execute_table_logFile.format(date=input_date),'w')

        print("input_date:%s" %input_date)

        ## 判断是否已经执行过
        if __name__ != "__main__":
            existSql = "SELECT COUNT(1) NM FROM DSA.AUTOMATIC_LOG WHERE SMY_DT>='{date}' and GUID='{guid}'".format(date=input_date, guid=guid)
            cursor_dwmm.execute(existSql)
            rows = cursor_dwmm.fetchone()
            if rows.NM != 0:
                print(u"execute_table error!\n请勿重复执行\n请先回滚后重新执行")
                return_dict["returnCode"] = 400
                return_dict["returnMsg"] = u'请勿重复执行\n 请先回滚后重新执行'

                return json.dumps(return_dict, ensure_ascii=False)

        if os.path.exists('/etl/etldata/script/yatop_update/{date}/execute_table.error'.format(date=input_date)):
            os.remove('/etl/etldata/script/yatop_update/{date}/execute_table.error'.format(date=input_date))

        ## 创建握手文件
        tableHandFilePath = '/etl/etldata/script/yatop_update/{date}/tableHandFile'.format(date=input_date)

        with open(tableHandFilePath, 'w') as f:
            f.write('waiting')


        execute_logFile.write(input_date)

        ret = validate_date(input_date)
        if ret:
            return_dict["returnCode"] = 400
            return_dict["returnMsg"] = "the date is < max(date) in job_log"
            execute_logFile.write(return_dict["returnMsg"]+'\n')
            return json.dumps(return_dict)

        if not os.path.exists(config.backup_path.format(date=input_date)):
            os.makedirs(config.backup_path.format(date=input_date))

        execute_list = ["alter_table.sql", "delta_tables.ddl", "ods_tables.ddl", "his_tables.ddl"] #, "alter_table.sql", "ods_tables.ddl", "his_tables.ddl" ,"delta_tables.ddl"

        for file in execute_list:
            try:
                ret = execute(input_date, file, guid)
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                with open('/etl/etldata/script/yatop_update/{date}/execute_table.error'.format(date=input_date), 'w') as f:
                    f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

                return_dict["returnCode"] = 400
                return_dict["returnMsg"] = u"执行 {0} 错误!\n".format(file)
                execute_logFile.write(return_dict["returnMsg"]+'\n')
                return json.dumps(return_dict)

            if ret:
                return_dict["returnCode"] = 400
                return_dict["returnMsg"] = u"执行 {0} 错误!\n".format(file)
                execute_logFile.write(return_dict["returnMsg"]+'\n')
                return json.dumps(return_dict)

    finally:
        try:
            cursor_edw.close()
            cursor_dwmm.close()
            execute_logFile.close()
            con_edw.close()
            con_dwmm.close()
        except Exception:
            pass

    # with open(config.etl_path.format(date=input_date)+'execute_table.ok','w') as f:
    #     f.write('ok')

    return json.dumps({"returnCode":200, "returnMsg":u"执行成功"})


if __name__ == "__main__":
    print("start...")
    guid = ""
    input_date = sys.argv[1].strip()
    # import uuid
    # guid = str(uuid.uuid1())
    # input_date = raw_input('input_date:')
    # guid = raw_input('guid:')
    print("input_date:", input_date)
    resp = main(input_date, guid)
    print(resp)

    if eval(resp).get("returnCode") != 200:
        print "error:", eval(resp).get("returnMsg")
        exit(-1)

