# -*- coding: utf-8 -*-
import os
import config
import sys
import pyodbc
import re
import traceback
import codecs
import json
import chardet
reload(sys)
sys.setdefaultencoding('utf-8')

software_version = 2.7

class rollBackObj(object):
    def __init__(self):
        self.edwCon = pyodbc.connect('DSN={edwdsn};charset=gb2312'.format(edwdsn=config.edw_dsn))
        self.dwmmCon = pyodbc.connect('DSN={dwmm_dsn};charset=gb2312'.format(dwmm_dsn=config.dwmm_dsn))
        self.edwCursor = self.edwCon.cursor()
        self.dwmmCursor = self.dwmmCon.cursor()

    def rollBackCreate(self, conditions):
        conditions["OPERATION"] = "CREATE"
        sql = "SELECT NOW_TABNM, SMY_DT FROM DSA.AUTOMATIC_LOG WHERE {conditions} AND ROLLBK_STATUS IS NULL".format(conditions=" AND ".join(["%s=\'%s\'" %(key, value) for key, value in conditions.items()]))
        print(sql)
        self.dwmmCursor.execute(sql)
        rows = self.dwmmCursor.fetchall()

        for row in rows:
            try:
                syscode, table_name = row.NOW_TABNM.split('.')

                print("rollBackCreate %s" %row.NOW_TABNM)
                sql = "DROP TABLE {tablename}".format(tablename=row.NOW_TABNM)
                print(sql)

                self.edwCursor.execute(sql)

                tmp_conditions = conditions.copy()
                tmp_conditions["NOW_TABNM"] = row.NOW_TABNM

                logSql = "UPDATE DSA.AUTOMATIC_LOG SET ROLLBK_STATUS='ROLLBACKED' WHERE {conditions}".format(conditions=" AND ".join(["%s=\'%s\'" %(key, value) for key, value in tmp_conditions.items()]))
                print(logSql)

                self.dwmmCursor.execute(logSql)

                uptTableSql = "UPDATE DSA.ORGIN_TABLE_DETAIL_T SET IS_RL_TBL_F = '0' WHERE CHANGE_DATE = '{date}' AND SRC_STM_ID='{syscode}' AND TAB_CODE='{tablename}'".format(date=row.SMY_DT, syscode=syscode, tablename=table_name)
                print(uptTableSql)

                self.dwmmCursor.execute(uptTableSql)

                self.edwCursor.commit()    ## 需要都没有问题后,一起提交,不然就回滚
                self.dwmmCursor.commit()
                print("rollBackCreate {tablename} success".format(tablename=row.NOW_TABNM))

            except Exception:
                    print(sql)
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    with open('/etl/etldata/script/yatop_update/{date}/rollback_table.error'.format(date=conditions.get('SMY_DT')), 'w') as f:
                        f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                    print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                    return -1
        return 0

    def rollBackDrop(self, conditions):
        conditions["OPERATION"] = "DROP"
        sql = "SELECT LST_TABNM FROM DSA.AUTOMATIC_LOG WHERE {conditions} AND ROLLBK_STATUS IS NULL".format(conditions=" AND ".join(["%s=\'%s\'" %(key, value) for key, value in conditions.items()]))
        print(sql)
        self.dwmmCursor.execute(sql)
        rows = self.dwmmCursor.fetchall()
        for row in rows:
            print("rollBackDrop %s" %row.LST_TABNM)
            f = codecs.open(config.backup_path.format(date=conditions.get('SMY_DT')) + row.LST_TABNM + '.ddl.bak', 'r')

            data = f.read()

            f.close()
            data = re.sub('^-.*\n', '', data, flags=re.M)    ## 去掉所有的注释行
            data = data.decode('gb18030').encode('utf-8')

            #### CREATE 表 ####
            tableList = data.split('CREATE TABLE ')
            tableList = tableList[1:]
            # print(tableList[0])

            for table in tableList:
                try:
                    table = 'CREATE TABLE ' + table

                    table = table.replace("COMMIT WORK;", "")  ## 去除COMMIT WORK;
                    table = table.replace("CONNECT RESET;", "")  ## 去除CONNECT RESET;
                    table = table.replace("TERMINATE;", "")  ## 去除TERMINATE;

                    sqlList = re.split(';\s+\n', table)

                    for sql in sqlList:
                        if sql.strip():
                            sql = sql +';\n'

                            print(sql)
                            self.edwCursor.execute(sql)

                    tmp_conditions = conditions.copy()
                    tmp_conditions["LST_TABNM"] = row.LST_TABNM

                    logSql = "UPDATE DSA.AUTOMATIC_LOG SET ROLLBK_STATUS='ROLLBACKED' WHERE {conditions}".format(conditions=" AND ".join(["%s=\'%s\'" %(key, value) for key, value in tmp_conditions.items()]))
                    print(logSql)

                    self.dwmmCursor.execute(logSql)

                    self.edwCursor.commit()    ## 需要都没有问题后,一起提交,不然就回滚
                    self.dwmmCursor.commit()
                    print("rollBackDrop {tablename} success".format(tablename=row.LST_TABNM))

                except Exception:
                    print(sql)
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    with open('/etl/etldata/script/yatop_update/{date}/rollback_table.error'.format(date=conditions.get('SMY_DT')), 'w') as f:
                        f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                    print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                    return -1

        return 0

    def rollBackRename(self, conditions):
        conditions["OPERATION"] = "RENAME"
        sql = "SELECT LST_TABNM, NOW_TABNM FROM DSA.AUTOMATIC_LOG WHERE {conditions} AND ROLLBK_STATUS IS NULL".format(conditions=" AND ".join(["%s=\'%s\'" %(key, value) for key, value in conditions.items()]))

        self.dwmmCursor.execute(sql)
        rows = self.dwmmCursor.fetchall()
        for row in rows:
            try:
                print("rollBackRename %s" %row.NOW_TABNM)
                syscode = row.LST_TABNM.split('.')[0]
                sql = "RENAME TABLE {syscode}.{srcname} to {desname}".format(syscode=syscode, srcname=row.NOW_TABNM, desname=row.LST_TABNM.split('.')[1])
                print(sql)
                self.edwCursor.execute(sql)

                tmp_conditions = conditions.copy()
                tmp_conditions["LST_TABNM"] = row.LST_TABNM
                logSql = "UPDATE DSA.AUTOMATIC_LOG SET ROLLBK_STATUS='ROLLBACKED' WHERE {conditions}".format(conditions=" AND ".join(["%s=\'%s\'" %(key, value) for key, value in tmp_conditions.items()]))
                print(logSql)

                self.dwmmCursor.execute(logSql)

                self.edwCursor.commit()    ## 需要都没有问题后,一起提交,不然就回滚
                self.dwmmCursor.commit()
                print("rollBackRename {tablename} success".format(tablename=row.NOW_TABNM))

            except Exception:
                print(sql)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                with open('/etl/etldata/script/yatop_update/{date}/rollback_table.error'.format(date=conditions.get('SMY_DT')), 'w') as f:
                    f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                return -1
        return 0

    def rollBackAddCol(self, conditions):
        conditions["OPERATION"] = "ADDCOL"
        sql = "SELECT NOW_TABNM, NOW_COLNM FROM DSA.AUTOMATIC_LOG WHERE {conditions} AND ROLLBK_STATUS IS NULL".format(conditions=" AND ".join(["%s=\'%s\'" %(key, value) for key, value in conditions.items()]))
        print(sql)

        self.dwmmCursor.execute(sql)
        rows = self.dwmmCursor.fetchall()
        for row in rows:
            try:
                print("rollBackAddCol %s.%s" %(row.NOW_TABNM, row.NOW_COLNM))
                sql = "ALTER TABLE {tablename} DROP COLUMN {columnname}".format(tablename=row.NOW_TABNM, columnname=row.NOW_COLNM)
                print(sql)

                self.edwCursor.execute(sql)

                sql = "CALL SYSPROC.ADMIN_CMD('reorg table {tablename}')".format(tablename=row.NOW_TABNM)
                print(sql)

                self.edwCursor.execute(sql)

                tmp_conditions = conditions.copy()
                tmp_conditions["NOW_TABNM"] = row.NOW_TABNM
                tmp_conditions["NOW_COLNM"] = row.NOW_COLNM

                logSql = "UPDATE DSA.AUTOMATIC_LOG SET ROLLBK_STATUS='ROLLBACKED' WHERE {conditions}".format(conditions=" AND ".join(["%s=\'%s\'" %(key, value) for key, value in tmp_conditions.items()]))
                print(logSql)

                self.dwmmCursor.execute(logSql)

                self.edwCursor.commit()    ## 需要都没有问题后,一起提交,不然就回滚
                self.dwmmCursor.commit()
                print("rollBackAddCol {tablename}.{columnname} success".format(tablename=row.NOW_TABNM, columnname=row.NOW_COLNM))

            except Exception:
                print(sql)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                with open('/etl/etldata/script/yatop_update/{date}/rollback_table.error'.format(date=conditions.get('SMY_DT')), 'w') as f:
                    f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                return -1
        return 0


    def rollBackUptCol(self, conditions):
        conditions["OPERATION"] = "UPTCOL"
        sql = "SELECT NOW_TABNM, NOW_COLNM, LST_COLTP, NOW_COLTP FROM DSA.AUTOMATIC_LOG WHERE {conditions} AND ROLLBK_STATUS IS NULL".format(conditions=" AND ".join(["%s=\'%s\'" %(key, value) for key, value in conditions.items()]))
        print(sql)

        self.dwmmCursor.execute(sql)
        rows = self.dwmmCursor.fetchall()
        for row in rows:
            try:
                print("rollBackUptCol %s.%s" %(row.NOW_TABNM, row.NOW_COLNM))
                sql = "alter table {tablename} alter column {columnname} set data type {lstcoltp};".format(tablename=row.NOW_TABNM, columnname=row.NOW_COLNM, lstcoltp=row.LST_COLTP)
                print(sql)
                self.edwCursor.execute(sql)

                sql = "CALL SYSPROC.ADMIN_CMD('reorg table {tablename}')".format(tablename=row.NOW_TABNM)
                print(sql)
                self.edwCursor.execute(sql)

                tmp_conditions = conditions.copy()
                tmp_conditions["NOW_TABNM"] = row.NOW_TABNM
                tmp_conditions["NOW_COLNM"] = row.NOW_COLNM

                logSql = "UPDATE DSA.AUTOMATIC_LOG SET ROLLBK_STATUS='ROLLBACKED' WHERE {conditions}".format(conditions=" AND ".join(["%s=\'%s\'" %(key, value) for key, value in tmp_conditions.items()]))

                self.dwmmCursor.execute(logSql)

                self.edwCursor.commit()    ## 需要都没有问题后,一起提交,不然就回滚
                self.dwmmCursor.commit()
                print("rollBackUptCol {tablename}.{columnname} success".format(tablename=row.NOW_TABNM, columnname=row.NOW_COLNM))

            except Exception:
                print(sql)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                with open('/etl/etldata/script/yatop_update/{date}/rollback_table.error'.format(date=conditions.get('SMY_DT')), 'w') as f:
                    f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
                return -1
        return 0


def main(input_dict):
    rollBackTab = rollBackObj()

    return_dict = {}
    return_dict["typeName"]="rollback_table"
    return_dict["returnCode"] = 200
    return_dict["returnMsg"] = u"回滚成功"

    ## 删除错误日志
    if os.path.exists('/etl/etldata/script/yatop_update/{date}/rollback_table.error'.format(date=input_dict.get('SMY_DT'))):
        os.remove('/etl/etldata/script/yatop_update/{date}/rollback_table.error'.format(date=input_dict.get('SMY_DT')))

    ret = rollBackTab.rollBackCreate(input_dict)
    if ret:
        return_dict["returnCode"] = 400
        return_dict["returnMsg"] = u"回滚创建表错误"
        return json.dumps(return_dict, ensure_ascii=False)

    ret = rollBackTab.rollBackDrop(input_dict)
    if ret:
        return_dict["returnCode"] = 400
        return_dict["returnMsg"] = u"回滚删除表错误"
        return json.dumps(return_dict, ensure_ascii=False)

    ret = rollBackTab.rollBackRename(input_dict)
    if ret:
        return_dict["returnCode"] = 400
        return_dict["returnMsg"] = u"回滚更名表错误"
        return json.dumps(return_dict, ensure_ascii=False)

    ret = rollBackTab.rollBackAddCol(input_dict)
    if ret:
        return_dict["returnCode"] = 400
        return_dict["returnMsg"] = u"回滚新增字段错误"
        return json.dumps(return_dict, ensure_ascii=False)

    ret = rollBackTab.rollBackUptCol(input_dict)
    if ret:
        return_dict["returnCode"] = 400
        return_dict["returnMsg"] = u"回滚更新字段错误"
        return json.dumps(return_dict, ensure_ascii=False)

    return json.dumps(return_dict, ensure_ascii=False)

if __name__ == "__main__":
    input_dict = {"SMY_DT":"20170426", "guid":"123456"}

    main(input_dict)
