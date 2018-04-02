# -*- coding=utf-8 -*-

"""
重新改写compare
"""

import glob
# import ibm_db
import pyodbc
import re
import sys
import logging
import os
import codecs
import time
import config
import execute_ap
import execute_job
import execute_table
import rollback_table
import shutil
from collections import Counter
import stat
import traceback
import json
import subprocess
from future.utils import raise_with_traceback


if config.VERSION == 2:
    reload(sys)
    sys.setdefaultencoding('utf-8')
    import commands


def get_return_data(func):
    """
    打包返回
    """
    def wrapper(self, *arg, **kwargs):
        return_dict = {}
        return_dict["returnCode"] = 200
        ret, returnMsg = func(self, *arg, **kwargs)

        if ret:
            return_dict["returnCode"] = 400
        return_dict["returnMsg"] = returnMsg
        return_dict["typeName"]="compare_generate"

        print "return_dict_wrapper: ",return_dict

        return return_dict

    return wrapper

class CompareObj(object):
    def __init__(self):
        # self.conn = ibm_db.connect("DATABASE={DATABASE};HOSTNAME={HOSTNAME};PORT={PORT};PROTOCOL={PROTOCOL};UID={UID};PWD={PWD};".format(DATABASE=config.DATABASE, HOSTNAME=config.HOSTNAME, PORT=config.PORT, PROTOCOL=config.PROTOCOL, UID=config.UID, PWD=config.PWD), "", "")
        print("DSN :%s" %config.dwmm_dsn)
        self.conn = pyodbc.connect('DSN=%s' %config.dwmm_dsn)
        self.dwmm_cursur = self.conn.cursor()

        self.edw_conn = pyodbc.connect('DSN=%s' %config.edw_dsn)
        self.edw_cursor = self.edw_conn.cursor()

        self.influencedJoblist = []

    def _none_to_string(self, data):
        return data if data else ''

    def _getResultList(self, sql):

        self.dwmm_cursur.execute(sql)
        rows = self.dwmm_cursur.fetchall()
        # stmt = ibm_db.exec_immediate(self.conn, sql)
        # tuple = ibm_db.fetch_tuple(stmt)
        # rows = []
        # while tuple:
        #     rows.append(tuple)
        #     tuple = ibm_db.fetch_tuple(stmt)
        return rows

    def generate_rollback_ap(self, input_date):
        """生成回滚ap rollback的shell脚本,以及备份AP"""

        self._muti_outStream("generate rollback ap shell...\n")

        return_dict = {}
        return_dict["returnCode"] = 200
        return_dict["returnMsg"] = ""
        APlist = os.listdir(config.apsql_path.format(date=input_date, APNAME=""))

        ##判断/etl/etldata/script/odssql/路径是否存在
        if not os.path.exists(config.apsql_ods_path.format(APNAME="")):
            try:
                os.makedirs(config.apsql_ods_path.format(APNAME=""))
            except Exception:
                return_dict["returnCode"] = 400
                exc_type, exc_value, exc_traceback = sys.exc_info()
                return_dict["returnMsg"] = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
                return return_dict

        roll_back_ap_file = open(config.rollback_ap_path.format(date=input_date), 'w')

        roll_back_ap_file.write('cd /etl/etldata/script/yatop_update/{date}\n\n'.format(date=input_date))
        roll_back_ap_file.write('[ -f rollback_ap.error ] && rm rollback_ap.error \n')

        roll_back_ap_file.write('[ ! -f apHandFile ] && echo "无需回滚" && exit -1 \n\n')
        roll_back_ap_file.write('rm -f apHandFile >> rollback_ap.log 2>>rollback_ap.error \n')
        roll_back_ap_file.write("[ $? -ne 0 ] && exit -1\n")

        try:
            ## 回滚操作
            cmd = 'rm -rf {ods_path} >> rollback_ap.log 2>>rollback_ap.error \n'.format(ods_path=config.apsql_ods_path.format(APNAME=''))
            roll_back_ap_file.write(cmd)
            roll_back_ap_file.write("[ $? -ne 0 ] && exit -1\n")

            cmd = "cp -rp {backup_path} {ods_path} >> rollback_ap.log 2>>rollback_ap.error \n".format(backup_path=config.backup_path.format(date=input_date)+'AP', ods_path=config.apsql_ods_path.format(APNAME=''))   # 将原来备份的还原
            roll_back_ap_file.write(cmd)
            roll_back_ap_file.write("[ $? -ne 0 ] && exit -1\n")

            roll_back_ap_file.write('[ "`cat rollback_ap.error`" == "" ] && rm rollback_ap.error\n\n')


            ## 备份操作
            if not os.path.exists(config.apsql_ods_path.format(APNAME="")):   # 若原ods目录下没有对应的AP
                pass
            else:
                if os.path.exists(config.backup_path.format(date=input_date)+'AP'):
                    print('AP backup exists...')
                else:
                    ## 复制整个odssql 目录到 backup目录
                    shutil.copytree(config.apsql_ods_path.format(APNAME=''), config.backup_path.format(date=input_date)+'AP')

        except Exception:
            # return_dict["returnCode"] = 400
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # return_dict["returnMsg"] = u"备份AP或生成AP回滚程序错误: ".encode('utf-8') + repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
            self._muti_outStream(u"备份AP或生成AP回滚程序错误: ".encode('utf-8') + repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            self.read_me_file.write(u"备份AP或生成AP回滚程序错误: ".encode('utf-8') + repr(traceback.format_exception(exc_type, exc_value, exc_traceback)) + '\n')
            return -1

        roll_back_ap_file.write("echo \"rollback ap ok!\"\n")
        roll_back_ap_file.close()

        os.chmod(config.rollback_ap_path.format(date=input_date), stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)  ## 具有 777 权限

        return 0

    def generate_rollback_table(self, input_date):
        """备份所需要的表"""

        self._muti_outStream("generate_rollback_table...\n")

        return_dict = {}
        return_dict["returnCode"] = 200
        return_dict["returnMsg"] = ""
        with open(config.alter_table_path.format(date=input_date), 'r') as f:
            data = f.read()

        reg = re.compile(r"alter table (\w+\.\w+)", re.IGNORECASE)
        alter_table_list = re.findall(reg, data)

        alter_table_add_column_dict = {}
        alter_table_add_column_list = []

        alter_table_update_dict = {}

        reg = re.compile(r"alter table (\w+\.\w+) add column (\w+)", re.IGNORECASE)
        result = re.findall(reg , data)
        for tablename, column in result:
            alter_table_add_column_dict[tablename] = column

        for tablename in alter_table_add_column_dict.keys():
            alter_table_add_column_list.append(tablename)

        self._muti_outStream("alter_table_add_column_dict %s\n" % repr(alter_table_add_column_dict))

        reg = re.compile(r'alter table (\w+.\w+) alter column (\w+)', re.IGNORECASE)
        result = re.findall(reg, data)

        for tab_num, column in result:
            alter_table_update_dict[tab_num] = column


        reg = re.compile(r"rename table (\w+\.\w+)", re.IGNORECASE)
        rename_table_list = re.findall(reg, data)

        reg = re.compile(r"DROP TABLE (\w+\.\w+)", re.IGNORECASE)
        drop_table_list = re.findall(reg, data)

        with open(config.delta_tables_path.format(date=input_date), 'r') as f:
            data = f.read()

        backup_table_list = list(set(alter_table_add_column_list + alter_table_list + rename_table_list + drop_table_list))
        print("backup_table_list:", backup_table_list)
        self._muti_outStream("backup_table_list: %s\n" % ','.join(backup_table_list))

        ret = self.backup_tables(input_date, backup_table_list)
        if ret:
            return -1
        return 0

    def backup_tables(self, input_date, backup_table_list):
        """
        备份需要的表结构:包括
        ALTER TABLE 中需要 ALTER 和 RENAME 和 DROP 的表,若已备份则不备份
        """
        ret = 0

        reg = re.compile(r'CREATE TABLE')
        for table in backup_table_list:
            if len(table.split(".")) != 2:
                raise Exception("tablename error! %s" % table)
            schema, tablename = table.split('.')
            backup_path = config.backup_path.format(date=input_date)+table+".ddl.bak"

            if os.path.exists(backup_path):
                self._muti_outStream("backup exists %s\n" %table)
            else:
                cmd = "db2look -d {edwdb} -z {schema} -e -t {tablename} -nofed -o /etl/etldata/script/yatop_update/{date}/backup/{table}.ddl.bak -i {edwuser} -w {edwpwd}".format(edwdb=config.edwdb, schema=schema,tablename=tablename,date=input_date,table=table, edwuser=config.edwuser, edwpwd=config.edwpwd)
                print(cmd)

                self._muti_outStream("backup %s\n" %table)
                commands.getstatusoutput(cmd)
                read_log_cmd = 'cat /etl/etldata/script/yatop_update/{date}/backup/{table}.ddl.bak'.format(date=input_date, table=table)

                status, output = commands.getstatusoutput(read_log_cmd)

                status = re.findall(reg, output)

                if not status:
                    print("create ddl error %s" %table)
                    self._muti_outStream("create ddl error %s\n" %table)

                    self._muti_outStream(output + '\n')
                    self.read_me_file.write(u"备份表结构错误%s:%s\n" %(table, output))
                    ret = -1
                    break
        return ret

    def generate_rollback_job(self, input_date):
        """
        生成回滚job rollback的shell脚本
        """
        ############生成回滚脚本########################
        roll_back_job_file = open(config.rollback_job_path.format(date=input_date), 'w')

        roll_back_job_file.write('cd /etl/etldata/script/yatop_update/{date}\n\n'.format(date=input_date))
        roll_back_job_file.write('[ -f rollback_job.error ] && rm rollback_job.error\n')

        roll_back_job_file.write('[ ! -f jobHandFile ] && echo "无需回滚" && exit -1 \n\n')

        roll_back_job_file.write("rm -f jobHandFile >> rollback_job.log 2>>rollback_job.error \n")
        roll_back_job_file.write("[ $? -ne 0 ] && echo \"删除标识文件失败\" &&exit -1\n")


        cmd = 'db2 connect to {dwmmdb} user {dwmmuser} using {dwmmpwd} && db2 "load from /etl/etldata/script/yatop_update/{date}/backup/JOB_METADATA.del of del modified by identityoverride replace into ETL.JOB_METADATA"  >> rollback_job.log 2>>rollback_job.error \n'.format(dwmmdb=config.dwmmdb, dwmmuser=config.dwmmuser, dwmmpwd=config.dwmmpwd, date=input_date)

        roll_back_job_file.write(cmd)
        roll_back_job_file.write("[ $? -ne 0 ] && exit -1\n")

        cmd = 'db2 connect to {dwmmdb} user {dwmmuser} using {dwmmpwd} && db2 "load from /etl/etldata/script/yatop_update/{date}/backup/JOB_SEQ.del of del replace into ETL.JOB_SEQ"  >> rollback_job.log 2>>rollback_job.error \n'.format(dwmmdb=config.dwmmdb, dwmmuser=config.dwmmuser, dwmmpwd=config.dwmmpwd, date=input_date)

        roll_back_job_file.write(cmd)
        roll_back_job_file.write("[ $? -ne 0 ] && exit -1\n")

        roll_back_job_file.write("echo \"rollback job ok!\"\n")

        roll_back_job_file.write('[ "`cat rollback_job.error`" == "" ] && rm rollback_job.error\n\n')
        roll_back_job_file.close()

        os.chmod(config.rollback_job_path.format(date=input_date), stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)  ## 具有 777 权限

        ################################################


        for table in ["JOB_METADATA", "JOB_SEQ"]:
            if table == "JOB_METADATA":
                path = config.job_metadata_path
            elif table == "JOB_SEQ":
                path = config.job_seq_path

            if os.path.exists(path.format(date=input_date)):
                print(path.format(date=input_date))
                print("backup exists %s" %table)
            else:
                print("export %s..." %table)
                cmd = 'db2 connect to {dwmmdb} user {dwmmuser} using {dwmmpwd} && db2 "export to /etl/etldata/script/yatop_update/{date}/backup/{table}.del of del select * from ETL.{table}"'.format(dwmmdb=config.dwmmdb, dwmmuser=config.dwmmuser, dwmmpwd=config.dwmmpwd, date=input_date, table=table)

                print(cmd)

                status, output = commands.getstatusoutput(cmd)

                if status:
                    print("export %s error" % table)
                    self._muti_outStream(cmd)
                    self._muti_outStream("export %s error" % table)
                    print(output)

                    self.read_me_file.write(u"备份%s 错误:%s\n" %(table, output))
                    return -1
        return 0

    def _getInfluencedJob(self, table):
        """
        查找受影响的作业
        """
        tablestr = table.replace('.', '_')

        sql = "SELECT DISTINCT JOB_NM FROM ETL.JOB_SEQ WHERE PRE_JOB IN ('LD_ODS_{tablestr}','AP_ODS_{tablestr}') AND JOB_NM NOT IN ('AP_ODS_{tablestr}','ODS_DONE','AP_ODS_{tablestr}_YATOPUPDATE')".format(tablestr=tablestr)

        rows = self._getResultList(sql)

        for row in rows:
            self.influencedJoblist.append(row[0])

    def _init_log_file(self, input_date):
        """
        初始化所有的日志文件
        """

        if os.path.isdir(os.path.dirname(config.delta_tables_path.format(date=input_date))):
            shutil.move(os.path.dirname(config.delta_tables_path.format(date=input_date)),
            os.path.dirname(config.delta_tables_path.format(date=input_date)) + '_' + str(int(time.time())))

        os.makedirs(os.path.dirname(config.delta_tables_path.format(date=input_date)))
        os.makedirs(config.apsql_path.format(date=input_date, APNAME=''))

        self.readMeAFile = codecs.open(config.read_me_a_path.format(date=input_date), 'w', encoding='utf-8')
        self.compare_log_file = codecs.open(config.compare_generate_path.format(date=input_date), 'w', encoding='utf-8')
        self.delta_ddl_file = codecs.open(config.delta_tables_path.format(date=input_date), 'w', encoding=config.delta_log_encoding)
        self.ods_ddl_file = codecs.open(config.ods_tables_path.format(date=input_date), 'w', encoding=config.all_log_encoding)
        self.his_ddl_file = codecs.open(config.his_tables_path.format(date=input_date), 'w', encoding=config.his_log_encoding)
        self.read_me_file = codecs.open(config.read_me_path.format(date=input_date), 'w', encoding='utf-8')
        self.job_schedule_file = codecs.open(config.job_schedule_path.format(date=input_date), 'w', encoding='utf-8')
        self.alter_table_file = codecs.open(config.alter_table_path.format(date=input_date), 'w', encoding=config.alter_table_encoding)

        self.alter_table_file.write("connect to {0} user {1} using {2};\n".format(config.edwdb, config.edwuser, config.edwpwd))
        self.delta_ddl_file.write("connect to {0} user {1} using {2};\n".format(config.edwdb, config.edwuser, config.edwpwd))
        self.ods_ddl_file.write("connect to {0} user {1} using {2};\n".format(config.edwdb, config.edwuser, config.edwpwd))
        self.his_ddl_file.write("connect to {0} user {1} using {2};\n".format(config.edwdb, config.edwuser, config.edwpwd))
        self.job_schedule_file.write("connect to {0} user {1} using {2};\n".format(config.dwmmdb, config.dwmmuser, config.dwmmpwd))

    def _muti_outStream(self, msg):
        msg = time.strftime('%Y-%m-%d %T', time.localtime()) + msg
        sys.stdout.write(msg)
        self.compare_log_file.write(msg)

    @get_return_data
    def _get_date_list(self, input_date, flag):

        sql = "SELECT DISTINCT CHANGE_DATE FROM DSA.ORGIN_TABLE_DETAIL ORDER BY CHANGE_DATE"
        rows = self._getResultList(sql)

        datelist = []
        for date in rows:
            datelist.append(date[0])

        if input_date not in datelist:
            print(time.strftime('%Y-%m-%d %T', time.localtime()) + u"输入日期不存在于数据库")
            return -1, u"输入日期不存在于数据库"
        else:
            position = datelist.index(input_date)
            # 如果选择的是全量初始化，直接返回['19000101',input_date]
            if flag == 'A':
                datelist = ['19000101', input_date]
            elif flag == 'D':
                if position < 1:   ## 如果为数据库内第一个日期
                    print(time.strftime('%Y-%m-%d %T', time.localtime()) + u"输入日期是数据库中的第一个日期,但是你选择了增量模式")
                    return -1, u"输入日期是数据库中的第一个日期,但是你选择了增量模式"
                else:
                    datelist = datelist[position-1: position+1]
            else:
                print(time.strftime('%Y-%m-%d %T', time.localtime()) + u"输入模式不合法,请选择增量或全量模式(D/A)")
                return -1, u"输入模式不合法,请选择增量或全量模式(D/A)"

        return 0, datelist

    def deal_add_schema(self, schema):
        self._muti_outStream(u"新增模式名: %s\n" %schema)
        self.read_me_file.write(u"新增模式名：%s\n" %schema)
        self.job_schedule_file.write("--add schema\n")

        chk_sql = "SELECT COUNT(1) FROM ETL.JOB_METADATA WHERE JOB_NM = 'UNCOMPRESS_%s'" %schema
        rows = self._getResultList(chk_sql)
        if rows[0][0] == 0:
            self.job_schedule_file.write("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('UNCOMPRESS_%s','DAY','CMD','L_ODSLD','uncompress.sh','%s $dateid','5','1','UNCOMPRESS_%s','N',CURRENT TIMESTAMP,'1','1','%s','','%s');\n" %(schema, schema, schema, schema, config.IP))
            self._muti_outStream("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('UNCOMPRESS_%s','DAY','CMD','L_ODSLD','uncompress.sh','%s $dateid','5','1','UNCOMPRESS_%s','N',CURRENT TIMESTAMP,'1','1','%s','','%s');\n" %(schema, schema, schema, schema, config.IP))

        chk_sql = "SELECT COUNT(1) FROM ETL.JOB_METADATA WHERE JOB_NM = 'FTP_DOWNLOAD_%s'" %schema
        rows = self._getResultList(chk_sql)
        if rows[0][0] == 0:
            self.job_schedule_file.write("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD"
                                         ",PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,"
                                         "SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) "
                                         "VALUES ('FTP_DOWNLOAD_{schema}','DAY','FTP','FTP',"
                                         "'{distribute_server_ip} {FTP_USER} {FTP_PWD} 21 DOWNLOAD',"
                                         "'{distribute_server_path} {FTP_LOCAL_PATH} {schema}_{BANK_ID}_$dateid_ADD.tar.Z {schema}_{BANK_ID}_$dateid_ADD.tar.Z'"
                                         ",'5','1','FTP_DOWNLOAD_{schema}','N',CURRENT TIMESTAMP,'1','1','{schema}','','{IP}');\n"
                                         .format(FTP_LOCAL_PATH = config.FTP_LOCAL_PATH, schema=schema,
                                                 distribute_server_ip= config.distribute_server_ip,
                                                 distribute_server_path = config.distribute_server_path,
                                                 BANK_ID = config.BANK_ID, IP = config.IP,
                                                 FTP_USER=config.FTP_USER, FTP_PWD = config.FTP_PWD
                                                 )
                                         .format(schema=schema))
            self._muti_outStream("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD"
                                         ",PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,"
                                         "SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) "
                                         "VALUES ('FTP_DOWNLOAD_{schema}','DAY','FTP','FTP',"
                                         "'{distribute_server_ip} {FTP_USER} {FTP_PWD} 21 DOWNLOAD',"
                                         "'{distribute_server_path} {FTP_LOCAL_PATH} {schema}_{BANK_ID}_$dateid_ADD.tar.Z {schema}_{BANK_ID}_$dateid_ADD.tar.Z'"
                                         ",'5','1','FTP_DOWNLOAD_{schema}','N',CURRENT TIMESTAMP,'1','1','{schema}','','{IP}');\n"
                                         .format(FTP_LOCAL_PATH = config.FTP_LOCAL_PATH, schema=schema,
                                                 distribute_server_ip= config.distribute_server_ip,
                                                 distribute_server_path = config.distribute_server_path,
                                                 BANK_ID = config.BANK_ID, IP = config.IP,
                                                 FTP_USER=config.FTP_USER, FTP_PWD = config.FTP_PWD
                                                 )
                                         .format(schema=schema))

        chk_sql = "SELECT COUNT(1) FROM ETL.JOB_SEQ WHERE JOB_NM = 'UNCOMPRESS_%s'" %schema
        rows = self._getResultList(chk_sql)
        if rows[0][0] == 0:
            self.job_schedule_file.write("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('UNCOMPRESS_%s','FTP_DOWNLOAD_%s',CURRENT TIMESTAMP);\n" % (schema, schema))
            self._muti_outStream("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('UNCOMPRESS_%s','FTP_DOWNLOAD_%s',CURRENT TIMESTAMP);\n" % (schema, schema))

    def find_deal_schema(self, olddate, newdate, src_name):
        """
        找到两次变更中新增的schema, 进行处理
        """
        if config.SKIP_SCHEMA_LIST:
            skip_schema_str = ' AND trim(SRC_STM_ID) not in (%s)' %(','.join(["'%s'" %i for i in config.SKIP_SCHEMA_LIST]))
        else:
            skip_schema_str = ''

        sql = "SELECT DISTINCT(trim(SRC_STM_ID)) FROM DSA.ORGIN_TABLE_DETAIL WHERE CHANGE_DATE = '{0}' AND COMMENT NOT LIKE '%停止下发%'{skip_schema_str}".format(olddate, skip_schema_str=skip_schema_str)
        if config.isLinuxSystem():
            sql = "SELECT DISTINCT(trim(SRC_STM_ID)) FROM DSA.ORGIN_TABLE_DETAIL_T WHERE CHANGE_DATE = '{0}' AND (SOURCE_TYPE = '0' OR (SOURCE_TYPE='1' AND IS_DW_T_F='1')) {skip_schema_str}".format(olddate,skip_schema_str=skip_schema_str)

        self._muti_outStream(sql + '\n')
        rows = self._getResultList(sql)
        old_schema_list = []
        new_schema_list = []

        for i in rows:
            old_schema_list.append(i[0])

        sql = "SELECT DISTINCT(trim(SRC_STM_ID)) FROM DSA.ORGIN_TABLE_DETAIL WHERE CHANGE_DATE = '{0}' AND COMMENT NOT LIKE '%停止下发%'{skip_schema_str}".format(newdate,skip_schema_str=skip_schema_str)
        if config.isLinuxSystem():
            sql = "SELECT DISTINCT(trim(SRC_STM_ID)) FROM DSA.ORGIN_TABLE_DETAIL_T WHERE CHANGE_DATE = '{0}' AND (SOURCE_TYPE = '0' OR (SOURCE_TYPE='1' AND IS_DW_T_F='1')){skip_schema_str}".format(newdate,skip_schema_str=skip_schema_str)

        self._muti_outStream(sql + '\n')
        rows = self._getResultList(sql)
        for i in rows:
            new_schema_list.append(i[0])

        print "old_schema_list: ", old_schema_list
        print "new_schema_list:", new_schema_list

        for schema in new_schema_list:
            if schema not in old_schema_list:
                if not src_name:   ## 无输入参数
                    self.deal_add_schema(schema)
                else:
                    ##有输入参数时
                    if schema == src_name:
                        self.deal_add_schema(schema)

    def is_primary_table(self, table_name, input_date):
        """
        返回这张表是否为主键表, 返回主键列表 primary_key_list 若为空则为非主键表
        """
        primary_key_list = []
        if len(table_name.split(".")) != 2:
            raise Exception("tablename error! %s" % table_name)
        syscode, tablenm = table_name.split(".")
        sql = "SELECT TRIM(FIELD_CODE), CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END FROM DSA.ORGIN_TABLE_DETAIL WHERE TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablename}' AND CHANGE_DATE='{date}'".format(syscode=syscode, tablename=tablenm, date=input_date)

        rows = self._getResultList(sql)

        for row in rows:
            if row[1] == 'Y':
                primary_key_list.append(row[0])

        if primary_key_list:
            self._muti_outStream(u"%s 是主键表\n" %table_name)
        else:
            self._muti_outStream(u"%s 不是主键表\n" %table_name)
        return primary_key_list

    def is_flow_table(self, table_name, input_date):
        """
        返回这张表是否为流水表, 1,0
        """
        if len(table_name.split(".")) != 2:
            raise Exception("tablename error! %s" %table_name)
        syscode, tablenm = table_name.split(".")
        sql = "SELECT IS_RL_TBL_F FROM DSA.ORGIN_TABLE_DETAIL_T WHERE TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablename}' AND CHANGE_DATE='{date}'".format(syscode=syscode, tablename=tablenm, date=input_date)

        rows = self._getResultList(sql)

        for row in rows:
            if row[0] == '1':
                self._muti_outStream(u"{tablename} 是流水表\n".format(tablename=table_name))
                return True
            else:
                self._muti_outStream(u"{tablename} 不是流水表\n".format(tablename=table_name))
                return False

    def generate_ddl(self, table, type, input_date):
        """
        创建delta表, ods表 和 odshis表
        I: type = delta , ods , odshis
        """

        table_comment = ""
        field_comment = ""

        is_flow_table_flag = self.is_flow_table(table, input_date)

        delta_tablename = "DELTA."+table.replace(".", "_")
        his_tablename = "ODSHIS."+table.replace(".", "_")

        if len(table.split(".")) != 2:
            raise Exception("tablename error! %s" %table)
        syscode, tablenm = table.split(".")

        sql = "SELECT TRIM(FIELD_CODE),TRIM(DATA_TP),TRIM(LENGTH),TRIM(PRECSN),CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END, TRIM(TAB_NM),TRIM(FIELD_NM) FROM DSA.ORGIN_TABLE_DETAIL WHERE TRIM(SRC_STM_ID) ='{2}' AND TRIM(TAB_CODE) = '{0}' AND CHANGE_DATE='{1}' ORDER BY CAST(COLUMN_ID AS INT)".format(tablenm, input_date, syscode)

        rows = self._getResultList(sql)

        primary_list = []
        field_code_list = []
        filed_line_list = []
        field_code_dict = {}

        for row in rows:
            field_code, filed_type, length, precsn, is_primary, table_comment, field_comment = row

            table_comment = self._none_to_string(table_comment).replace('\'', '\'\'').replace('\"', '\\\"')
            field_comment = self._none_to_string(field_comment).replace('\'', '\'\'').replace('\"', '\\\"')

            field_code_list.append(field_code)
            field_code_dict[field_code] = field_comment

            if filed_type == "CHARACTER":
                filed_line = field_code +'\t'+'CHARACTER(' +length+ ')'
            elif filed_type == "CHAR":
                filed_line = field_code +'\t'+'CHAR(' +length+ ')'
            elif filed_type == "DECIMAL":
                filed_line = field_code +'\t'+'DECIMAL(' +length+','+precsn+ ')'
            elif filed_type == "VARCHAR":
                filed_line = field_code +'\t'+'VARCHAR(' +length+ ')'
            else:
                filed_line = field_code +'\t'+ filed_type

            if is_primary == "Y":
                filed_line = filed_line + "\t NOT NULL"
                primary_list.append(field_code)

            filed_line_list.append(filed_line)

        print("field_code_dict:", field_code_dict)

        self._muti_outStream("primary_list: %s\n" % ','.join(primary_list))
        etl_dt_code = 'ETL_DT' if 'ETL_DT' not in field_code_list else 'ETL_DATE'
        eff_dt_code = 'EFF_DT' if 'EFF_DT' not in field_code_list else 'EFF_DATE'
        end_dt_code = 'END_DT' if 'END_DT' not in field_code_list else 'END_DATE'

        if type == "delta":
            self._muti_outStream("generate table {tablename}\n".format(tablename=delta_tablename))
            self.delta_ddl_file.write("\nCREATE TABLE {delta_tablename} (\n".format(delta_tablename=delta_tablename))
            self.delta_ddl_file.write("\n,".join(filed_line_list))
            self.delta_ddl_file.write('\n,%s\tDATE' %etl_dt_code)
            self.delta_ddl_file.write("\n,ETL_FLAG	CHARACTER(1)	With Default 'I'\n) IN {tablespace}\n".format(tablespace=config.TABSPACE))
            if primary_list:
                self.delta_ddl_file.write('Partitioning Key ({primary_key_str}) Using Hashing\n'.format(primary_key_str=','.join(primary_list)))
            self.delta_ddl_file.write('Compress Yes;\n')

            if table_comment:
                self.delta_ddl_file.write("Comment on Table "+delta_tablename+" is "+'\''+table_comment+'\';\n')

            for field_code, field_comment in field_code_dict.items():
                self.delta_ddl_file.write("Comment on Column "+delta_tablename+'.'+field_code+'\tis \''+field_comment+'\';\n')

            if primary_list:
                self.delta_ddl_file.write('\ncreate  Index '+delta_tablename + '\n')
                self.delta_ddl_file.write('   on '+delta_tablename + '\n')
                self.delta_ddl_file.write('   ({primary_key_str})    Allow Reverse Scans;\n\n'.format(primary_key_str=','.join(primary_list)))

        elif type == 'ods':
            if table.upper() == "CORE.BGFMCINF":
                self._muti_outStream("CORE.BGFMCINF is not generated, continue...\n")
                self.read_me_file.write("跳过生成ODS表CORE_BGFMCINF\n")
                return

            self._muti_outStream("generate table {tablename}\n".format(tablename=table))
            self.ods_ddl_file.write("\nCREATE TABLE {ods_tablename} (\n".format(ods_tablename=table))
            self.ods_ddl_file.write("\n,".join(filed_line_list))

            if not is_flow_table_flag:
                self.ods_ddl_file.write('\n,%s\tDATE\tNOT NULL' %eff_dt_code)
                self.ods_ddl_file.write('\n,%s\tDATE' %end_dt_code)
            else:
                self.ods_ddl_file.write('\n,LAST_ETL_ACG_DT\tDATE\t')

            self.ods_ddl_file.write("\n,JOB_SEQ_ID	INTEGER\n) IN {tablespace}\n".format(tablespace=config.TABSPACE))

            if primary_list:
                if not is_flow_table_flag:
                    self.ods_ddl_file.write('Partitioning Key ({primary_key_str}, {effDtCode}) Using Hashing\n'.format(primary_key_str=','.join(primary_list), effDtCode=eff_dt_code))
                else:
                    self.ods_ddl_file.write('Partitioning Key ({primary_key_str}) Using Hashing\n'.format(primary_key_str=','.join(primary_list)))
            else:
                self.ods_ddl_file.write('Partitioning Key ({effDtCode}) Using Hashing\n'.format(effDtCode=eff_dt_code))

            self.ods_ddl_file.write('Compress Yes;\n')

            if table_comment:
                self.ods_ddl_file.write("Comment on Table "+table+" is "+'\''+table_comment+'\';\n')

            for field_code, field_comment in field_code_dict.items():
                self.ods_ddl_file.write("Comment on Column "+table+'.'+field_code+'\tis \''+field_comment+'\';\n')

            if not is_flow_table_flag:
                self.ods_ddl_file.write('\ncreate  Index '+table+'_'+input_date+'_1\n')
                self.ods_ddl_file.write('   on '+table+'\n')
                self.ods_ddl_file.write('   ('+end_dt_code+')    Allow Reverse Scans;\n')

            self.ods_ddl_file.write('create  Index '+table+'_'+input_date+'_2\n')
            self.ods_ddl_file.write('   on '+table+'\n')
            self.ods_ddl_file.write('   (JOB_SEQ_ID)    Allow Reverse Scans;\n\n')

            if primary_list:
                if not is_flow_table_flag:
                    self.ods_ddl_file.write('ALTER TABLE {tablename} ADD PRIMARY KEY ({primary_str}, {effDtCode});\n\n'.format(tablename=table, primary_str=','.join(primary_list), effDtCode=eff_dt_code))
                else:
                    self.ods_ddl_file.write('ALTER TABLE {tablename} ADD PRIMARY KEY ({primary_str});\n\n'.format(tablename=table, primary_str=','.join(primary_list)))

        elif type == 'odshis':
            self._muti_outStream("generate table {tablename}\n".format(tablename=his_tablename))
            self.his_ddl_file.write("\nCREATE TABLE {his_tablename} (\n".format(his_tablename=his_tablename))
            self.his_ddl_file.write("\n,".join(filed_line_list))

            if not is_flow_table_flag:
                self.his_ddl_file.write('\n,%s\tDATE\tNOT NULL' %eff_dt_code)
                self.his_ddl_file.write('\n,%s\tDATE' %end_dt_code)
            else:
                self.his_ddl_file.write('\n,LAST_ETL_ACG_DT\tDATE\t')

            self.his_ddl_file.write("\n,JOB_SEQ_ID\tINTEGER\n,NEW_JOB_SEQ_ID\tINTEGER\n)  IN {tablespace}\n".format(tablespace=config.TABSPACE))

            if primary_list:
                self.his_ddl_file.write('Partitioning Key ({primary_key_str}) Using Hashing\n'.format(primary_key_str=','.join(primary_list)))

            self.his_ddl_file.write('Compress Yes;\n')

            if table_comment:
                self.his_ddl_file.write("Comment on Table "+his_tablename+" is "+'\''+table_comment+'\';\n')

            for field_code, field_comment in field_code_dict.items():
                self.his_ddl_file.write("Comment on Column "+his_tablename+'.'+field_code+'\tis \''+field_comment+'\';\n')

            self.his_ddl_file.write('\ncreate  Index '+his_tablename+'_'+input_date+'\n')
            self.his_ddl_file.write('   on '+his_tablename + '\n')
            self.his_ddl_file.write('   (NEW_JOB_SEQ_ID)    Allow Reverse Scans;\n\n')
        else:
            self._muti_outStream("create ddl type error !!!!\n")
            raise_with_traceback(ValueError("create ddl type error !!!!\n"))

    def get_field_code_list(self, table, input_date):
        """
        获取表的所有列
        """

        field_code_list = []

        if len(table.split(".")) != 2:
            raise Exception("tablename error! %s" %table)
        syscode, tablenm = table.split(".")

        sql = "SELECT TRIM(FIELD_CODE) FROM DSA.ORGIN_TABLE_DETAIL WHERE TRIM(SRC_STM_ID) ='{2}' AND TRIM(TAB_CODE) = '{0}' AND CHANGE_DATE='{1}' ORDER BY CAST(COLUMN_ID AS INT)".format(tablenm, input_date, syscode)

        rows = self._getResultList(sql)

        for row in rows:
            field_code_list.append(row[0])

        return field_code_list

    def generate_ap_sql_init(self, table, input_date):
        """
        生成ap_sql_init
        """
        delta_tablename = "DELTA."+table.replace(".", "_")
        his_tablename = "ODSHIS."+table.replace(".", "_")

        is_flow_table_flag = self.is_flow_table(table, input_date)

        if len(table.split(".")) != 2:
            raise Exception("tablename error! %s" %table)
        syscode, tablenm = table.split(".")

        field_code_list = self.get_field_code_list(table, input_date)

        self._muti_outStream(table + ":generate ap_sql_init\n")
        file = open(config.apsql_path.format(date=input_date, APNAME='AP_ODS_'+table.replace('.', '_')+'_INIT.SQL'), 'w')
        file.write('SELECT \'Rows readed:\',COUNT(1),\'Rows changed:\',COUNT(1) FROM (SELECT 1 FROM '+delta_tablename+') S;\n')
        file.write('VALUES(\'Rows updated:\',0);\n')

        src_list = [ 'S.' + field_code for field_code in field_code_list ]

        if is_flow_table_flag:
            src_str = ','.join(src_list) + ",'#DATEOFDATA#', New_JOB_SEQ_ID FROM "
        else:
            src_str = ','.join(src_list) + ",'#DATEOFDATA#','9999-12-31',New_JOB_SEQ_ID FROM "
        file.write("DECLARE MYCUR CURSOR FOR SELECT "+src_str+delta_tablename+" S;\n")

        des_str = ','.join(field_code_list)

        effDtCode = 'EFF_DT' if 'EFF_DT' not in field_code_list else 'EFF_DATE'
        endDtCode = 'END_DT' if 'END_DT' not in field_code_list else 'END_DATE'

        if is_flow_table_flag:
            file.write("LOAD FROM MYCUR OF CURSOR REPLACE INTO "+table+'('+des_str+',LAST_ETL_ACG_DT,JOB_SEQ_ID);\n')
        else:
            file.write("LOAD FROM MYCUR OF CURSOR REPLACE INTO "+table+'('+des_str+','+effDtCode+','+endDtCode+',JOB_SEQ_ID);\n')
        file.close()

    def generate_ap_sql(self, table, input_date):
        """
        生成ap_sql
        """

        delta_tablename = "DELTA."+table.replace(".", "_")
        his_tablename = "ODSHIS."+table.replace(".", "_")

        is_flow_table_flag = self.is_flow_table(table, input_date)
        primary_list = self.is_primary_table(table, input_date)

        field_code_list = self.get_field_code_list(table, input_date)

        effDtCode = 'EFF_DT' if 'EFF_DT' not in field_code_list else 'EFF_DATE'
        endDtCode = 'END_DT' if 'END_DT' not in field_code_list else 'END_DATE'

        self._muti_outStream(table+":generate ap_sql\n")

        file = open(config.apsql_path.format(date=input_date, APNAME='AP_ODS_'+table.replace('.', '_')+'.SQL'), 'w')

        if not is_flow_table_flag:
            field_code_str = ','.join(field_code_list)+','+effDtCode+','+endDtCode+',JOB_SEQ_ID'
            if primary_list: ## 如果为主键表 0712 修改 (更改)
                file.write("SELECT \'Rows updated:\',COUNT(1) FROM (SELECT 1 FROM "+delta_tablename+" WHERE ETL_FLAG IN (\'A\',\'D\')) S;\n\n")
                file.write('--REDO：DELETE LAST JOB LOADED DATA\n')
                file.write('DELETE FROM '+table+' WHERE JOB_SEQ_ID= New_JOB_SEQ_ID;\n\n')
                file.write('--REDO：RECOVERY DATA FROM HISTORY TABLE\n')
                file.write('INSERT INTO '+table+'('+field_code_str+')\n')
                file.write('select '+field_code_str+'\nfrom '+his_tablename+' WHERE NEW_JOB_SEQ_ID= New_JOB_SEQ_ID;\n\n')
                file.write('--REDO：DELETE HISTORY DATA\n')
                file.write('DELETE FROM '+his_tablename+' WHERE NEW_JOB_SEQ_ID= New_JOB_SEQ_ID;\n\n')
                file.write('--BACKUP DATA TO HISTROY TABLE\n')
                file.write('SELECT \'Rows readed:\',COUNT(1),\'Rows changed:\',COUNT(1) FROM (SELECT 1 FROM '+delta_tablename+' WHERE ETL_FLAG IN (\'I\',\'A\',\'D\')) S;\n')
                file.write('SELECT \'Rows updated:\',COUNT(1) FROM NEW TABLE (\n')
                file.write('INSERT INTO '+his_tablename+'('+field_code_str+',NEW_JOB_SEQ_ID)\n')
                file.write('select '+field_code_str+',New_JOB_SEQ_ID\n from '+ str(table) + ' T\n')
                file.write('WHERE T.'+endDtCode+'=\'9999-12-31\' AND EXISTS ( SELECT 1 FROM '+delta_tablename+' S\n')

                primary_str = ' AND '.join("T.%s=S.%s" %(x, x) for x in primary_list)

                file.write('WHERE '+primary_str+' ));\n\n')
                file.write('--DROP ZIPPER\n')
                file.write('MERGE INTO '+table+' T \nUSING (SELECT * FROM '\
                    +delta_tablename+' WHERE ETL_FLAG IN (\'I\',\'D\',\'A\')) S\nON '\
                    +primary_str+'  AND T.'+endDtCode+'=\'9999-12-31\' \n   WHEN MATCHED THEN UPDATE SET \nT.'+endDtCode+'=\'#DATEOFDATA#\', T.JOB_SEQ_ID= New_JOB_SEQ_ID;\n\n')
                file.write('--CREATE ZIPPER\nINSERT INTO '+table+'('+field_code_str+')\nselect '+','.join(field_code_list)+',\'#DATEOFDATA#\',\'9999-12-31\',New_JOB_SEQ_ID\n')
                file.write('from '+delta_tablename+' where ETL_FLAG in (\'A\',\'I\');\n\n')
                file.write('--CONFERM DATA INTEGRITY\n')
                file.write('MERGE INTO '+table+' T \nUSING (SELECT * FROM '+delta_tablename+' WHERE ETL_FLAG = \'D\' ) S\n')
                file.write('ON '+primary_str+'\nWHEN NOT MATCHED THEN\nINSERT ('+field_code_str+')\n')
                file.write('VALUES ('+','.join(field_code_list)+',\'#DATEOFDATA#\',\'#DATEOFDATA#\',New_JOB_SEQ_ID);')
            else: ## 如果为非主键表 (更改以下)
                file.write("SELECT \'Rows updated:\',COUNT(1) FROM (SELECT 1 FROM "+delta_tablename+" WHERE ETL_FLAG IN (\'A\',\'D\',\'I\')) S;\n\n")
                file.write("----重跑：删除已跑入数据\n")
                file.write("DELETE FROM "+table+" WHERE JOB_SEQ_ID= New_JOB_SEQ_ID;\n")
                file.write('--加链\n')
                file.write('INSERT INTO '+table+'('+field_code_str+')\n')
                file.write('select '+','.join(field_code_list)+',\'#DATEOFDATA#\',\'#DATEOFDATA#\',New_JOB_SEQ_ID\n')
                file.write('from '+delta_tablename+' where ETL_FLAG in (\'D\');\n\n')
                file.write('--加链\n')
                file.write('INSERT INTO '+table+'('+field_code_str+')\n')
                file.write('select '+','.join(field_code_list)+',\'#DATEOFDATA#\',\'9999-12-31\',New_JOB_SEQ_ID\n')
                file.write('from '+delta_tablename+' where ETL_FLAG in (\'A\',\'I\');\n\n')

        else:
            if primary_list:

                field_code_str = ','.join(field_code_list)+',LAST_ETL_ACG_DT,JOB_SEQ_ID'

                file.write("SELECT \'Rows updated:\',COUNT(1) FROM (SELECT 1 FROM "+delta_tablename+" WHERE ETL_FLAG IN (\'A\',\'D\')) S;\n\n")
                file.write('--REDO：DELETE LAST JOB LOADED DATA\n')
                file.write('DELETE FROM '+table+' WHERE JOB_SEQ_ID= New_JOB_SEQ_ID;\n\n')
                file.write('--REDO：RECOVERY DATA FROM HISTORY TABLE\n')
                file.write('INSERT INTO '+table+'('+field_code_str+')\n')
                file.write('select '+field_code_str+'\nfrom '+his_tablename+' WHERE NEW_JOB_SEQ_ID= New_JOB_SEQ_ID;\n\n')
                file.write('--REDO：DELETE HISTORY DATA\n')
                file.write('DELETE FROM '+his_tablename+' WHERE NEW_JOB_SEQ_ID= New_JOB_SEQ_ID;\n\n')
                file.write('--BACKUP DATA TO HISTROY TABLE\n')
                file.write('SELECT \'Rows readed:\',COUNT(1),\'Rows changed:\',COUNT(1) FROM (SELECT 1 FROM '+delta_tablename+' WHERE ETL_FLAG IN (\'I\',\'A\',\'D\')) S;\n')
                file.write('SELECT \'Rows updated:\',COUNT(1) FROM NEW TABLE (\n')
                file.write('INSERT INTO '+his_tablename+'('+field_code_str+',NEW_JOB_SEQ_ID)\n')
                file.write('select ' + field_code_str + ',New_JOB_SEQ_ID\n from '+ str(table) + ' T\n')
                file.write('WHERE EXISTS ( SELECT 1 FROM '+delta_tablename+' S\n')

                primary_str = ' AND '.join("T.%s=S.%s" %(x, x) for x in primary_list)

                filed_code_str = '\n,'.join("T.%s=S.%s" %(x, x) for x in field_code_list)
                s_filed_code_list = ['S.' + field_code for field_code in field_code_list]

                file.write('WHERE '+primary_str+' ));\n\n')
                file.write('--DROP ZIPPER\n')
                file.write('MERGE INTO '+table+' T \nUSING (SELECT * FROM ' \
                    +delta_tablename+' WHERE ETL_FLAG IN (\'I\',\'D\',\'A\')) S\nON ' \
                    +primary_str+"\nWHEN MATCHED AND ETL_FLAG ='D' THEN DELETE\nWHEN MATCHED AND ETL_FLAG <>'D' THEN UPDATE SET " \
                    +filed_code_str +"\n,T.LAST_ETL_ACG_DT='#DATEOFDATA#'\n,T.JOB_SEQ_ID =New_JOB_SEQ_ID\nWHEN NOT MATCHED AND ETL_FLAG IN ('A','I') THEN \nINSERT (" \
                    +field_code_str +")\nVALUES\n(" + ','.join(s_filed_code_list) + ",'#DATEOFDATA#',New_JOB_SEQ_ID);\n")
            else:
                field_code_str = ','.join(field_code_list)+',LAST_ETL_ACG_DT,JOB_SEQ_ID'

                file.write('--REDO：DELETE LAST JOB LOADED DATA\n')
                file.write('DELETE FROM '+table+' WHERE JOB_SEQ_ID= New_JOB_SEQ_ID;\n\n')
                file.write('--INSERT\n')
                file.write("INSERT INTO "+ table + "(" +field_code_str +")\nSELECT " + field_code_str +" FROM " + delta_tablename + " WHERE ETL_FLAG IN ('I','A');\n")

        file.close()

    def generate_ap_sql_yatopupdate(self, table, add_column_list, primary_list, input_date):
        """
        生成ap_yatopupdate
        """
        field_code_list = self.get_field_code_list(table, input_date)
        is_flow_table_flag = self.is_flow_table(table, input_date)

        his_tablename = "ODSHIS."+table.replace(".", "_")

        self._muti_outStream(table + ":generate ap_sql_yatopupdate\n")

        if is_flow_table_flag:
            if primary_list:
                f = open(config.apsql_path.format(date=input_date, APNAME='AP_ODS_'+table.replace('.', '_')+'_YATOPUPDATE.SQL'), 'w')
                primary_str = ','.join(primary_list)
                primary_and_str = ' AND '.join("A.%s=B.%s" %(x, x) for x in primary_list)
                primary_and_str2 = ' AND '.join("T.%s=S.%s" %(x, x) for x in primary_list)

                add_str = ','.join(add_column_list)
                add_and_str = ','.join("T.%s=S.%s" %(x, x) for x in add_column_list)


                null_str = ' = NULL,'.join(add_column_list)
                null_str = null_str + " = NULL"

                all_column = ','.join(field_code_list)

                f.write("--redo:\n")
                f.write("UPDATE {0} SET {1};\n".format(table, null_str))

                f.write("DELETE FROM {0} WHERE JOB_SEQ_ID= (SELECT JOB_SEQ_ID FROM ETL.JOB_LOG WHERE TO_CHAR(DATA_PRD,'yyyymmdd')='{1}' AND JOB_NM ='AP_ODS_{2}');\n".format(table, input_date, table.replace('.', '_')))

                f.write("INSERT INTO {0}({1},LAST_ETL_ACG_DT,JOB_SEQ_ID) select {1},LAST_ETL_ACG_DT,JOB_SEQ_ID from {2} WHERE NEW_JOB_SEQ_ID= (SELECT  JOB_SEQ_ID FROM ETL.JOB_LOG WHERE TO_CHAR(DATA_PRD,'yyyymmdd')='{3}' AND JOB_NM ='AP_ODS_{4}');\n".format(table, all_column, his_tablename, input_date, table.replace('.', '_')))

                f.write("DELETE FROM {0} WHERE NEW_JOB_SEQ_ID= (SELECT  JOB_SEQ_ID FROM ETL.JOB_LOG WHERE TO_CHAR(DATA_PRD,'yyyymmdd')='{1}' AND JOB_NM ='AP_ODS_{2}');\n".format(his_tablename, input_date, table.replace('.', '_')))

                f.write("DROP TABLE DELTA.{0}_YATOPUPDATE;\n\n".format(table.replace('.', '_')))
                f.write("CREATE TABLE DELTA.{0}_YATOPUPDATE LIKE DELTA.{0};\n".format(table.replace('.', '_')))

                f.write("LOAD CLIENT FROM /etl/etldata/input/init/{0}/{1}_{0} of del replace into DELTA.{1}_YATOPUPDATE;\n".format(input_date, table.replace('.', '_')))

                f.write("MERGE INTO {0} T USING (SELECT {1},{2} FROM DELTA.{3}_YATOPUPDATE A WHERE NOT EXISTS (SELECT 1 FROM DELTA.{3} B WHERE {4})) S ON {5} AND T.LAST_ETL_ACG_DT='#DATEOFDATA#' WHEN MATCHED THEN UPDATE SET {6};\n".format(table, primary_str, add_str, table.replace('.', '_'), primary_and_str, primary_and_str2, add_and_str))

                f.write("DROP TABLE DELTA.{0}_YATOPUPDATE;\n".format(table.replace('.', '_')))

                f.close()

        else:
            f = open(config.apsql_path.format(date=input_date, APNAME='AP_ODS_'+table.replace('.', '_')+'_YATOPUPDATE.SQL'), 'w')

            if primary_list:
                primary_str = ','.join(primary_list)
                primary_and_str = ' AND '.join("A.%s=B.%s" %(x, x) for x in primary_list)
                primary_and_str2 = ' AND '.join("T.%s=S.%s" %(x, x) for x in primary_list)
            else:
                primary_str = ','.join(field_code_list)
                primary_and_str = ' AND '.join("A.%s=B.%s" %(x, x) for x in field_code_list)
                primary_and_str2 = ' AND '.join("T.%s=S.%s" %(x, x) for x in field_code_list)

            add_str = ','.join(add_column_list)
            add_and_str = ','.join("T.%s=S.%s" %(x, x) for x in add_column_list)


            null_str = ' = NULL,'.join(add_column_list)
            null_str = null_str + " = NULL"

            all_column = ','.join(field_code_list)

            eff_dt_code = 'EFF_DT' if 'EFF_DT' not in field_code_list else 'EFF_DATE'

            end_dt_code = 'END_DT' if 'END_DT' not in field_code_list else 'END_DATE'

            f.write("--redo:\n")
            sql = "UPDATE {0} SET {1};\n".format(table, null_str)
            f.write(sql)

            sql = "DELETE FROM {0} WHERE JOB_SEQ_ID= (SELECT JOB_SEQ_ID FROM ETL.JOB_LOG WHERE TO_CHAR(DATA_PRD,'yyyymmdd')='{1}' AND JOB_NM ='AP_ODS_{2}');\n".format(table, input_date, table.replace('.', '_'))
            f.write(sql)

            sql = "INSERT INTO {0}({1},{5},{6},JOB_SEQ_ID) select {1},{5},{6},JOB_SEQ_ID from {2} WHERE NEW_JOB_SEQ_ID= (SELECT  JOB_SEQ_ID FROM ETL.JOB_LOG WHERE TO_CHAR(DATA_PRD,'yyyymmdd')='{3}' AND JOB_NM ='AP_ODS_{4}');\n".format(table, all_column, his_tablename, input_date, table.replace('.', '_'), eff_dt_code, end_dt_code)
            f.write(sql)

            sql = "DELETE FROM {0} WHERE NEW_JOB_SEQ_ID= (SELECT  JOB_SEQ_ID FROM ETL.JOB_LOG WHERE TO_CHAR(DATA_PRD,'yyyymmdd')='{1}' AND JOB_NM ='AP_ODS_{2}');\n".format(his_tablename, input_date, table.replace('.', '_'))
            f.write(sql)
            f.write("DROP TABLE DELTA.{0}_YATOPUPDATE;\n\n".format(table.replace('.', '_')))

            sql = "CREATE TABLE DELTA.{0}_YATOPUPDATE LIKE DELTA.{0};\n".format(table.replace('.', '_'))
            f.write(sql)
            sql = "LOAD CLIENT FROM /etl/etldata/input/init/{0}/{1}_{0} of del replace into DELTA.{1}_YATOPUPDATE;\n".format(input_date, table.replace('.', '_'))
            f.write(sql)

            sql = "MERGE INTO {0} T USING (SELECT {1},{2} FROM DELTA.{3}_YATOPUPDATE A WHERE NOT EXISTS (SELECT 1 FROM DELTA.{3} B WHERE {4})) S ON {5} AND T.{7}='9999-12-31' WHEN MATCHED THEN UPDATE SET {6};\n".format(table, primary_str, add_str, table.replace('.', '_'), primary_and_str, primary_and_str2, add_and_str, end_dt_code)
            f.write(sql)
            sql = "DROP TABLE DELTA.{0}_YATOPUPDATE;\n".format(table.replace('.', '_'))
            f.write(sql)
            f.close()

    def deal_table_add(self, add_tablelist, input_date):
        """
        处理新增表
        """
        def muti_outStream(msg):
            self.compare_log_file.write(msg + '\n')
            sys.stdout.write(msg + '\n')
            self.job_schedule_file.write(msg + '\n')

        for table in add_tablelist:
            '''
            新建delta表, ods表, odshis表
            '''

            # 查找受影响的作业
            self._getInfluencedJob(table)

            self._muti_outStream(u"新增下发表{tablename}\n".format(tablename=table))
            self.generate_ddl(table, type='delta', input_date=input_date)
            self.generate_ddl(table, type='ods', input_date=input_date)
            self.generate_ddl(table, type='odshis', input_date=input_date)
            '''
            新建apsql_init 和 apsql
            '''
            self.generate_ap_sql(table, input_date)
            self.generate_ap_sql_init(table, input_date)

            tablestr = table.replace('.', '_')
            if len(table.split(".")) != 2:
                raise Exception("tablename error! %s" % table)
            syscode, tablenm = table.split('.')

            muti_outStream("--add table")
            muti_outStream("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('LD_ODS_%s_INIT','DAY','CMD','L_ODSLD','load_from_files_to_nds.sh','%s %s $dateid ALL','5','1','LD_ODS_%s_INIT','Y',CURRENT TIMESTAMP,'1','1','%s','','%s');" %(tablestr, tablenm, syscode, tablestr, syscode, config.IP))
            muti_outStream("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('AP_ODS_%s_INIT','DAY','SQL','L_ODS','AP_ODS_%s_INIT.SQL','EDW /etl/etldata/script/odssql','5','1','AP_ODS_%s_INIT','Y',CURRENT TIMESTAMP,'1','1','%s','','%s');" %(tablestr, tablestr, tablestr, syscode, config.IP))
            muti_outStream("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('LD_ODS_%s','DAY','CMD','L_ODSLD','load_from_files_to_nds.sh','%s %s $dateid ADD','5','1','LD_ODS_%s','N',CURRENT TIMESTAMP,'1','1','%s','','%s');" %(tablestr, tablenm, syscode, tablestr, syscode, config.IP))
            muti_outStream("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('AP_ODS_%s','DAY','SQL','L_ODS','AP_ODS_%s.SQL','EDW /etl/etldata/script/odssql','5','1','AP_ODS_%s','N',CURRENT TIMESTAMP,'1','1','%s','','%s');" %(tablestr, tablestr, tablestr, syscode, config.IP))
            # 0712 修改 muti_outStream("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('AP_ODS_%s_YATOPUPDATE','DAY','SQL','L_ODS','AP_ODS_%s_YATOPUPDATE.SQL','EDW /etl/etldata/script/odssql','5','1','AP_ODS_%s_YATOPUPDATE','U',CURRENT TIMESTAMP,'1','1','%s','','%s');\n" %(tablestr,tablestr,tablestr,syscode,ip))
            muti_outStream("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('LD_ODS_%s_INIT','UNCOMPRESS_INIT',CURRENT TIMESTAMP);" %tablestr)
            muti_outStream("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('LD_ODS_%s','UNCOMPRESS_%s',CURRENT TIMESTAMP);" %(tablestr,syscode))
            muti_outStream("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s','LD_ODS_%s',CURRENT TIMESTAMP);" %(tablestr,tablestr))
            muti_outStream("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s','LD_ODS_%s_INIT',CURRENT TIMESTAMP);" %(tablestr,tablestr))
            muti_outStream("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s_INIT','LD_ODS_%s_INIT',CURRENT TIMESTAMP);" %(tablestr,tablestr))
            muti_outStream("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('ODS_DONE','AP_ODS_%s',CURRENT TIMESTAMP);" %tablestr)
            muti_outStream("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('ODS_DONE_INIT','AP_ODS_%s_INIT',CURRENT TIMESTAMP);" %tablestr)
            # 0712 修改 muti_outStream("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s_YATOPUPDATE','LD_ODS_%s',CURRENT TIMESTAMP);" %(tablestr,tablestr))
            # 0712 修改 muti_outStream("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s_YATOPUPDATE','UNCOMPRESS_INIT',CURRENT TIMESTAMP);" %tablestr)
            # 0712 修改 muti_outStream("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s','AP_ODS_%s_YATOPUPDATE',CURRENT TIMESTAMP);" %(tablestr,tablestr))

            muti_outStream("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';")
            muti_outStream("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';")
            muti_outStream("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='LD_ODS_%s_INIT';" %tablestr)
            muti_outStream("UPDATE ETL.JOB_METADATA SET INIT_FLAG='W' WHERE JOB_NM ='LD_ODS_%s';\n" %tablestr)

    def deal_table_del(self, del_tablelist, input_date):
        """
        处理删除表
        """

        for table in del_tablelist:
            self._muti_outStream(u"删除下发表{tablename}\n".format(tablename=table))
            self.job_schedule_file.write("--del table\n")
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG ='X' WHERE JOB_NM LIKE '%_ODS_{0}%';\n".format(table.replace('.', '_')))

    @get_return_data
    def find_different_tables(self, olddate, newdate, src_name, table_list):
        """
        找到两次变更中新增的表和删除的表
        """
        if config.SKIP_SCHEMA_LIST:
            skip_schema_str = ' AND trim(SRC_STM_ID) not in (%s)' %(','.join(["'%s'" %i.upper() for i in config.SKIP_SCHEMA_LIST]))
        else:
            skip_schema_str = ''

        old_tablelist = []

        if not src_name and not table_list: ## 若都为空
            sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL WHERE CHANGE_DATE={0} AND COMMENT NOT LIKE '%停止下发%'{skip_schema_str}".format(olddate,skip_schema_str=skip_schema_str)
            if config.isLinuxSystem():
                sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL_T WHERE CHANGE_DATE={0} AND ALT_STS_TP <> 'D' AND (SOURCE_TYPE = '0' OR (SOURCE_TYPE='1' AND IS_DW_T_F='1')){skip_schema_str}".format(olddate,skip_schema_str=skip_schema_str)
        elif src_name and not table_list:  ## 值存在模式名
            sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL WHERE CHANGE_DATE={0} AND TRIM(SRC_STM_ID)='{1}' AND COMMENT NOT LIKE '%停止下发%'{skip_schema_str}".format(olddate, src_name,skip_schema_str=skip_schema_str)
            if config.isLinuxSystem():
                sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL_T WHERE CHANGE_DATE={0} AND ALT_STS_TP <> 'D' AND TRIM(SRC_STM_ID)='{1}' AND (SOURCE_TYPE = '0' OR (SOURCE_TYPE='1' AND IS_DW_T_F='1')){skip_schema_str}".format(olddate, src_name,skip_schema_str=skip_schema_str)
        elif src_name and table_list:
            sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL WHERE CHANGE_DATE={0} AND TRIM(SRC_STM_ID)='{1}' AND TRIM(TAB_CODE) IN ({2}) AND COMMENT NOT LIKE '%停止下发%'{skip_schema_str}".format(olddate, src_name, ','.join(["\'%s\'" %x for x in table_list]),skip_schema_str=skip_schema_str)
            if config.isLinuxSystem():
                sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL_T WHERE CHANGE_DATE={0} AND ALT_STS_TP <> 'D' AND TRIM(SRC_STM_ID)='{1}' AND TRIM(TAB_CODE) IN ({2}) AND (SOURCE_TYPE = '0' OR (SOURCE_TYPE='1' AND IS_DW_T_F='1')){skip_schema_str}".format(olddate, src_name, ','.join(["\'%s\'" %x for x in table_list]), skip_schema_str=skip_schema_str)
        else:
            self._muti_outStream(u"异常:模式名为空且表名不为空")
            return -1, u'异常:模式名为空且表名不为空'

        self._muti_outStream(sql + '\n')
        rows = self._getResultList(sql)


        for row in rows:
            old_tablelist.append(row[0])

        old_set = set(old_tablelist)


        new_tablelist = []
        if not src_name and not table_list: ## 若都为空
            sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL WHERE CHANGE_DATE={0} AND COMMENT NOT LIKE '%停止下发%'{skip_schema_str}".format(newdate, skip_schema_str=skip_schema_str)
            if config.isLinuxSystem():
                sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL_T WHERE CHANGE_DATE={0} AND ALT_STS_TP <> 'D' AND (SOURCE_TYPE = '0' OR (SOURCE_TYPE='1' AND IS_DW_T_F='1')){skip_schema_str}".format(newdate, skip_schema_str=skip_schema_str)
        elif src_name and not table_list:  ## 值存在模式名
            sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL WHERE CHANGE_DATE={0} AND TRIM(SRC_STM_ID)='{1}' AND COMMENT NOT LIKE '%停止下发%'{skip_schema_str}".format(newdate, src_name, skip_schema_str=skip_schema_str)
            if config.isLinuxSystem():
                sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL_T WHERE CHANGE_DATE={0} AND ALT_STS_TP <> 'D' AND TRIM(SRC_STM_ID)='{1}' AND (SOURCE_TYPE = '0' OR (SOURCE_TYPE='1' AND IS_DW_T_F='1')){skip_schema_str}".format(newdate, src_name, skip_schema_str=skip_schema_str)
        elif src_name and table_list:
            sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL WHERE CHANGE_DATE={0} AND TRIM(SRC_STM_ID)='{1}' AND TRIM(TAB_CODE) IN ({2}) AND COMMENT NOT LIKE '%停止下发%'{skip_schema_str}".format(newdate, src_name, ','.join(["\'%s\'" %x for x in table_list]), skip_schema_str=skip_schema_str)
            if config.isLinuxSystem():
                sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL_T WHERE CHANGE_DATE={0} AND ALT_STS_TP <> 'D' AND TRIM(SRC_STM_ID)='{1}' AND TRIM(TAB_CODE) IN ({2}) AND (SOURCE_TYPE = '0' OR (SOURCE_TYPE='1' AND IS_DW_T_F='1')){skip_schema_str}".format(newdate, src_name, ','.join(["\'%s\'" %x for x in table_list]), skip_schema_str=skip_schema_str)
        else:
            self._muti_outStream(u"异常:模式名为空且表名不为空")
            return -1, u'异常:模式名为空且表名不为空'

        self._muti_outStream(sql + '\n')
        rows = self._getResultList(sql)

        for row in rows:
            new_tablelist.append(row[0])

        new_set = set(new_tablelist)

        add_set = new_set - old_set

        common_set = old_set & new_set

        del_tablelist = []
        if config.isLinuxSystem():
            if not src_name and not table_list:  ## 若都为空
                sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL_T WHERE CHANGE_DATE={0} AND ALT_STS_TP = 'D' AND (SOURCE_TYPE = '0' OR (SOURCE_TYPE='1' AND IS_DW_T_F='1')){skip_schema_str}".format(
                    newdate, skip_schema_str=skip_schema_str)
            elif src_name and not table_list:  ## 值存在模式名
                sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL_T WHERE CHANGE_DATE={0} AND ALT_STS_TP = 'D' AND TRIM(SRC_STM_ID)='{1}' AND (SOURCE_TYPE = '0' OR (SOURCE_TYPE='1' AND IS_DW_T_F='1')){skip_schema_str}".format(
                    newdate, src_name, skip_schema_str=skip_schema_str)
            elif src_name and table_list:
                sql = "SELECT DISTINCT TRIM(SRC_STM_ID)||'.'||TRIM(TAB_CODE) FROM DSA.ORGIN_TABLE_DETAIL_T WHERE CHANGE_DATE={0} AND ALT_STS_TP = 'D' AND TRIM(SRC_STM_ID)='{1}' AND TRIM(TAB_CODE) IN ({2}) AND (SOURCE_TYPE = '0' OR (SOURCE_TYPE='1' AND IS_DW_T_F='1')){skip_schema_str}".format(
                    newdate, src_name, ','.join(["\'%s\'" % x for x in table_list]),
                    skip_schema_str=skip_schema_str)
            else:
                self._muti_outStream(u"异常:模式名为空且表名不为空")
                return -1, u'异常:模式名为空且表名不为空'
            rows = self._getResultList(sql)

            for row in rows:
                del_tablelist.append(row[0])
            del_set = set(del_tablelist)
        else:
            del_set = old_set - new_set

        if add_set:
            self._muti_outStream(u"新增表: %s\n\n" % ','.join(add_set))
            self.read_me_file.write(u"新增表: %s\n\n" % '\n'.join(add_set))
            self.deal_table_add(list(add_set), newdate)

        if del_set:
            self._muti_outStream(u"下线表: %s\n\n" % ','.join(del_set))
            self.read_me_file.write(u"下线表: %s\n\n" % '\n'.join(del_set))
            self.deal_table_del(list(del_set), newdate)

        deal_list = list(common_set)

        different_tables = []

        for table in deal_list:
            if len(table.split(".")) != 2:
                raise Exception("tablename error! %s" % table)
            syscode, tablenm = table.split('.')
            old_sql = "SELECT TRIM(FIELD_CODE),TRIM(DATA_TP)||','||TRIM(LENGTH)||','||TRIM(PRECSN),CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END FROM DSA.ORGIN_TABLE_DETAIL WHERE TRIM(SRC_STM_ID) ='{0}' AND TRIM(TAB_CODE) = '{1}' AND CHANGE_DATE='{2}' ORDER BY CAST(COLUMN_ID AS INT)".format(syscode, tablenm, olddate)

            old_rows = self._getResultList(old_sql)

            new_sql = "SELECT TRIM(FIELD_CODE),TRIM(DATA_TP)||','||TRIM(LENGTH)||','||TRIM(PRECSN),CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END FROM DSA.ORGIN_TABLE_DETAIL WHERE TRIM(SRC_STM_ID) ='{0}' AND TRIM(TAB_CODE) = '{1}' AND CHANGE_DATE='{2}' ORDER BY CAST(COLUMN_ID AS INT)".format(syscode, tablenm, newdate)

            new_rows = self._getResultList(new_sql)

            if old_rows != new_rows:
                different_tables.append(table)

        return 0, different_tables

    def common_deal(self, table, is_add_primary_column_flag, is_del_primary_column_flag, is_change_to_primary_column_flag, is_change_from_primary_column_flag, is_add_common_column_flag, is_del_common_column_flag, is_change_column_property_flag, olddate, newdate, primary_list):

        if len(table.split(".")) != 2:
            raise Exception("tablename error! %s" %table)
        syscode, tablenm = table.split('.')
        delta_tablename = "DELTA."+table.replace(".", "_")
        his_tablename = "ODSHIS."+table.replace(".", "_")

        if is_add_primary_column_flag:
            self.generate_ddl(table, type='delta', input_date=newdate)
            self.generate_ddl(table, type='ods', input_date=newdate)
            self.generate_ddl(table, type='odshis', input_date=newdate)
            self.generate_ap_sql_init(table, newdate)
            self.generate_ap_sql(table, newdate)

            self.alter_table_file.write('-- <add primary key>\n')
            self.alter_table_file.write("DROP TABLE {delta_tablename};\n".format(delta_tablename=delta_tablename))
            self.alter_table_file.write("rename table {tablename} to {tablenm}_{newdate};\n".format(tablename=table, tablenm=tablenm, newdate=newdate))
            self.alter_table_file.write("rename table {his_tablename} to {syscode}_{tablenm}_{newdate};\n".format(his_tablename=his_tablename, syscode=syscode, tablenm=tablenm, newdate=newdate))
            self.alter_table_file.write('-- <end>\n')

            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';\n")
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';\n")
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' where JOB_NM='LD_ODS_%s_INIT';\n" %(table.replace('.','_')))
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='W' where job_nm='LD_ODS_%s';\n" %(table.replace('.','_')))

            return

        if is_del_primary_column_flag:
            self.generate_ddl(table, type='delta', input_date=newdate)
            self.generate_ddl(table, type='ods', input_date=newdate)
            self.generate_ddl(table, type='odshis', input_date=newdate)
            self.generate_ap_sql_init(table, newdate)
            self.generate_ap_sql(table, newdate)

            self.alter_table_file.write('-- <del primary key>\n')
            self.alter_table_file.write("DROP TABLE {delta_tablename};\n".format(delta_tablename=delta_tablename))
            self.alter_table_file.write("rename table {tablename} to {tablenm}_{newdate};\n".format(tablename=table, tablenm=tablenm, newdate=newdate))
            self.alter_table_file.write("rename table {his_tablename} to {syscode}_{tablenm}_{newdate};\n".format(his_tablename=his_tablename, syscode=syscode, tablenm=tablenm, newdate=newdate))
            self.alter_table_file.write('-- <end>\n')

            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';\n")
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';\n")
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' where JOB_NM='LD_ODS_%s_INIT';\n" % (table.replace('.', '_')))
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='W' where job_nm='LD_ODS_%s';\n" % (table.replace('.', '_')))

            return

        if is_change_to_primary_column_flag:
            self.generate_ddl(table, type='delta', input_date=newdate)
            self.generate_ddl(table, type='ods', input_date=newdate)
            self.generate_ddl(table, type='odshis', input_date=newdate)
            self.generate_ap_sql_init(table, newdate)
            self.generate_ap_sql(table, newdate)

            self.alter_table_file.write('-- <change to primary key>\n')
            self.alter_table_file.write("DROP TABLE {delta_tablename};\n".format(delta_tablename=delta_tablename))
            self.alter_table_file.write("rename table {tablename} to {tablenm}_{newdate};\n".format(tablename=table, tablenm=tablenm, newdate=newdate))
            self.alter_table_file.write("rename table {his_tablename} to {syscode}_{tablenm}_{newdate};\n".format(his_tablename=his_tablename, syscode=syscode, tablenm=tablenm, newdate=newdate))
            self.alter_table_file.write('-- <end>\n')

            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';\n")
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';\n")
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' where JOB_NM='LD_ODS_%s_INIT';\n" % (table.replace('.', '_')))
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='W' where job_nm='LD_ODS_%s';\n" % (table.replace('.', '_')))

            return

        if is_change_from_primary_column_flag:
            self.generate_ddl(table, type='delta', input_date=newdate)
            self.generate_ddl(table, type='ods', input_date=newdate)
            self.generate_ddl(table, type='odshis', input_date=newdate)
            self.generate_ap_sql_init(table, newdate)
            self.generate_ap_sql(table, newdate)

            self.alter_table_file.write('-- <change from primary key>\n')
            self.alter_table_file.write("DROP TABLE {delta_tablename};\n".format(delta_tablename=delta_tablename))
            self.alter_table_file.write("rename table {tablename} to {tablenm}_{newdate};\n".format(tablename=table, tablenm=tablenm, newdate=newdate))
            self.alter_table_file.write("rename table {his_tablename} to {syscode}_{tablenm}_{newdate};\n".format(his_tablename=his_tablename, syscode=syscode, tablenm=tablenm, newdate=newdate))
            self.alter_table_file.write('-- <end>\n')

            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';\n")
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';\n")
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' where JOB_NM='LD_ODS_%s_INIT';\n" % (table.replace('.', '_')))
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='W' where job_nm='LD_ODS_%s';\n" % (table.replace('.', '_')))

            return

        if is_change_column_property_flag == 2:
            self.generate_ddl(table, type='delta', input_date=newdate)
            self.generate_ddl(table, type='ods', input_date=newdate)
            self.generate_ddl(table, type='odshis', input_date=newdate)

            self.alter_table_file.write('-- <change primary column type>\n')
            self.alter_table_file.write("DROP TABLE {delta_tablename};\n".format(delta_tablename=delta_tablename))
            self.alter_table_file.write(
                "rename table {tablename} to {tablenm}_{newdate};\n".format(tablename=table, tablenm=tablenm,
                                                                            newdate=newdate))
            self.alter_table_file.write(
                "rename table {his_tablename} to {syscode}_{tablenm}_{newdate};\n".format(his_tablename=his_tablename,
                                                                                          syscode=syscode,
                                                                                          tablenm=tablenm,
                                                                                          newdate=newdate))
            self.alter_table_file.write('-- <end>\n')

            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';\n")
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';\n")
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' where JOB_NM='LD_ODS_%s_INIT';\n" % (table.replace('.', '_')))
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='W' where job_nm='LD_ODS_%s';\n" % (table.replace('.', '_')))

            return

        if is_change_column_property_flag == 1:
            sql = '''
            SELECT A.FIELD_CODE, A.DATA_TP NEW_TP, B.DATA_TP OLD_TP, A.LENGTH NEW_LENGTH, B.LENGTH OLD_LENGTH, A.PRECSN NEW_PRECSN, B.PRECSN OLD_PRECSN, A.PRIMARY_FLAG FROM
            (SELECT TRIM(FIELD_CODE) FIELD_CODE, TRIM(DATA_TP) DATA_TP, TRIM(LENGTH) LENGTH, TRIM(PRECSN) PRECSN, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG
            FROM DSA.ORGIN_TABLE_DETAIL
            WHERE change_date = '{newdate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')A
            LEFT JOIN
            (SELECT TRIM(FIELD_CODE) FIELD_CODE, TRIM(DATA_TP) DATA_TP, TRIM(LENGTH) LENGTH, TRIM(PRECSN) PRECSN, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG
            FROM DSA.ORGIN_TABLE_DETAIL
            WHERE change_date = '{olddate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')B
            ON A.FIELD_CODE = B.FIELD_CODE AND A.PRIMARY_FLAG = B.PRIMARY_FLAG
            WHERE A.DATA_TP <> B.DATA_TP OR A.LENGTH <> B.LENGTH OR A.PRECSN <> B.PRECSN
            '''.format(olddate=olddate, newdate=newdate, syscode=syscode, tablenm=tablenm)

            rows = self._getResultList(sql)

            if rows:
                self.alter_table_file.write('-- <change property>\n')
                for row in rows:
                    field_code, new_type, old_type, new_length, old_length, new_precsn, old_precsn, primary = row

                    if new_type == "CHARACTER":
                        new_filed_line = 'CHARACTER(' +new_length+ ')'
                    elif new_type == "CHAR":
                        new_filed_line = 'CHAR(' +new_length+ ')'
                    elif new_type == "DECIMAL":
                        new_filed_line = 'DECIMAL(' +new_length+','+new_precsn+ ')'
                    elif new_type == "VARCHAR":
                        new_filed_line = 'VARCHAR(' +new_length+ ')'
                    else:
                        new_filed_line = new_type

                    if old_type == "CHARACTER":
                        old_filed_line = 'CHARACTER(' +old_length+ ')'
                    elif old_type == "CHAR":
                        old_filed_line = 'CHAR(' +old_length+ ')'
                    elif old_type == "DECIMAL":
                        old_filed_line = 'DECIMAL(' +old_length+','+old_precsn+ ')'
                    elif old_type == "VARCHAR":
                        old_filed_line = 'VARCHAR(' +old_length+ ')'
                    else:
                        old_filed_line = old_type

                    self.alter_table_file.write("alter table {0} alter column {1} set data type {2};--old type:{3}\n".format(table, field_code, new_filed_line, old_filed_line))

                    self.alter_table_file.write("REORG TABLE "+table+';\n')

                    self.alter_table_file.write("alter table {0} alter column {1} set data type {2};--old type:{3}\n".format(his_tablename, field_code, new_filed_line, old_filed_line))

                    self.alter_table_file.write("REORG TABLE "+his_tablename+';\n')

                self.alter_table_file.write("REORG TABLE "+table+';\n')
                self.alter_table_file.write("REORG TABLE "+his_tablename+';\n')
                #self.alter_table_file.write("REORG TABLE "+delta_tablename+';\n')
                self.alter_table_file.write('-- <end>\n')

        if is_add_common_column_flag:
            add_column_list = []

            sql = '''
            SELECT A.FIELD_CODE, A.DATA_TP, A.LENGTH, A.PRECSN, A.FIELD_NM FROM
            (SELECT TRIM(FIELD_CODE) FIELD_CODE, TRIM(DATA_TP) DATA_TP, TRIM(LENGTH) LENGTH, TRIM(PRECSN) PRECSN, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG, TRIM(FIELD_NM) FIELD_NM FROM DSA.ORGIN_TABLE_DETAIL WHERE change_date = '{newdate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')A
            LEFT JOIN
            (SELECT TRIM(FIELD_CODE) FIELD_CODE FROM DSA.ORGIN_TABLE_DETAIL WHERE change_date = '{olddate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')B
            ON A.FIELD_CODE = B.FIELD_CODE
            WHERE B.FIELD_CODE IS NULL AND A.PRIMARY_FLAG = 'N'
            '''.format(olddate=olddate, newdate=newdate, syscode=syscode, tablenm=tablenm)
            print(sql)
            rows = self._getResultList(sql)

            if rows:
                self.alter_table_file.write('-- <add column>\n')
            for row in rows:
                field_code, filed_type, length, precsn, field_comment = row

                add_column_list.append(field_code)

                field_comment = self._none_to_string(field_comment).replace('\'', '\'\'').replace('\"', '\\\"')

                if filed_type == "CHARACTER":
                    filed_line = field_code +'\t'+'CHARACTER(' +length+ ')'
                elif filed_type == "CHAR":
                    filed_line = field_code +'\t'+'CHAR(' +length+ ')'
                elif filed_type == "DECIMAL":
                    filed_line = field_code +'\t'+'DECIMAL(' +length+','+precsn+ ')'
                elif filed_type == "VARCHAR":
                    filed_line = field_code +'\t'+'VARCHAR(' +length+ ')'
                else:
                    filed_line = field_code +'\t'+ filed_type

                self.alter_table_file.write("alter table {tablename} add column {filed_line};\n".format(tablename=table, filed_line=filed_line))

                self.alter_table_file.write("REORG TABLE {tablename};\n".format(tablename=table))

                self.alter_table_file.write("Comment on Column "+table+'.'+field_code+'\tis \''+field_comment+'\';\n')

                self.alter_table_file.write("alter table {his_tablename} add column {filed_line};\n".format(his_tablename=his_tablename, filed_line=filed_line))

                self.alter_table_file.write("REORG TABLE {his_tablename};\n".format(his_tablename=his_tablename))

                self.alter_table_file.write("Comment on Column "+his_tablename+'.'+field_code+'\tis \''+field_comment+'\';\n')

            if rows:
                self.alter_table_file.write("REORG TABLE {tablename};\n".format(tablename=table))
                self.alter_table_file.write("REORG TABLE {his_tablename};\n".format(his_tablename=his_tablename))
                self.alter_table_file.write('CREATE TABLE {delta_tablename}_YATOPUPDATE LIKE {delta_tablename};\n'.format(delta_tablename=delta_tablename))
                '''
                生成调度信息
                '''
                self._muti_outStream(u'生成调度信息 %s\n' %table)

                self.job_schedule_file.write("--add table columns\n")

                tablestr = table.replace('.', '_')

                self.job_schedule_file.write("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('AP_ODS_%s_YATOPUPDATE','DAY','SQL','L_ODS','AP_ODS_%s_YATOPUPDATE.SQL','EDW /etl/etldata/script/odssql','5','1','AP_ODS_%s_YATOPUPDATE','U',CURRENT TIMESTAMP,'1','1','%s','','%s');\n" %(tablestr, tablestr, tablestr, syscode, config.IP))
                self.job_schedule_file.write("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s_YATOPUPDATE','LD_ODS_%s',CURRENT TIMESTAMP);\n" %(tablestr, tablestr))
                self.job_schedule_file.write("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s_YATOPUPDATE','UNCOMPRESS_INIT',CURRENT TIMESTAMP);\n" %tablestr)
                self.job_schedule_file.write("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s','AP_ODS_%s_YATOPUPDATE',CURRENT TIMESTAMP);\n" %(tablestr, tablestr))

                self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='AP_ODS_%s_YATOPUPDATE';\n" %tablestr)
                self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';\n")
                self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';\n")

                self.generate_ap_sql_yatopupdate(table, add_column_list, primary_list, newdate)

        if is_del_common_column_flag:
            pass


    def is_add_primary_column(self, table, is_flow_table_flag, primary_list, olddate, newdate):
        """
        判断是否为新增主键字段, 并处理
        """
        self._muti_outStream(u"\n%s 新增主键字段的处理: \n" %table)

        if len(table.split(".")) != 2:
            raise Exception("tablename error! %s" %table)
        syscode, tablenm = table.split('.')

        delta_tablename = "DELTA."+table.replace(".", "_")
        his_tablename = "ODSHIS."+table.replace(".", "_")

        sql = '''
        SELECT A.FIELD_CODE, A.DATA_TP, A.LENGTH, A.PRECSN, A.PRIMARY_FLAG FROM
        (SELECT TRIM(FIELD_CODE) FIELD_CODE, TRIM(DATA_TP) DATA_TP, TRIM(LENGTH) LENGTH, TRIM(PRECSN) PRECSN, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG FROM DSA.ORGIN_TABLE_DETAIL WHERE change_date = '{newdate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')A
        LEFT JOIN
        (SELECT TRIM(FIELD_CODE) FIELD_CODE FROM DSA.ORGIN_TABLE_DETAIL WHERE change_date = '{olddate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')B
        ON A.FIELD_CODE = B.FIELD_CODE
        WHERE B.FIELD_CODE IS NULL AND A.PRIMARY_FLAG = 'Y'
        '''.format(olddate=olddate, newdate=newdate, syscode=syscode, tablenm=tablenm)

        rows = self._getResultList(sql)

        for row in rows:
            self._muti_outStream(u'{tablename} 新增主键字段 {columnname}\n'.format(tablename=table, columnname=row[0]))
            self.read_me_file.write(u'{tablename} 新增主键字段 {columnname}\n'.format(tablename=table, columnname=row[0]))

        return True if rows else False

    def is_del_primary_column(self, table, is_flow_table_flag, primary_list, olddate, newdate):
        """
        判断是否为删除主键字段, 并处理
        """
        self._muti_outStream(u"\n%s 删除主键字段的处理: \n" %(table))

        if len(table.split(".")) != 2:
            raise Exception("tablename error! %s" %table)
        syscode, tablenm = table.split('.')

        delta_tablename = "DELTA."+table.replace(".", "_")
        his_tablename = "ODSHIS."+table.replace(".", "_")

        sql = '''
        SELECT A.FIELD_CODE, A.DATA_TP, A.LENGTH, A.PRECSN, A.PRIMARY_FLAG FROM
        (SELECT TRIM(FIELD_CODE) FIELD_CODE, TRIM(DATA_TP) DATA_TP, TRIM(LENGTH) LENGTH, TRIM(PRECSN) PRECSN, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG
        FROM DSA.ORGIN_TABLE_DETAIL
        WHERE change_date = '{olddate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')A
        LEFT JOIN
        (SELECT TRIM(FIELD_CODE) FIELD_CODE
        FROM DSA.ORGIN_TABLE_DETAIL
        WHERE change_date = '{newdate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')B
        ON A.FIELD_CODE = B.FIELD_CODE
        WHERE B.FIELD_CODE IS NULL AND A.PRIMARY_FLAG = 'Y'
        '''.format(olddate=olddate, newdate=newdate, syscode=syscode, tablenm=tablenm)

        rows = self._getResultList(sql)

        for row in rows:
            self._muti_outStream(u'{tablename} 删除主键字段 {columnname}\n'.format(tablename=table, columnname=row[0]))
            self.read_me_file.write(u'{tablename} 删除主键字段 {columnname}\n'.format(tablename=table, columnname=row[0]))

        return True if rows else False

    def is_change_to_primary_column(self, table, is_flow_table_flag, primary_list, olddate, newdate):
        """
        判断字段是否变为主键
        """
        self._muti_outStream(u"\n%s 字段变为主键的处理: \n" %(table))

        if len(table.split(".")) != 2:
            raise Exception("tablename error! %s" %table)
        syscode, tablenm = table.split('.')

        delta_tablename = "DELTA."+table.replace(".", "_")
        his_tablename = "ODSHIS."+table.replace(".", "_")

        sql = '''
        SELECT A.FIELD_CODE, A.DATA_TP, A.LENGTH, A.PRECSN, A.PRIMARY_FLAG FROM
        (SELECT TRIM(FIELD_CODE) FIELD_CODE, TRIM(DATA_TP) DATA_TP, TRIM(LENGTH) LENGTH, TRIM(PRECSN) PRECSN, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG
        FROM DSA.ORGIN_TABLE_DETAIL
        WHERE change_date = '{newdate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')A
        LEFT JOIN
        (SELECT TRIM(FIELD_CODE) FIELD_CODE, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG
        FROM DSA.ORGIN_TABLE_DETAIL
        WHERE change_date = '{olddate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')B
        ON A.FIELD_CODE = B.FIELD_CODE
        WHERE A.PRIMARY_FLAG = 'Y' AND B.PRIMARY_FLAG = 'N'
        '''.format(olddate=olddate, newdate=newdate, syscode=syscode, tablenm=tablenm)

        rows = self._getResultList(sql)

        for row in rows:
            self._muti_outStream(u'{tablename} 字段 {columnname} 变为主键 \n'.format(tablename=table, columnname=row[0]))
            self.read_me_file.write(u'{tablename} 字段 {columnname} 变为主键 \n'.format(tablename=table, columnname=row[0]))

        return True if rows else False

    def is_change_from_primary_column(self, table, is_flow_table_flag, primary_list, olddate, newdate):
        """
        判断字段是否变为非主键
        """
        self._muti_outStream(u"\n%s 字段变为非主键的处理: \n" %(table))

        if len(table.split(".")) != 2:
            raise Exception("tablename error! %s" %table)
        syscode, tablenm = table.split('.')

        delta_tablename = "DELTA."+table.replace(".", "_")
        his_tablename = "ODSHIS."+table.replace(".", "_")

        sql = '''
        SELECT A.FIELD_CODE, A.DATA_TP, A.LENGTH, A.PRECSN, A.PRIMARY_FLAG FROM
        (SELECT TRIM(FIELD_CODE) FIELD_CODE, TRIM(DATA_TP) DATA_TP, TRIM(LENGTH) LENGTH, TRIM(PRECSN) PRECSN, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG
        FROM DSA.ORGIN_TABLE_DETAIL
        WHERE change_date = '{newdate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')A
        LEFT JOIN
        (SELECT TRIM(FIELD_CODE) FIELD_CODE, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG
        FROM DSA.ORGIN_TABLE_DETAIL
        WHERE change_date = '{olddate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')B
        ON A.FIELD_CODE = B.FIELD_CODE
        WHERE A.PRIMARY_FLAG = 'N' AND B.PRIMARY_FLAG = 'Y'
        '''.format(olddate=olddate, newdate=newdate, syscode=syscode, tablenm=tablenm)

        rows = self._getResultList(sql)

        for row in rows:
            self._muti_outStream(u'{tablename} 字段 {columnname} 变为非主键 \n'.format(tablename=table, columnname=row[0]))
            self.read_me_file.write(u'{tablename} 字段 {columnname} 变为非主键 \n'.format(tablename=table, columnname=row[0]))

        return True if rows else False

    def is_add_common_column(self, table, is_flow_table_flag, primary_list, olddate, newdate):
        """
        判断是否新增普通字段
        """
        self._muti_outStream(u"\n%s 新增普通字段的处理: \n" %(table))

        add_column_list = []
        if len(table.split(".")) != 2:
            raise Exception("tablename error! %s" %table)
        syscode, tablenm = table.split('.')
        delta_tablename = "DELTA."+table.replace(".", "_")
        his_tablename = "ODSHIS."+table.replace(".", "_")

        sql = '''
        SELECT A.FIELD_CODE, A.DATA_TP, A.LENGTH, A.PRECSN, A.FIELD_NM FROM
        (SELECT TRIM(FIELD_CODE) FIELD_CODE, TRIM(DATA_TP) DATA_TP, TRIM(LENGTH) LENGTH, TRIM(PRECSN) PRECSN, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG, TRIM(FIELD_NM) FIELD_NM FROM DSA.ORGIN_TABLE_DETAIL WHERE change_date = '{newdate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')A
        LEFT JOIN
        (SELECT TRIM(FIELD_CODE) FIELD_CODE FROM DSA.ORGIN_TABLE_DETAIL WHERE change_date = '{olddate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')B
        ON A.FIELD_CODE = B.FIELD_CODE
        WHERE B.FIELD_CODE IS NULL AND A.PRIMARY_FLAG = 'N'
        '''.format(olddate=olddate, newdate=newdate, syscode=syscode, tablenm=tablenm)

        rows = self._getResultList(sql)

        for row in rows:
            field_code, filed_type, length, precsn, field_comment = row

            add_column_list.append(field_code)

            field_comment = self._none_to_string(field_comment).replace('\'', '\'\'').replace('\"', '\\\"')

            if filed_type == "CHARACTER":
                filed_line = field_code +'\t'+'CHARACTER(' +length+ ')'
            elif filed_type == "CHAR":
                filed_line = field_code +'\t'+'CHAR(' +length+ ')'
            elif filed_type == "DECIMAL":
                filed_line = field_code +'\t'+'DECIMAL(' +length+','+precsn+ ')'
            elif filed_type == "VARCHAR":
                filed_line = field_code +'\t'+'VARCHAR(' +length+ ')'
            else:
                filed_line = field_code +'\t'+ filed_type

            self._muti_outStream(u'{tablename} 新增普通字段 {columnname}\n'.format(tablename=table, columnname=filed_line))
            self.read_me_file.write(u'{tablename} 新增普通字段 {columnname}\n'.format(tablename=table, columnname=filed_line))

        return True if rows else False

    def is_del_common_column(self, table, is_flow_table_flag, primary_list, olddate, newdate):
        """
        判断是否删除普通字段
        """
        self._muti_outStream(u"\n%s 删除普通字段的处理: \n" %(table))

        if len(table.split(".")) != 2:
            raise Exception("tablename error! %s" %table)
        syscode, tablenm = table.split('.')

        delta_tablename = "DELTA."+table.replace(".", "_")

        sql = '''
        SELECT A.FIELD_CODE FROM
        (SELECT TRIM(FIELD_CODE) FIELD_CODE, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG
        FROM DSA.ORGIN_TABLE_DETAIL
        WHERE change_date = '{olddate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')A
        LEFT JOIN
        (SELECT TRIM(FIELD_CODE) FIELD_CODE
        FROM DSA.ORGIN_TABLE_DETAIL
        WHERE change_date = '{newdate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')B
        ON A.FIELD_CODE = B.FIELD_CODE
        WHERE B.FIELD_CODE IS NULL AND A.PRIMARY_FLAG = 'N'
        '''.format(olddate=olddate, newdate=newdate, syscode=syscode, tablenm=tablenm)

        rows = self._getResultList(sql)

        for row in rows:
            self._muti_outStream(u"%s 表删除普通字段 %s\n" %(table, row[0]))
            self.read_me_file.write(u"%s 表删除普通字段 %s\n" %(table, row[0]))

        return True if rows else False

    def is_change_column_property(self, table, olddate, newdate):
        """
        判断是否为字段属性的变更
        """
        self._muti_outStream(u"\n%s 字段属性变更的处理: \n" %(table))

        if len(table.split(".")) != 2:
            raise Exception("tablename error! %s" %table)
        syscode, tablenm = table.split('.')

        delta_tablename = "DELTA."+table.replace(".", "_")
        his_tablename = "ODSHIS."+table.replace(".", "_")

        sql = '''
        SELECT A.FIELD_CODE, A.DATA_TP NEW_TP, B.DATA_TP OLD_TP, A.LENGTH NEW_LENGTH, B.LENGTH OLD_LENGTH, A.PRECSN NEW_PRECSN, B.PRECSN OLD_PRECSN, A.PRIMARY_FLAG FROM
        (SELECT TRIM(FIELD_CODE) FIELD_CODE, CASE WHEN TRIM(DATA_TP) = 'CHARACTER' THEN 'CHAR' ELSE TRIM(DATA_TP) END DATA_TP, TRIM(LENGTH) LENGTH, TRIM(PRECSN) PRECSN, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG
        FROM DSA.ORGIN_TABLE_DETAIL
        WHERE change_date = '{newdate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')A
        LEFT JOIN
        (SELECT TRIM(FIELD_CODE) FIELD_CODE, CASE WHEN TRIM(DATA_TP) = 'CHARACTER' THEN 'CHAR' ELSE TRIM(DATA_TP) END DATA_TP, TRIM(LENGTH) LENGTH, TRIM(PRECSN) PRECSN, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG
        FROM DSA.ORGIN_TABLE_DETAIL
        WHERE change_date = '{olddate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')B
        ON A.FIELD_CODE = B.FIELD_CODE AND A.PRIMARY_FLAG = B.PRIMARY_FLAG
        WHERE (A.DATA_TP <> B.DATA_TP OR A.LENGTH <> B.LENGTH OR A.PRECSN <> B.PRECSN )
        AND (A.DATA_TP <> 'TIMESTAMP' AND A.DATA_TP <> 'DATE' AND A.DATA_TP <> 'INTEGER' AND A.DATA_TP <> 'SMALLINT' AND A.DATA_TP <> 'TIME' AND A.DATA_TP <> 'BIGINT')
UNION ALL
        SELECT A.FIELD_CODE, A.DATA_TP NEW_TP, B.DATA_TP OLD_TP, A.LENGTH NEW_LENGTH, B.LENGTH OLD_LENGTH, A.PRECSN NEW_PRECSN, B.PRECSN OLD_PRECSN, A.PRIMARY_FLAG FROM
        (SELECT TRIM(FIELD_CODE) FIELD_CODE, TRIM(DATA_TP) AS DATA_TP, TRIM(LENGTH) LENGTH, TRIM(PRECSN) PRECSN, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG
        FROM DSA.ORGIN_TABLE_DETAIL
        WHERE change_date = '{newdate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')A
        LEFT JOIN
        (SELECT TRIM(FIELD_CODE) FIELD_CODE, TRIM(DATA_TP) AS DATA_TP, TRIM(LENGTH) LENGTH, TRIM(PRECSN) PRECSN, CASE WHEN PRIMARY_KEY_FLAG='' THEN 'N' ELSE 'Y' END PRIMARY_FLAG
        FROM DSA.ORGIN_TABLE_DETAIL
        WHERE change_date = '{olddate}' AND TRIM(SRC_STM_ID) = '{syscode}' AND TRIM(TAB_CODE) = '{tablenm}')B
        ON A.FIELD_CODE = B.FIELD_CODE AND A.PRIMARY_FLAG = B.PRIMARY_FLAG
        WHERE (A.DATA_TP <> B.DATA_TP )
        AND (A.DATA_TP = 'TIMESTAMP' OR A.DATA_TP = 'DATE' OR A.DATA_TP = 'INTEGER' OR A.DATA_TP = 'SMALLINT' OR A.DATA_TP = 'TIME' OR A.DATA_TP = 'BIGINT')

        '''.format(olddate=olddate, newdate=newdate, syscode=syscode, tablenm=tablenm)

        rows = self._getResultList(sql)

        primary_flag = 1

        if rows:
            for row in rows:
                field_code, new_type, old_type, new_length, old_length, new_precsn, old_precsn, primary = row

                if primary == 'Y':
                    primary_flag = 2

                if new_type == "CHARACTER":
                    new_filed_line = 'CHARACTER(' +new_length+ ')'
                elif new_type == "CHAR":
                    new_filed_line = 'CHAR(' +new_length+ ')'
                elif new_type == "DECIMAL":
                    new_filed_line = 'DECIMAL(' +new_length+','+new_precsn+ ')'
                elif new_type == "VARCHAR":
                    new_filed_line = 'VARCHAR(' +new_length+ ')'
                else:
                    new_filed_line = new_type

                if old_type == "CHARACTER":
                    old_filed_line = 'CHARACTER(' +old_length+ ')'
                elif old_type == "CHAR":
                    old_filed_line = 'CHAR(' +old_length+ ')'
                elif old_type == "DECIMAL":
                    old_filed_line = 'DECIMAL(' +old_length+','+old_precsn+ ')'
                elif old_type == "VARCHAR":
                    old_filed_line = 'VARCHAR(' +old_length+ ')'
                else:
                    old_filed_line = old_type

                self._muti_outStream(u'%s.%s 字段属性变更 旧:%s  新:%s 是否为主键:%s\n' %(table, field_code, old_filed_line, new_filed_line, primary))
                self.read_me_file.write(u'%s.%s 字段属性变更 旧:%s  新:%s 是否为主键:%s\n' %(table, field_code, old_filed_line, new_filed_line, primary))

            return primary_flag
        return False

    def find_all_changes(self, olddate, newdate, different_tables):
        """
        找到一张表中的所有字段变更情况, 包括新增、删除、更新
        """

        rebuild_delta_tblist = []   ## 保证delta表只被重建一次

        for table in different_tables:

            # 查找受影响的作业
            self._getInfluencedJob(table)

            is_flow_table_flag = self.is_flow_table(table, newdate)
            primary_list = self.is_primary_table(table, newdate)

            is_add_primary_column_flag = self.is_add_primary_column(table, is_flow_table_flag, primary_list, olddate, newdate) # 新增主键字段

            is_del_primary_column_flag = self.is_del_primary_column(table, is_flow_table_flag, primary_list, olddate, newdate)  # 删除主键字段

            is_change_to_primary_column_flag =  self.is_change_to_primary_column(table, is_flow_table_flag, primary_list, olddate, newdate) # 字段变为主键

            is_change_from_primary_column_flag = self.is_change_from_primary_column(table, is_flow_table_flag, primary_list, olddate, newdate) # 字段变为非主键

            is_add_common_column_flag = self.is_add_common_column(table, is_flow_table_flag, primary_list, olddate, newdate) # 增加普通字段的处理

            is_del_common_column_flag = self.is_del_common_column(table, is_flow_table_flag, primary_list, olddate, newdate) # 删除普通字段的处理

            is_change_column_property_flag = self.is_change_column_property(table, olddate, newdate) # 字段属性变更的处理

            self.common_deal(table, is_add_primary_column_flag, is_del_primary_column_flag, is_change_to_primary_column_flag, is_change_from_primary_column_flag, is_add_common_column_flag, is_del_common_column_flag, is_change_column_property_flag, olddate, newdate, primary_list)

            # 新增普通字段\删除普通字段\更改普通字段属性 只需要重建一次delta表
            if (is_add_common_column_flag or is_del_common_column_flag or is_change_column_property_flag == 1) and not (is_add_primary_column_flag or is_del_primary_column_flag or is_change_to_primary_column_flag or is_change_from_primary_column_flag or is_change_column_property_flag == 2):
                if is_del_common_column_flag:
                    self.alter_table_file.write("-- <del column>\n")
                self.generate_ddl(table, type='delta', input_date=newdate)
                self.alter_table_file.write("DROP TABLE {delta_tablename};\n".format(delta_tablename="DELTA."+table.replace(".", "_")))

                self.generate_ap_sql_init(table, newdate)
                self.generate_ap_sql(table, newdate)

                self.alter_table_file.write("-- <end>\n")

    def is_distribute_all(self, input_date, flag):
        if flag:
            if config.isLinuxSystem():

                filelist = glob.glob(config.all_package_path + input_date + '*')
                if glob.glob(config.all_package_path + input_date + '全量包说明*'):
                    quanliang_flag = 1
                else:
                    quanliang_flag = 0

            else:
                f = codecs.open('content.txt', 'r', encoding='gb18030')
                data = f.read()

                reg = re.compile("/S-999000/ODS/ALL/"+input_date+"/")
                result = re.findall(reg, data)
                if result:
                    self._muti_outStream("这次变更为全量\n")
                    quanliang_flag = 1
                else:
                    self._muti_outStream("这次变更不为全量\n")
                    quanliang_flag = 0

            if quanliang_flag == 0:
                self.job_schedule_file.write("UPDATE ETL.JOB_METADATA set INIT_FLAG ='Y' WHERE JOB_NM LIKE '%_INIT';\n")
                self.job_schedule_file.write("UPDATE ETL.JOB_METADATA set INIT_FLAG ='N' WHERE INIT_FLAG= 'W';\n")
                self.job_schedule_file.write("UPDATE ETL.JOB_METADATA set INIT_FLAG ='U' WHERE JOB_NM LIKE '%_YATOPUPDATE';\n")

    def data_fix(self, input_date):

        readme_data = ""
        all_note_data = ""
        update_note_data = ""
        data_fix_list = []
        if os.path.exists(config.read_me_path.format(date=input_date)):
            with open(config.read_me_path.format(date=input_date), 'r') as f:
                readme_data = f.read()

        read_me_set = set(re.findall('\w+\.*\w+', readme_data)).union(set(re.findall('\w+', readme_data)))  # 需要把表名和模式名分开来, 不然过滤不了notelist中的不带模式名的表
        print "read_me_set: ", read_me_set

        notelist = glob.glob(config.all_package_path + input_date + '*')
        if not notelist:
            self.compare_log_file.write('无说明文件, 无数据修复\n')

        else:
            if os.path.exists(config.all_package_path + input_date + u'全量包说明'):
                with open(config.all_package_path + input_date + u'全量包说明', 'r') as f:
                    all_note_data = f.read()
            if os.path.exists(config.all_package_path + input_date + u'升级说明'):
                with open(config.all_package_path + input_date + u'升级说明', 'r') as f:
                    update_note_data = f.read()

        if read_me_set:
            data = (all_note_data + update_note_data).upper()
            print "data: ", data
            note_set = set(re.findall('\w+\.*\w+', data))
            print "note_set: " ,note_set
            tmp_datafix_set = note_set - read_me_set
            for table in tmp_datafix_set:
                print(len(table.split('.')))
                if len(table.split('.')) == 2:
                    syscode, tabnm = table.split('.')
                    sql = "SELECT COUNT(1) FROM SYSCAT.TABLES WHERE TABSCHEMA='{syscode}' AND TABNAME='{tabnm}'".format(
                        syscode=syscode, tabnm=tabnm)
                else:
                    tabnm = table
                    sql = "SELECT COUNT(1) FROM SYSCAT.TABLES WHERE TABNAME='{tabnm}'".format(
                        tabnm=tabnm)
                self.edw_cursor.execute(sql)
                rows = self.edw_cursor.fetchall()
                if rows[0][0]:
                    if len(table.split('.')) == 1:
                        sql = "SELECT TABSCHEMA,TABNAME FROM SYSCAT.TABLES WHERE TABNAME='{tabnm}'".format(tabnm=tabnm)
                        self.edw_cursor.execute(sql)
                        rows = self.edw_cursor.fetchall()
                        syscode, tabnm = rows[0]
                        table = syscode + '.' + tabnm
                    print '%s 表需要数据修复\n' % table
                    self.compare_log_file.write('%s 表需要数据修复\n' % table)
                    data_fix_list.append(table)
                else:
                    print '由于库中不存在%s表,不需要数据修复\n' % table
        else:
            print "read_me_set 为空"

        if data_fix_list:
            for table in data_fix_list:
                if len(table.split(".")) != 2:
                    raise Exception("tablename error! %s" % table)
                syscode, tablenm = table.split('.')
                sql = "UPDATE DSA.ORGIN_TABLE_DETAIL_T SET ALT_STS_TP='F' WHERE SRC_STM_ID='{syscode}' AND TAB_CODE = '{tablenm}' AND CHANGE_DATE = '{date}'".format(
                    date=input_date, tablenm=tablenm, syscode=syscode)

                self.dwmm_cursur.execute(sql)
                self.dwmm_cursur.commit()

        sql = "SELECT SRC_STM_ID, TAB_CODE FROM DSA.ORGIN_TABLE_DETAIL_T WHERE ALT_STS_TP='F' AND CHANGE_DATE = '{date}'".format(date=input_date)
        rows = self._getResultList(sql)

        if rows:

            self.read_me_file.write("\n数据修复:\n")
            self.job_schedule_file.write("--data fix\n")
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';\n")
            self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';\n")

            for row in rows:
                syscode, tablenm = row
                print("库中需要数据修复表: {syscode}.{tablenm}\n".format(syscode=syscode, tablenm=tablenm))
                table = syscode.strip() + '.' + tablenm.strip()
                his_tablename = "ODSHIS."+table.replace(".", "_")
                self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='LD_ODS_{syscode}_{tablenm}_INIT';\n".format(syscode=syscode, tablenm=tablenm))
                self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='AP_ODS_{syscode}_{tablenm}_INIT';\n".format(syscode=syscode, tablenm=tablenm))
                self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='W' WHERE JOB_NM ='AP_ODS_{syscode}_{tablenm}';\n".format(syscode=syscode, tablenm=tablenm))
                self.job_schedule_file.write("UPDATE ETL.JOB_METADATA SET INIT_FLAG='W' WHERE JOB_NM ='LD_ODS_{syscode}_{tablenm}';\n".format(syscode=syscode, tablenm=tablenm))
                self.read_me_file.write("数据修复表: {syscode}.{tablenm}\n".format(syscode=syscode, tablenm=tablenm))


                self.alter_table_file.write("rename table {tablename} to {tablenm}_{newdate};\n".format(tablename=table, tablenm=tablenm, newdate=input_date))
                self.alter_table_file.write("rename table {his_tablename} to {syscode}_{tablenm}_{newdate};\n".format(his_tablename=his_tablename, syscode=syscode, tablenm=tablenm, newdate=input_date))

                self.generate_ddl(table, type='ods', input_date=input_date)
                self.generate_ddl(table, type='odshis', input_date=input_date)

    def display_notice(self, input_date):

        filelist = glob.glob(config.all_package_path + input_date.encode() + '*')

        for i in filelist:
            with open(i, 'r') as f:
                data = f.read()

            self.read_me_file.write('\n\n'+'*'*100 + '\n' + data + '\n')
            self.readMeAFile.write('\n\n'+'*'*100 + '\n' + data + '\n')

    def deal_init_job(self, src_name, table_name, flag):
        if not src_name and not table_name and flag == 'A':
            self.job_schedule_file.write("--初始化全量:\n")

            self.job_schedule_file.write("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,PLD_ST_DT,PLD_ST_TM,EXP_ED_DT,EXP_ED_TM,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,MAP_RULE_SET_ID,RSPL_PRT_ID,RSPL_PRT_NM,SCHD_ENGIN_IP) VALUES ('UNCOMPRESS_INIT','DAY','CMD','L_ODSLD','uncompress_init.sh','$dateid','5','1',null,null,null,null,'UNCOMPRESS_INIT','Y',current timestamp,'1','1','ODS','',null,null,null,'%s');\n" %config.IP)

            self.job_schedule_file.write("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,PLD_ST_DT,PLD_ST_TM,EXP_ED_DT,EXP_ED_TM,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,MAP_RULE_SET_ID,RSPL_PRT_ID,RSPL_PRT_NM,SCHD_ENGIN_IP) VALUES ('FTP_DOWNLOAD_INIT','DAY','CMD','L_ODSLD','ftp_all_file.sh','%s %s %s /etl/etldata/input/init %sODS/ALL/$dateid','5','1',null,null,null,null,'FTP_DOWNLOAD_INIT','Y',current timestamp,'1','1','ODS','',null,null,null,'%s');\n" %(config.distribute_server_ip, config.FTP_USER, config.FTP_PWD, config.distribute_server_path, config.IP))

            self.job_schedule_file.write("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,PLD_ST_DT,PLD_ST_TM,EXP_ED_DT,EXP_ED_TM,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,MAP_RULE_SET_ID,RSPL_PRT_ID,RSPL_PRT_NM,SCHD_ENGIN_IP) VALUES ('ODS_DONE','DAY','SQL','L_ODS','ODS_DONE.SQL','EDW /etl/etldata/script/odssql','5','1',null,null,null,null,'ODS_DONE','N',current timestamp,'1','1','ODS','',null,null,null,'%s');\n" %config.IP)

            self.job_schedule_file.write("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,PLD_ST_DT,PLD_ST_TM,EXP_ED_DT,EXP_ED_TM,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,MAP_RULE_SET_ID,RSPL_PRT_ID,RSPL_PRT_NM,SCHD_ENGIN_IP) VALUES ('ALL_DONE','DAY','SQL','L_ODS','ALL_DONE.SQL','EDW /etl/etldata/script/odssql','5','1',null,null,null,null,'ALL_DONE','N',current timestamp,'1','1','ODS','',null,null,null,'%s');\n" %config.IP)

            self.job_schedule_file.write("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,PLD_ST_DT,PLD_ST_TM,EXP_ED_DT,EXP_ED_TM,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,MAP_RULE_SET_ID,RSPL_PRT_ID,RSPL_PRT_NM,SCHD_ENGIN_IP) VALUES ('JOBSCHED_UPDATE','DAY','CMD','L_ODSLD','jobsched_update.sh','$dateid','5','1',null,null,null,null,'JOBSCHED_UPDATE','N',current timestamp,'1','1','ODS','',null,null,null,'%s');\n" %config.IP)

            self.job_schedule_file.write("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,PLD_ST_DT,PLD_ST_TM,EXP_ED_DT,EXP_ED_TM,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,MAP_RULE_SET_ID,RSPL_PRT_ID,RSPL_PRT_NM,SCHD_ENGIN_IP) VALUES ('CLEAR_JOB_LOG','DAY','CMD','L_ODSLD','clear_job_log.sh','$dateid','5','1',null,null,null,null,'CLEAR_JOB_LOG','N',current timestamp,'1','1','ODS','',null,null,null,'%s');\n" %config.IP)

            self.job_schedule_file.write("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,PLD_ST_DT,PLD_ST_TM,EXP_ED_DT,EXP_ED_TM,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,MAP_RULE_SET_ID,RSPL_PRT_ID,RSPL_PRT_NM,SCHD_ENGIN_IP) VALUES ('BRUSH_JOBS_LINSHI','DAY','CMD','L_ODSLD','brush_jobs.sh','$dateid','5','1',null,null,null,null,'BRUSH_JOBS_LINSHI','N',current timestamp,'1','1','ODS','',null,null,null,'%s');\n" %config.IP)

            self.job_schedule_file.write("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,PLD_ST_DT,PLD_ST_TM,EXP_ED_DT,EXP_ED_TM,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,MAP_RULE_SET_ID,RSPL_PRT_ID,RSPL_PRT_NM,SCHD_ENGIN_IP) VALUES ('AUTO_ALTER_ODS_LINSHI','DAY','CMD','L_ODSLD','auto_alter_ods.sh','$dateid','5','1',null,null,null,null,'AUTO_ALTER_ODS_LINSHI','X',current timestamp,'1','1','ODS','',null,null,null,'%s');\n" %config.IP)

            self.job_schedule_file.write("insert into etl.job_metadata (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,PLD_ST_DT,PLD_ST_TM,EXP_ED_DT,EXP_ED_TM,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,MAP_RULE_SET_ID,RSPL_PRT_ID,RSPL_PRT_NM,SCHD_ENGIN_IP) values ('TERMLY_DATA_CLEAR','DAY','CMD','L_ODSLD','termly_data_clear.sh','$dateid','5','1',null,null,null,null,'TERMLY_DATA_CLEAR','N',current timestamp,'1','1','ODS','',null,null,null,'%s');\n" %config.IP)

            self.job_schedule_file.write("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,PLD_ST_DT,PLD_ST_TM,EXP_ED_DT,EXP_ED_TM,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,MAP_RULE_SET_ID,RSPL_PRT_ID,RSPL_PRT_NM,SCHD_ENGIN_IP) VALUES ('SMY_DONE','DAY','SQL','L_ODS','SMY_DONE.SQL','EDW /etl/etldata/script/odssql','5','1',null,null,null,null,'SMY_DONE','N',current timestamp,'1','1','ODS','',null,null,null,'%s');\n" %config.IP)

            self.job_schedule_file.write("insert into etl.job_metadata (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,PLD_ST_DT,PLD_ST_TM,EXP_ED_DT,EXP_ED_TM,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,MAP_RULE_SET_ID,RSPL_PRT_ID,RSPL_PRT_NM,SCHD_ENGIN_IP) values ('DQ_ODS_GL_BAL_CHK','DAY','SP','L_SMY','DQ.ODS_GL_BAL_CHK','EDW|$dateid|','5','1',null,null,null,null,'DQ_ODS_GL_BAL_CHK','N',current timestamp,'1','1','ODS','',null,null,null,'%s');\n" %config.IP)

            self.job_schedule_file.write(
                "update etl.job_metadata set init_flag ='N' WHERE INIT_FLAG ='W';\n")
            self.job_schedule_file.write(
                "update etl.job_metadata set init_flag ='Y' WHERE JOB_NM LIKE '%_INIT' and init_flag <>'X' and init_flag <> 'Y';\n")
            self.job_schedule_file.write(
                "INSERT INTO ETL.JOB_SEQ VALUES ('ALL_DONE','ODS_DONE',NULL,CURRENT TIMESTAMP);\n")
            self.job_schedule_file.write(
                "INSERT INTO ETL.JOB_SEQ VALUES ('ALL_DONE','DQ_ODS_GL_BAL_CHK',NULL,CURRENT TIMESTAMP);\n")
            self.job_schedule_file.write(
                "INSERT INTO ETL.JOB_SEQ VALUES ('ALL_DONE','SMY_DONE',NULL,CURRENT TIMESTAMP);\n")
            self.job_schedule_file.write(
                "INSERT INTO ETL.JOB_SEQ VALUES ('AUTO_ALTER_ODS_LINSHI','JOBSCHED_UPDATE',NULL,CURRENT TIMESTAMP);\n")
            self.job_schedule_file.write(
                "INSERT INTO ETL.JOB_SEQ VALUES ('BRUSH_JOBS_LINSHI','AUTO_ALTER_ODS_LINSHI',NULL,CURRENT TIMESTAMP);\n")
            self.job_schedule_file.write(
                "INSERT INTO ETL.JOB_SEQ VALUES ('BRUSH_JOBS_LINSHI','CLEAR_JOB_LOG',NULL,CURRENT TIMESTAMP);\n")
            self.job_schedule_file.write(
                "INSERT INTO ETL.JOB_SEQ VALUES ('CLEAR_JOB_LOG','ALL_DONE',NULL,CURRENT TIMESTAMP);\n")
            self.job_schedule_file.write(
                "INSERT INTO ETL.JOB_SEQ VALUES ('DQ_ODS_GL_BAL_CHK','ODS_DONE',NULL,CURRENT TIMESTAMP);\n")
            self.job_schedule_file.write(
                "INSERT INTO ETL.JOB_SEQ VALUES ('JOBSCHED_UPDATE','ALL_DONE',NULL,CURRENT TIMESTAMP);\n")
            self.job_schedule_file.write(
                "INSERT INTO ETL.JOB_SEQ VALUES ('TERMLY_DATA_CLEAR','BRUSH_JOBS_LINSHI',NULL,CURRENT TIMESTAMP);\n")
            self.job_schedule_file.write(
                "INSERT INTO ETL.JOB_SEQ VALUES ('UNCOMPRESS_INIT','FTP_DOWNLOAD_INIT',NULL,CURRENT TIMESTAMP);\n")

            if not os.path.exists('/etl/etldata/script/odssql/ODS_DONE.SQL'):
                with open('/etl/etldata/script/odssql/ODS_DONE.SQL','w') as f:
                    pass
            if not os.path.exists('/etl/etldata/script/odssql/ALL_DONE.SQL'):
                with open('/etl/etldata/script/odssql/ALL_DONE.SQL','w') as f:
                    pass
            if not os.path.exists('/etl/etldata/script/odssql/SMY_DONE.SQL'):
                with open('/etl/etldata/script/odssql/SMY_DONE.SQL','w') as f:
                    pass

    @get_return_data
    def main(self, input_dict):
        try:
            input_date = input_dict.get('date')
            src_name = input_dict.get('srcName')
            table_name = input_dict.get('tableName')

            if not input_date:
                print(u"Warning !!!  输入日期为空\n")
                time.sleep(2)
                sql = "SELECT DISTINCT CHANGE_DATE FROM DSA.ORGIN_TABLE_DETAIL ORDER BY CHANGE_DATE"

                rows = self._getResultList(sql)

                date_list = []
                for date in rows:
                    date_list.append(date[0])

                date_list = date_list[-2:]
                flag = 'D'
            else:
                input_date, flag = input_date.split('_')

                return_dict = self._get_date_list(input_date, flag)
                print "return_dict:" ,return_dict

                if return_dict.get('returnCode') == 400:
                    return -1, return_dict.get('returnMsg')

                date_list = return_dict.get('returnMsg')

            # 初始化生成文件
            self._init_log_file(date_list[1])
            print(date_list)
            print("------------------------flag%s: " %flag)
            self._muti_outStream(u"输入日期:%s,%s\n" %(date_list[0], date_list[1]))

            if table_name:
                if len(table_name.split(".")) != 2:
                    raise Exception("tablename error! %s" % table_name)
                table_list = table_name.split(',')
            else:
                table_list = []

            table_list = [x.strip() for x in table_list]

            ## 软件版本
            software_version = 2.8
            self.read_me_file.write(u"compare版本:%s\n" %software_version)
            self.read_me_file.write(u"config版本:%s\n" %config.software_version)
            self.read_me_file.write(u"execute_table版本:%s\n" %execute_table.software_version)
            self.read_me_file.write(u"execute_ap版本:%s\n" % execute_ap.software_version)
            self.read_me_file.write(u"execute_job版本:%s\n" % execute_job.software_version)
            self.read_me_file.write(u"rollback_table版本:%s\n" % rollback_table.software_version)

            self._muti_outStream(u"时间: {date}  系统名: {srcName}  表名: {tableName}\n".format(date=date_list[1], srcName=src_name, tableName=table_name))
            self.read_me_file.write(u"时间: {date}  系统名: {srcName}  表名: {tableName}\n".format(date=date_list[1], srcName=src_name, tableName=table_name))

            '''
                步骤一: 找到新增的schema
                I: 上一版本日期, 这一版本日期, 指定的schema名
                O: 文件输出
            '''
            print(time.strftime('%Y-%m-%d %T', time.localtime()) + "---------------------find_deal_schema------------------------")

            self.find_deal_schema(date_list[0], date_list[1], src_name)

            '''
                步骤二: 找到新增和删除的所有表以及有差异的表
                I: 上一版本日期, 这一版本日期, 指定的schema名, 指定的表名
                O: 有差异的表名单
            '''
            print(time.strftime('%Y-%m-%d %T', time.localtime()) + u"步骤二: 找到新增和删除的所有表以及有差异的表")
            return_dict = self.find_different_tables(date_list[0], date_list[1], src_name, table_list)
            if return_dict.get('returnCode') == 400:
                return -1, return_dict.get('returnMsg')
            different_tables = return_dict.get('returnMsg')
            self._muti_outStream(u"有差异的表: %s \n%s\n" % (','.join(different_tables), '*' *50))

            '''
                步骤三: 找到有差异的表, 按表级找到所有差异
                I: 上一版本日期, 这一版本日期, 有差异的表名单
                O:
            '''
            print(time.strftime('%Y-%m-%d %T', time.localtime()) + u"步骤三: 找到有差异的表, 按表级找到所有差异")
            self.find_all_changes(date_list[0], date_list[1], different_tables)

            '''
                步骤四: 备份 生成 回滚程序
            '''
            print(time.strftime('%Y-%m-%d %T', time.localtime())+ u"步骤四: 备份 生成 回滚程序")
            if config.isLinuxSystem():
                ret = self.generate_rollback_ap(date_list[1])
                if ret:
                    return -1, u"备份ap错误"
                ret = self.generate_rollback_job(date_list[1])
                if ret:
                    return -1, u"备份Job错误"
                ret = self.generate_rollback_table(date_list[1])
                if ret:
                    return -1, u"备份表结构错误"

            '''
                步骤五: 查找数据修复表
                I:这一版本日期
            '''

            self._muti_outStream(u"查找数据修复表\n")
            self.data_fix(date_list[1].encode())

            '''
                步骤六: 判断是否为全量
                I: 这一版本日期
            '''
            self.is_distribute_all(date_list[1], flag = config.CHECK_DISTRIBUTE_ALL_FLAG)

            if self.influencedJoblist:
                self._muti_outStream(u"受影响的作业有 %s\n" %(','.join(self.influencedJoblist)))
                self.read_me_file.write(u"受影响的作业有 %s\n" %(','.join(self.influencedJoblist)))

            '''
               步骤0: 若只输入日期则调度新增, 相当于初始化新增调度
           '''
            print(time.strftime('%Y-%m-%d %T', time.localtime()) + u"步骤0: 若只输入日期则调度新增, 相当于初始化新增调度")
            self.deal_init_job(src_name, table_name, flag)

            # self.read_me_file.close()

            f = codecs.open(config.read_me_path.format(date=date_list[1]), 'r', encoding='utf-8')
            data = f.read()

            print(time.strftime('%Y-%m-%d %T', time.localtime()) + u"生成README_A")
            ## 生成README_A
            if flag == "A":
                schemaList = []
                reg = re.compile(r'(\w+)\.(\w+)')

                for schema, tablename in re.findall(reg,data):
                    schemaList.append(schema)

                schemaDict = Counter(schemaList)

                tablenum = len(re.findall(reg,data))

                reg = re.compile(r'新增模式名：\w+')
                schemalist = re.findall(reg,data)

                if not src_name and not table_list:
                    self.readMeAFile.write(u'筛选条件为时间：\n')
                    self.readMeAFile.write(u'%s版本部署脚本生成成功！\n' %date_list[1])
                    self.readMeAFile.write(u'本次生成：\n')
                    self.readMeAFile.write('\n'.join([u'%s系统表%s张' %(key, value) for key,value in schemaDict.items()]))

                elif src_name and not table_list:
                    self.readMeAFile.write('筛选条件时间+系统：\n')
                    self.readMeAFile.write('%s版本%s系统部署脚本生成成功！' %(date_list[1],src_name))
                    self.readMeAFile.write(u'本次生成：\n')
                    self.readMeAFile.write('\n'.join([u'%s系统表%s张' %(key, value) for key,value in schemaDict.items()]))

                elif src_name and table_list:
                    self.readMeAFile.write(u'筛选条件时间+系统+表名：')
                    self.readMeAFile.write(u'%s版本%s.%s表部署脚本生成成功！' %(date_list[1], src_name, table_name))

            '''
            显示公告内容
            '''

            print(time.strftime('%Y-%m-%d %T', time.localtime()) + u"显示公告内容")
            self.display_notice(date_list[1])

            return 0, u'执行成功'

        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            with open('compare_generate.error', 'w') as f:
                f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            return -1, repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
        finally:
            try:
                self.compare_log_file.close()
                self.delta_ddl_file.close()
                self.ods_ddl_file.close()
                self.his_ddl_file.close()
                self.read_me_file.close()
                self.job_schedule_file.close()
                self.alter_table_file.close()
                self.conn.close()
            except Exception:
                    pass


if __name__ == "__main__":
    print("当前版本: %s" %config.software_version)

    if config.isWindowsSystem():
        date_string = input(
            """使用方法:
        增量模式(比较输入日期与上一日期的差异) : 日期 + '_D'  (如 20160427_D)
        全量模式(生成以输入日期为全量的结果) : 日期 + '_A'  (如 20160427_A)
        注: 1、增量模式下, 数据库中必须存在输入日期对应的上一日期数据
            2、若不输入日期, 默认为数据库中最大日期对应的增量模式
    输入日期_标志:""")
        input_dict = {'date':date_string, 'srcName':'', 'tableName':''}
    else:
        input_dict = {'date':'20150325_A', 'srcName':'FDS', 'tableName':''}

    compare = CompareObj()

    try:
        return_dict = compare.main(input_dict)
    finally:
        compare.conn.close()
        compare.edw_conn.close()

    print(return_dict)

    if config.isWindowsSystem():
        if return_dict.get("returnCode") == 200:
            answer = input("Enter Y to execute sub_ftp.exe or enter N to Exit:")
            if answer.upper() == "Y":
                subprocess.call("sub_ftp.exe")
        else:
            input()

