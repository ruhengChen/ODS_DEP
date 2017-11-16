# -*- coding=utf-8 -*-

import platform
import sys


def isWindowsSystem():
    return 'Windows' in platform.system()


def isLinuxSystem():
    return 'Linux' in platform.system()

# 软件版本
software_version=2.1

# 数据库配置
edw_dsn = 'edw'
dwmm_dsn = 'dwmm'
ZONE = '金华'
TABSPACE = 'TS_ODS'
IP = '122.112.255.126'
dwmmdb = 'dwmm'
dwmmuser = 'dainst'
dwmmpwd = 'dainst'
edwdb = 'edw'
edwuser = 'edwinst'
edwpwd = 'edwinst'

# 过滤模式名列表
SKIP_SCHEMA_LIST = ['MCBS'   # 微粒贷
    ,]

# 行社代码
BANK_ID = '828000'

FTP_USER = 'sjff'
FTP_PWD = 'sjff'

## 分发数据服务器IP
distribute_server_ip = '154.125.31.82'
## 分发数据服务器路径
distribute_server_path = '/home/sjff/data/sdata/S-999000/'

# 省联社下发公告路径

all_package_path = '/etl/etldata/TDDOWNLOAD/'

# encoding
delta_log_encoding = 'gb18030'
all_log_encoding = 'gb18030'
his_log_encoding = 'gb18030'
alter_table_encoding = 'gb18030'

# 是否开启全量检查
CHECK_DISTRIBUTE_ALL_FLAG = True

# python版本
VERSION = sys.version_info.major

# 操作系统
if isLinuxSystem():
    # path
    etl_path = '/etl/etldata/script/yatop_update/{date}/'
    job_schedule_path = '/etl/etldata/script/yatop_update/{date}/job_schedule.SQL'
    delta_tables_path = '/etl/etldata/script/yatop_update/{date}/delta_tables.ddl'
    tmp_delta_tables_path = '/etl/etldata/script/yatop_update/{date}/tmp_delta_tables.ddl'
    alter_table_path = '/etl/etldata/script/yatop_update/{date}/alter_table.sql'
    his_tables_path = '/etl/etldata/script/yatop_update/{date}/his_tables.ddl'
    ods_tables_path = '/etl/etldata/script/yatop_update/{date}/ods_tables.ddl'
    read_me_path = '/etl/etldata/script/yatop_update/{date}/README.txt'
    read_me_a_path = '/etl/etldata/script/yatop_update/{date}/README_A.txt'

    apsql_path = '/etl/etldata/script/yatop_update/{date}/AP/{APNAME}'
    apsql_ods_path = '/etl/etldata/script/odssql/{APNAME}'

    # backup_path

    backup_path = '/etl/etldata/script/yatop_update/{date}/backup/'
    job_metadata_path = '/etl/etldata/script/yatop_update/{date}/backup/JOB_METADATA.del'
    job_seq_path = '/etl/etldata/script/yatop_update/{date}/backup/JOB_SEQ.del'

    # log_path
    compare_generate_path = '/etl/etldata/script/yatop_update/{date}/compare_generate.log'
    execute_table_logFile = '/etl/etldata/script/yatop_update/{date}/execute_table.log'
    execute_job_logFile = '/etl/etldata/script/yatop_update/{date}/execute_job.log'
    execute_ap_logFile = '/etl/etldata/script/yatop_update/{date}/execute_ap.log'
    execute_log_path = '/etl/etldata/script/yatop_update/{date}/execute.log'
    backup_list = '/etl/etldata/script/yatop_update/{date}/backup_list'

    # sh_path

    rollback_ap_path = '/etl/etldata/script/yatop_update/{date}/rollback_ap.sh'
    rollback_job_path = '/etl/etldata/script/yatop_update/{date}/rollback_job.sh'
    rollback_table_path = '/etl/etldata/script/yatop_update/{date}/rollback_table.sh'

elif isWindowsSystem:
    # path
    etl_path = '{date}/'
    job_schedule_path = '{date}/job_schedule.SQL'
    delta_tables_path = '{date}/delta_tables.ddl'
    tmp_delta_tables_path = '{date}/tmp_delta_tables.ddl'
    alter_table_path = '{date}/alter_table.sql'
    his_tables_path = '{date}/his_tables.ddl'
    ods_tables_path = '{date}/ods_tables.ddl'
    read_me_path = '{date}/README.txt'
    read_me_a_path = '{date}/README_A.txt'

    apsql_path = '{date}/AP/{APNAME}'
    apsql_ods_path = '/etl/etldata/script/odssql/{APNAME}'

    # backup_path

    backup_path = '{date}/backup/'
    job_metadata_path = '{date}/backup/JOB_METADATA.del'
    job_seq_path = '{date}/backup/JOB_SEQ.del'

    # log_path
    compare_generate_path = '{date}/compare_generate.log'
    execute_table_logFile = '{date}/execute_table.log'
    execute_job_logFile = '{date}/execute_job.log'
    execute_ap_logFile = '{date}/execute_ap.log'
    execute_log_path = '{date}/execute.log'
    backup_list = '{date}/backup_list'

    # sh_path

    rollback_ap_path = '{date}/rollback_ap.sh'
    rollback_job_path = '{date}/rollback_job.sh'
    rollback_table_path = '{date}/rollback_table.sh'

else:
    print("platform error")
