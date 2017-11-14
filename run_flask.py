# -*- coding: utf-8 -*-
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import commands
#import test
from imp import reload

from flask import Flask
from flask import json
from flask import request

from compare_generate import CompareObj
# import execute
import traceback
import execute_ap
import execute_job
import execute_table
import rollback_table
import codecs
app = Flask(__name__)

##写握手文件
def writeHandFile(Filename, data):
    if not os.path.exists(os.path.dirname(Filename)):
        os.makedirs(os.path.dirname(Filename))
    with open(Filename, 'w') as f:
        f.write(data)

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/compare',methods=['GET', 'POST'])  #执行生成一个生成比较的脚本
def compare():
    if request.method == 'POST':
        a = request.get_data()
        dict1 = json.loads(a)
        changeDate = dict1['date'].split('_')[0]

        ## 创建握手文件
        compareHandFilePath = '/etl/etldata/script/yatop_update/{date}/compareHandFile'.format(date=changeDate)
        if os.path.exists(compareHandFilePath):
            f = open(compareHandFilePath)
            content= f.read()
            if content == 'waiting':
                resp = json.dumps({'returnCode':400,'returnMsg':content})
                return resp.decode('utf-8').encode('utf-8')
           # elif content  == 'success':
                #os.remove(compareHandFilePath)  执行删除与否的效果不明显

        writeHandFile(compareHandFilePath, 'waiting')
        resp = {}
        try:
            compareObj = CompareObj()

            resp = compareObj.main(dict1)
        except Exception:
            print("failed")
            print resp
            exc_type, exc_value, exc_traceback = sys.exc_info()

            with open('compare.error','w') as f:
                f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

            writeHandFile(compareHandFilePath, 'failed')

            resp = json.dumps({'returnCode':400,'returnMsg':resp.get('returnMsg')})
            return resp.decode('utf-8').encode('utf-8')

        print(resp)
        if resp.get('returnCode') == 200:
            writeHandFile(compareHandFilePath, 'success')
        else:
            writeHandFile(compareHandFilePath, 'failed')

        return json.dumps(resp, ensure_ascii=False)
        #return resp.decode('utf-8').encode('utf-8')

#@app.route('/execute')
#def execute_script():
#    resp = execute.main()
#    return resp

@app.route('/rollback_ap',methods=['GET', 'POST'])  #执行回滚脚本
def rollbackap():
    status,output = -1, ""
    if request.method == 'POST':
        a = request.get_data()
        dict1 = json.loads(a)
        changeDate = dict1['date']
        print("rollBackApDate: %s" %changeDate)
        status, output = commands.getstatusoutput("sh /etl/etldata/script/yatop_update/"+changeDate+"/rollback_ap.sh")
        print(output)

    if status == 0:
        status = 200
        ret_dict={"returnCode":status,"returnMsg":output}
        response = app.response_class(
            response=json.dumps(ret_dict),
            status=200,
            mimetype='application/json'
        )
        return response
    else:
        status = 400
        ret_dict={"returnCode":status,"returnMsg":output}
        response = app.response_class(
            response=json.dumps(ret_dict),
            status=400,
            mimetype='application/json'
        )
        return response


@app.route('/rollback_job',methods=['GET', 'POST'])  #执行回滚脚本
def rollbackjob():
    if request.method == 'POST':
        a = request.get_data()
        dict1 = json.loads(a)
        changeDate = dict1['date']
        status, output = commands.getstatusoutput("sh /etl/etldata/script/yatop_update/"+changeDate+"/rollback_job.sh")
        if status ==0 : #将执行成功默认是200,status的值为200,表示成功,需要转码
            status = 200
            output = "回滚job成功"
        ret_dict={"returnCode":status,"returnMsg":output}
        response = app.response_class(
            response=json.dumps(ret_dict),
            status=200,
            mimetype='application/json'
        )
        return response


@app.route('/rollback_table',methods=['GET', 'POST'])  #执行回滚脚本
def rollbacktable():
    if request.method == 'POST':
        a = request.get_data()
        dict1 = json.loads(a)
        changeDate = dict1['SMY_DT']
        # dict1 = {"SMY_DT":"20170426"}

        try:
            resp = rollback_table.main(dict1)
        except Exception:
            print("failed")

            exc_type, exc_value, exc_traceback = sys.exc_info()

            with open('rollback_table.error'.format(date=changeDate),'w') as f:
                f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

            resp = json.dumps({'returnCode':400, 'returnMsg':'RollbackTable An Exception occured'})
            return resp.decode('utf-8').encode('utf-8')

        return resp.decode('utf-8').encode('utf-8')


@app.route('/execute_job',methods=['GET', 'POST'])  #执行JOB脚本
def executejob():
    a = request.get_data()
    dict1 = json.loads(a)
    changeDate = dict1['date']

    ## 创建握手文件
    jobHandFilePath = '/etl/etldata/script/yatop_update/{date}/jobHandFile'.format(date=changeDate)

    #writeHandFile(jobHandFilePath, 'waiting')

    try:
        resp = execute_job.main(changeDate)
    except Exception:
        print("failed")

        exc_type, exc_value, exc_traceback = sys.exc_info()

        with open('execute_job.error'.format(date=changeDate),'w') as f:
            f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

        writeHandFile(jobHandFilePath, 'failed')

        resp = json.dumps({'returnCode':400,'returnMsg':'Job An Exception occured'})
        return resp.decode('utf-8').encode('utf-8')

    if eval(resp).get('returnCode') == 200:
        writeHandFile(jobHandFilePath, 'success')
    else:
        writeHandFile(jobHandFilePath, 'failed')

    return resp.decode('utf-8').encode('utf-8')


@app.route('/execute_ap',methods=['GET', 'POST'])  #执行AP脚本
def executeap():
    a = request.get_data()
    dict1 = json.loads(a)
    changeDate = dict1['date']

    ## 创建握手文件
    apHandFilePath = '/etl/etldata/script/yatop_update/{date}/apHandFile'.format(date=changeDate)

    #writeHandFile(apHandFilePath, 'waiting')

    try:
        resp = execute_ap.mainaaa(changeDate)
    except Exception:
        print("failed")
        exc_type, exc_value, exc_traceback = sys.exc_info()

        with open('execute_ap.error'.format(date=changeDate),'w') as f:
            f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

        writeHandFile(apHandFilePath, 'failed')

        resp = json.dumps({'returnCode':400,'returnMsg':'AP An Exception occured'})
        return resp.decode('utf-8').encode('utf-8')

    if eval(resp).get('returnCode') == 200:
        writeHandFile(apHandFilePath, 'success')
    else:
        writeHandFile(apHandFilePath, 'failed')

    return resp.decode('utf-8').encode('utf-8')



@app.route('/execute_table',methods=['GET', 'POST'])  #执行表脚本
def executetable():
    a = request.get_data()
    dict1 = json.loads(a)
    changeDate = dict1['date']
    guid = dict1['guid']

    ## 创建握手文件
    tableHandFilePath = '/etl/etldata/script/yatop_update/{date}/tableHandFile'.format(date=changeDate)

    #writeHandFile(tableHandFilePath, 'waiting')

    try:
        resp = execute_table.main(changeDate, guid)
    except Exception:

        print("failed")
        writeHandFile(tableHandFilePath, 'failed')

        exc_type, exc_value, exc_traceback = sys.exc_info()

        with open('execute_table.error'.format(date=changeDate),'w') as f:
            f.write(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

        resp = json.dumps({'returnCode':400,'returnMsg':'Table An Exception occured, more /etl/etldata/script/yatop_update/{date}/execute_table.error to see detail'.format(date=changeDate)})
        return resp.decode('utf-8').encode('utf-8')

    if eval(resp).get('returnCode') == 200:
        writeHandFile(tableHandFilePath, 'success')
    else:
        writeHandFile(tableHandFilePath, 'failed')

    return resp.decode('utf-8').encode('utf-8')


@app.route('/getreadme',methods=['GET', 'POST'])#执行readMe脚本
def executegetreadme():

    if request.method == 'POST':
        a = request.get_data()
        dict1 = json.loads(a)
        changeDate = dict1['date']
        print "readme's Date:"+changeDate
        f = open('/etl/etldata/script/yatop_update/'+changeDate+'/README_A.txt', 'r')
        ret_dict={'returnData':f.read()}
        print "dict内容:"+f.read()
        response = app.response_class(
            response=json.dumps(ret_dict),
            status=200,
            mimetype='application/json'
        )
        return response

@app.route('/getAlterReadme',methods=['GET', 'POST'])
def executegetAlterReadme():

    if request.method == 'POST':
        a = request.get_data()
        dict1 = json.loads(a)
        changeDate = dict1['date']
        print "readme's Date:"+changeDate
        f = open('/etl/etldata/script/yatop_update/'+changeDate+'/README.txt', 'r')
        ret_dict={'returnData':f.read()}
        print "dict内容:"+f.read()
        response = app.response_class(
            response=json.dumps(ret_dict),
            status=200,
            mimetype='application/json'
        )
        return response


@app.route('/executeTableError', methods=['GET', 'POST'])  # 执行readMe脚本
def executeTableError():
    if request.method == 'POST':
        a = request.get_data()
        dict1 = json.loads(a)
        changeDate = dict1['date']
        print "readme's Date:" + changeDate
        f = open('/etl/etldata/script/yatop_update/' + changeDate + '/execute_table.error', 'r')
        dict = {'returnData': f.read()}
        print "dict内容:" + f.read()
        response = app.response_class(
            response=json.dumps(dict),
            status=200,
            mimetype='application/json'
        )
        return response


@app.route('/executeApError', methods=['GET', 'POST'])  # 执行readMe脚本
def executeApError():
    if request.method == 'POST':
        a = request.get_data()
        dict1 = json.loads(a)
        changeDate = dict1['date']
        print "readme's Date:" + changeDate
        f = open('/etl/etldata/script/yatop_update/' + changeDate + '/execute_ap.error', 'r')
        dict = {'returnData': f.read()}
        print "dict内容:" + f.read()
        response = app.response_class(
            response=json.dumps(dict),
            status=200,
            mimetype='application/json'
        )
        return response


@app.route('/executeJobError', methods=['GET', 'POST'])  # 执行readMe脚本
def executeJobError():
    if request.method == 'POST':
        a = request.get_data()
        dict1 = json.loads(a)
        changeDate = dict1['date']
        print "readme's Date:" + changeDate
        f = open('/etl/etldata/script/yatop_update/' + changeDate + '/execute_job.error', 'r')
        dict = {'returnData': f.read()}
        print "dict内容:" + f.read()
        response = app.response_class(
            response=json.dumps(dict),
            status=200,
            mimetype='application/json'
        )
        return response


@app.route('/test3')
def test2():

    f = codecs.open(u'aa',encoding='utf-8')
    ret_dict={'returnData':f.read()}
    response = app.response_class(
      response=json.dumps(ret_dict),
        status=200,
         mimetype='application/json'
       )
    return response


@app.route('/statusRefresh',methods=['GET', 'POST']) #判断返回执行的结果
def statusRefresh():
       # a = request.get_data()
       # dict1 = json.loads(a)
       # datestr = dict1['date']

        datestr=request.args.get('date')
        callback=request.args.get('callback')

        path="/etl/etldata/script/yatop_update/%s"%datestr

        execute_generate_ok = path+"/compareHandFile"

        execute_ap_ok = path+"/apHandFile"

        execute_job_ok = path +"/jobHandFile"

        execute_table_ok = path +"/tableHandFile"

        execute_generate_ok_result=""
        execute_ap_ok_result=""
        execute_job_ok_result=""
        execute_table_ok_result=""

        if os.path.exists(execute_generate_ok):
            f = open(execute_generate_ok)
            content= f.read()
            execute_generate_ok_result = content

        if os.path.exists(execute_ap_ok):
            f = open(execute_ap_ok)
            content= f.read()
            execute_ap_ok_result = content

        if os.path.exists(execute_job_ok):
            f = open(execute_job_ok)
            content= f.read()
            execute_job_ok_result = content

        if os.path.exists(execute_table_ok):
            f = open(execute_table_ok)
            content = f.read()
            execute_table_ok_result = content

        ret_dict={
            'status_1':execute_generate_ok_result,
            'status_2':execute_table_ok_result,
            'status_3':execute_ap_ok_result,
            'status_4':execute_job_ok_result
        }

        return callback+"("+json.dumps(ret_dict)+")"

if __name__ == "__main__":
    #app.run(host='122.112.255.126', port=5000, debug=True)
    app.run(host='0.0.0.0', port=5000, debug=True)
    #app.run(port=5000,debug=True)
