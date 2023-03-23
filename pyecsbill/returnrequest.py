#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import requests
import hashlib
from k3cloud_webapi_sdk.main import K3CloudApiSdk
from pyrda.dbms.rds import RdClient
import pandas as pd

def encryption(pageNum,pageSize,queryList,tableName):
    '''
    ECS的token加密
    :param pageNum:
    :param pageSize:
    :param queryList:
    :param tableName:
    :return:
    '''

    m = hashlib.md5()

    token=f'accessId=skyx@prod&accessKey=skyx@0512@1024@prod&pageNum={pageNum}&pageSize={pageSize}&queryList={queryList}&tableName={tableName}'

    m.update(token.encode())

    md5 = m.hexdigest()

    return md5


def ECS_post_info2(url,pageNum,pageSize,qw,qw2,tableName,updateTime,updateTime2,key):
    '''
    生科云选API接口
    :param url: 地址
    :param pageNum: 页码
    :param pageSize: 页面大小
    :param qw: 查询条件
    :param tableName: 表名
    :param updateTime: 时间戳
    :return: dataframe
    '''

    try:

        queryList='[{"qw":'+f'"{qw}"'+',"value":'+f'"{updateTime}"'+',"key":'+f'"{key}"'+'},{"qw":'+f'"{qw2}"'+',"value":'+f'"{updateTime2}"'+',"key":'+f'"{key}"'+'}]'

        # 查询条件
        queryList1=[{"qw":qw,"value":updateTime,"key":key},{"qw":qw2,"value":updateTime2,"key":key}]

        # 查询的表名
        tableName=tableName

        data ={
            "tableName": tableName,
            "pageNum": pageNum,
            "pageSize": pageSize,
            "token": encryption(pageNum, pageSize, queryList, tableName),
            "queryList": queryList1
        }
        data = json.dumps(data)


        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(url, headers=headers,data=data)

        info = response.json()

        df = pd.DataFrame(info['data']['list'])

        return df

    except Exception as e:

        return pd.DataFrame()


def viewPage(url,pageNum,pageSize,qw,qw2,tableName,updateTime,updateTime2,key):
    '''
    生科云选API接口
    :param url: 地址
    :param pageNum: 页码
    :param pageSize: 页面大小
    :param qw: 查询条件
    :param tableName: 表名
    :param updateTime: 时间戳
    :return: dataframe
    '''

    try:

        queryList='[{"qw":'+f'"{qw}"'+',"value":'+f'"{updateTime}"'+',"key":'+f'"{key}"'+'},{"qw":'+f'"{qw2}"'+',"value":'+f'"{updateTime2}"'+',"key":'+f'"{key}"'+'}]'

        # 查询条件
        queryList1=[{"qw":qw,"value":updateTime,"key":key},{"qw":qw2,"value":updateTime2,"key":key}]

        # 查询的表名
        tableName=tableName

        data ={
            "tableName": tableName,
            "pageNum": pageNum,
            "pageSize": pageSize,
            "token": encryption(pageNum, pageSize, queryList, tableName),
            "queryList": queryList1
        }
        data = json.dumps(data)

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(url, headers=headers,data=data)

        info = response.json()

        return info['data']['pages']

    except Exception as e:

        return ""




def associated(app2,api_sdk,option,data,app3):

    erro_list = []
    sucess_num = 0
    erro_num = 0

    api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                       option['app_sec'], option['server_url'])

    for i in data:

        try:

            if check_deliveryExist(api_sdk,i[0]['FMRBBILLNO'])!=True:

                model={
                    "Model": {
                        "FID": 0,
                        "FBillTypeID": {
                            "FNUMBER": "TLSQDD01_SYS"
                        },
                        "FBillNo": str(i[0]['FMRBBILLNO']),
                        "FDate": str(i[0]['FDATE']),
                        "FAPPORGID": {
                            "FNumber": "104"
                        },
                        "FRequireOrgId": {
                            "FNumber": "104"
                        },
                        "FRMTYPE": "B",
                        "FRMMODE": "A",
                        "FCorrespondOrgId": {
                            "FNumber": code_conversion(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'])
                        },
                        "FRMREASON": {
                            "FNumber": "01"
                        },
                        "FPURCHASEORGID": {
                            "FNumber": "104"
                        },
                        "FSUPPLIERID": {
                            "FNumber": code_conversion(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'])
                        },
                        "F_SubEntity_FIN": {
                            "FSettleTypeId": {
                                "FNumber": "104"
                            },
                            "FLOCALCURRID": {
                                "FNumber": "PRE001"
                            },
                            "FExchangeTypeId": {
                                "FNUMBER": "HLTX01_SYS"
                            },
                            "FISPRICEEXCLUDETAX": True
                        },
                        "FEntity": data_splicing(app2,api_sdk,i)
                    }
                }


                res=json.loads(api_sdk.Save("PUR_MRAPP",model))

                if res['Result']['ResponseStatus']['IsSuccess']:

                    FNumber = res['Result']['ResponseStatus']['SuccessEntitys'][0]['Number']

                    submit_res=ERP_submit(api_sdk,FNumber)

                    if submit_res:

                        audit_res=ERP_Audit(api_sdk,FNumber)

                        if audit_res:

                            insertLog(app3, "退料申请单", i[0]['FMRBBILLNO'], "数据同步成功", "1")

                            changeStatus(app3,str(i[0]['FMRBBILLNO']),"3")

                            sucess_num=sucess_num+1

                            pass

                        else:
                            pass
                    else:
                        pass
                else:

                    insertLog(app3, "退料申请单", i[0]['FMRBBILLNO'],res['Result']['ResponseStatus']['Errors'][0]['Message'],"2")

                    changeStatus(app3,str(i[0]['FMRBBILLNO']),"2")

                    erro_num=erro_num+1

                    erro_list.append(res)
            else:
                changeStatus(app3, str(i[0]['FMRBBILLNO']), "3")

        except Exception as e:

            insertLog(app3, "退料申请单", i[0]['FMRBBILLNO'],"数据异常","2")

    dict = {
        "sucessNum": sucess_num,
        "erroNum": erro_num,
        "erroList": erro_list
    }

    return dict



def check_deliveryExist(api_sdk,FNumber):

    model={
        "CreateOrgId": 0,
        "Number": FNumber,
        "Id": "",
        "IsSortBySeq": "false"
    }

    res=json.loads(api_sdk.View("PUR_MRAPP",model))

    return res['Result']['ResponseStatus']['IsSuccess']

def ERP_submit(api_sdk,FNumber):

    model={
        "CreateOrgId": 0,
        "Numbers": [FNumber],
        "Ids": "",
        "SelectedPostId": 0,
        "NetworkCtrl": "",
        "IgnoreInterationFlag": ""
    }

    res=json.loads(api_sdk.Submit("PUR_MRAPP",model))

    return res['Result']['ResponseStatus']['IsSuccess']

def ERP_Audit(api_sdk,FNumber):
    '''
    将订单审核
    :param api_sdk: API接口对象
    :param FNumber: 订单编码
    :return:
    '''

    model={
        "CreateOrgId": 0,
        "Numbers": [FNumber],
        "Ids": "",
        "InterationFlags": "",
        "NetworkCtrl": "",
        "IsVerifyProcInst": "",
        "IgnoreInterationFlag": "",
    }

    res = json.loads(api_sdk.Audit("PUR_MRAPP", model))

    return res['Result']['ResponseStatus']['IsSuccess']



def PurchaseOrder_view(api_sdk,value,materialID):
    '''
    单据查询
    :param value: 订单编码
    :return:
    '''

    res=json.loads(api_sdk.ExecuteBillQuery({"FormId": "PUR_PurchaseOrder", "FieldKeys": "FDate,FBillNo,FId,FPOOrderEntry_FEntryID,FMaterialId", "FilterString": [{"Left":"(","FieldName":"FMaterialId","Compare":"=","Value":materialID,"Right":")","Logic":"AND"},{"Left":"(","FieldName":"FBillNo","Compare":"=","Value":value,"Right":")","Logic":"AND"}], "TopRowCount": 0}))

    return res

def getCode(app3):
    '''
    查询出表中的编码
    :param app2:
    :return:
    '''

    sql="select distinct FMRBBILLNO from RDS_ECS_ODS_pur_return where FIsDo=0"

    res=app3.select(sql)

    return res


def isbatch(app2,FNumber):

    sql=f"select FISBATCHMANAGE from rds_vw_fisbatch where F_SZSP_SKUNUMBER='{FNumber}'"

    res = app2.select(sql)

    if res == []:

        return ""

    else:

        return res[0]['FISBATCHMANAGE']


def getClassfyData(app3,code):
    '''
    获得分类数据
    :param app2:
    :param code:
    :return:
    '''

    try:

        sql=f"select FMRBBILLNO,FPURORDERNO,FPOORDERSEQ,FBILLTYPEID,FCUSTOMERNUMBER,FSUPPLIERFIELD,FSUPPLIERNAME,FSUPPLIERABBR,FSTOCKID,FGOODSTYPEID,FBARCODE,FGOODSID,FPRDNAME,FRETSALEPRICE,FTAXRATE,FLOT,FRETQTY,FRETAMOUNT,FCHECKSTATUS,FUploadDate,FIsDo,FINISHTIME,FDATE,MANUFACTUREDATE,EFFECTDATE,FReturnId from RDS_ECS_ODS_pur_return where FMRBBILLNO='{code['FMRBBILLNO']}'"

        res=app3.select(sql)

        return res

    except Exception as e:

        return []

def code_conversion(app2,tableName,param,param2):
    '''
    通过ECS物料编码来查询系统内的编码
    :param app2: 数据库操作对象
    :param tableName: 表名
    :param param:  参数1
    :param param2: 参数2
    :return:
    '''

    sql=f"select FNumber from {tableName} where {param}='{param2}'"

    res=app2.select(sql)

    if res==[]:

        return ""

    else:

        return res[0]['FNumber']

def code_conversion_org(app2,tableName,param,param2,param3,param4):
    '''
    通过ECS物料编码来查询系统内的编码
    :param app2: 数据库操作对象
    :param tableName: 表名
    :param param:  参数1
    :param param2: 参数2
    :return:
    '''

    sql=f"select {param4} from {tableName} where {param}='{param2}' and FOrgNumber='{param3}'"

    res=app2.select(sql)

    if res==[]:

        return ""

    else:

        return res[0][param4]


def changeStatus(app3,fnumber,status):
    '''
    将没有写入的数据状态改为2
    :param app2: 执行sql语句对象
    :param fnumber: 订单编码
    :param status: 数据状态
    :return:
    '''

    sql=f"update a set a.Fisdo={status} from RDS_ECS_ODS_pur_return a where FMRBBILLNO='{fnumber}'"

    app3.update(sql)

def checkDataExist(app2, FReturnId):
    '''
    通过FSEQ字段判断数据是否在表中存在
    :param app2:
    :param FSEQ:
    :return:
    '''
    sql = f"select FReturnId from RDS_ECS_SRC_pur_return where FReturnId='{FReturnId}'"

    res = app2.select(sql)

    if res == []:

        return True

    else:

        return False


def insert_procurement_return(app2,app3,data):
    '''
    采购退货
    :param app2:
    :param data:
    :return:
    '''


    for i in data.index:

        if checkDataExist(app3,data.loc[i]['FReturnId']):

            if judgementData(app2, app3, data[data['FMRBBILLNO'] == data.loc[i]['FMRBBILLNO']]):

                inert_data(app3, data[data['FMRBBILLNO'] == data.loc[i]['FMRBBILLNO']])


def judgementData(app2, app3, data):
    '''
    判断数据是否合规
    :param app2:
    :param data:
    :return:
    '''

    flag = True

    for i in data.index:
        if code_conversion(app2, "rds_vw_supplier", "FNAME", data.loc[i]['FSUPPLIERNAME']) != "":

            if code_conversion(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", data.loc[i]['FGOODSID']) != "" or \
                    data.loc[i]['FGOODSID'] == "1":

                continue

            else:

                insertLog(app3, "退料申请单", data.loc[i]['FMRBBILLNO'], "物料不存在","2")

                flag = False

                break
        else:

            insertLog(app3, "退料申请单", data.loc[i]['FMRBBILLNO'], "客户不存在","2")

            flag = False

            break

    return flag


def inert_data(app3,data):

    for i in data.index:

        try:

            sql="insert into RDS_ECS_SRC_pur_return(FMRBBILLNO,FPURORDERNO,FPOORDERSEQ,FBILLTYPEID,FCUSTOMERNUMBER,FSUPPLIERFIELD,FSUPPLIERNAME,FSUPPLIERABBR,FSTOCKID,FGOODSTYPEID,FBARCODE,FGOODSID,FPRDNAME,FRETSALEPRICE,FTAXRATE,FLOT,FRETQTY,FRETAMOUNT,FCHECKSTATUS,FUploadDate,FIsDo,FINISHTIME,FDATE,MANUFACTUREDATE,EFFECTDATE,FReturnId) values('"+data.loc[i]['FMRBBILLNO']+"','"+data.loc[i]['FPURORDERNO']+"','"+data.loc[i]['FPOORDERSEQ']+"','"+data.loc[i]['FBILLTYPEID']+"','"+data.loc[i]['FSUPPLIERFIELD']+"','"+data.loc[i]['FCUSTOMERNUMBER']+"','"+data.loc[i]['FSUPPLIERNAME']+"','"+data.loc[i]['FSUPPLIERABBR']+"','"+data.loc[i]['FSTOCKID']+"','"+data.loc[i]['FGOODSTYPEID']+"','"+data.loc[i]['FBARCODE']+"','"+data.loc[i]['FGOODSID']+"','"+data.loc[i]['FPRDNAME']+"','"+data.loc[i]['FRETSALEPRICE']+"','"+data.loc[i]['FTAXRATE']+"','"+data.loc[i]['FLOT']+"','"+data.loc[i]['FRETQTY']+"','"+data.loc[i]['FRETAMOUNT']+"','"+data.loc[i]['FCHECKSTATUS']+"',getdate(),0,'"+data.loc[i]['FINISHTIME']+"','"+data.loc[i]['FDATE']+"','"+data.loc[i]['MANUFACTUREDATE']+"','"+data.loc[i]['EFFECTDATE']+"','"+data.loc[i]['FReturnId']+"')"

            app3.insert(sql)

            insertLog(app3, "退料申请单", data.loc[i]['FMRBBILLNO'], "数据插入成功", "1")

        except Exception as e:

            insertLog(app3, "退料申请单", data.loc[i]['FMRBBILLNO'], "插入SRC数据异常，请检查数据","2")

    pass



def insertLog(app2,FProgramName,FNumber,Message,FIsdo,cp='赛普'):
    '''
    异常数据日志
    :param app2:
    :param FNumber:
    :param Message:
    :return:
    '''

    sql="insert into RDS_ECS_Log(FProgramName,FNumber,FMessage,FOccurrenceTime,FCompanyName,FIsdo) values('"+FProgramName+"','"+FNumber+"','"+Message+"',getdate(),'"+cp+"','"+FIsdo+"')"

    app2.insert(sql)


def classification_process(app3,data):
    '''
    将编码进行去重，然后进行分类
    :param data:
    :return:
    '''

    res=fuz(app3,data)

    return res

def fuz(app3,codeList):
    '''
    通过编码分类，将分类好的数据装入列表
    :param app2:
    :param codeList:
    :return:
    '''

    singleList=[]

    for i in codeList:

        data=getClassfyData(app3,i)

        singleList.append(data)


    return singleList

def data_splicing(app2,api_sdk,data):
    '''
    将订单内的物料进行遍历组成一个列表，然后将结果返回给 FSaleOrderEntry
    :param data:
    :return:
    '''

    list=[]

    for i in data:

        result=json_model(app2,i,api_sdk)

        if result:

            list.append(result)

        else:

            return []

    return list

def json_model(app2,model_data,api_sdk):

    try:

        materialSKU="7.1.000001" if str(model_data['FGOODSID'])=='1' else str(model_data['FGOODSID'])
        materialId=code_conversion_org(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", materialSKU,"104","FMATERIALID")

        if materialSKU=="7.1.000001":

            materialId="466653"

        result=PurchaseOrder_view(api_sdk,str(model_data['FPURORDERNO']),materialId)

        if result!=[] and materialId!="":

            model={
                    "FMATERIALID": {
                        "FNumber": "7.1.000001" if model_data['FGOODSID']=='1' else str(code_conversion(app2,"rds_vw_material","F_SZSP_SKUNUMBER",model_data['FGOODSID']))
                    },
                    # "FUNITID": {
                    #     "FNumber": "01"
                    # },
                    "FMRAPPQTY": str(model_data['FRETQTY']),
                    # "FPRICEUNITID_F": {
                    #     "FNumber": "01"
                    # },
                    "FREPLENISHQTY": str(model_data['FRETQTY']),
                    "FKEAPAMTQTY": str(model_data['FRETQTY']),
                    # "FRMREASON_M": {
                    #     "FNumber": "01"
                    # },
                    "FGiveAway": False,
                    "FLot": {
                        "FNumber": str(model_data['FLOT']) if isbatch(app2,model_data['FGOODSID'])=='1' else ""
                    },
                    "FPRICECOEFFICIENT_F": 1.0,
                    "FPRICE_F": str(model_data['FRETSALEPRICE']),
                    "FTAXNETPRICE_F": str(model_data['FRETSALEPRICE']),
                    "FPriceBaseQty": str(model_data['FRETQTY']),
                    # "FPURUNITID": {
                    #     "FNumber": "01"
                    # },
                    "FPurQty": str(model_data['FRETQTY']),
                    "FPurBaseQty": str(model_data['FRETQTY']),
                    "FEntity_Link": [{
                        "FEntity_Link_FRuleId": "PUR_PurchaseOrder-PUR_MRAPP",
                        "FEntity_Link_FSTableName": "t_PUR_POOrderEntry",
                        "FEntity_Link_FSBillId": result[0][2],
                        "FEntity_Link_FSId": result[0][3],
                        "FEntity_Link_FBASEUNITQTYOld ": str(model_data['FRETQTY']),
                        "FEntity_Link_FBASEUNITQTY ": str(model_data['FRETQTY']),
                        "FEntity_Link_FPurBaseQtyOld ": str(model_data['FRETQTY']),
                        "FEntity_Link_FPurBaseQty ": str(model_data['FRETQTY']),
                    }]
                }
            return model
        else:
            return {}

    except Exception as e:

        return {}

def writeSRC(startDate, endDate, app2,app3):
    '''
    将ECS数据取过来插入SRC表中
    :param startDate:
    :param endDate:
    :return:
    '''

    url = "https://kingdee-api.bioyx.cn/dynamic/query"

    page = viewPage(url, 1, 1000, "ge", "le", "v_procurement_return", startDate, endDate, "UPDATETIME")

    for i in range(1, page + 1):
        df = ECS_post_info2(url, i, 1000, "ge", "le", "v_procurement_return", startDate, endDate, "UPDATETIME")

        insert_procurement_return(app2,app3, df)

    pass

def returnRequest(startDate,endDate):

    app2 = RdClient(token='57DEDF26-5C00-4CA9-BBF7-57ECE07E179B')
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')


    writeSRC(startDate,endDate,app2,app3)

    data = getCode(app3)

    if data:

        res = classification_process(app3, data)
        # 新账套
        option1 = {
            "acct_id": '62777efb5510ce',
            "user_name": 'DMS',
            "app_id": '235685_4e6vScvJUlAf4eyGRd3P078v7h0ZQCPH',
            # "app_sec": 'd019b038bc3c4b02b962e1756f49e179',
            "app_sec": 'b105890b343b40ba908ed51453940935',
            "server_url": 'http://192.168.1.13/K3Cloud',
        }


        api_sdk = K3CloudApiSdk()

        msg=associated(app2, api_sdk, option1, res,app3)

        return msg
    else:

        return {"message":"无订单需要同步"}



