#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from k3cloud_webapi_sdk.main import K3CloudApiSdk
from pyrda.dbms.rds import RdClient

def erp_save(app2,api_sdk,option,data,app3):


    erro_list = []
    sucess_num = 0
    erro_num = 0

    api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                       option['app_sec'], option['server_url'])

    for i in data:

        try:

            if exist_order(api_sdk,i[0]['FGODOWNNO'])!=True:

                    model={
                        "Model": {
                            "FID": 0,
                            "FBillNo": str(i[0]['FGODOWNNO']),
                            "FBillTypeID": {
                                "FNUMBER": "QTRKD01_SYS"
                            },
                            "FStockOrgId": {
                                "FNumber": "104"
                            },
                            "FStockDirect": "GENERAL",
                            "FDate": str(i[0]['FBUSINESSDATE']),
                            "FSUPPLIERID": {
                                "FNumber": code_conversion(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'])
                            },
                            "FDEPTID": {
                                "FNumber": "BM000040"
                            },
                            "FSTOCKERID": {
                                "FNumber": selectStockKeeper(app2,str(i[0]['FLIBRARYSIGN']))
                            },
                            "FSTOCKERGROUPID": {
                                "FNumber": "SKCKZ01"
                            },
                            "FOwnerTypeIdHead": "BD_OwnerOrg",
                            "FOwnerIdHead": {
                                "FNumber": "104"
                            },
                            "FNOTE": str(i[0]['FBILLNO']),
                            "FBaseCurrId": {
                                "FNumber": "PRE001"
                            },
                            "FEntity": data_splicing(app2,i)
                        }
                    }

                    save_res=json.loads(api_sdk.Save("STK_MISCELLANEOUS",model))

                    if save_res['Result']['ResponseStatus']['IsSuccess']:

                        submit_result = ERP_submit(api_sdk, str(i[0]['FGODOWNNO']))

                        if submit_result:

                            audit_result = ERP_Audit(api_sdk, str(i[0]['FGODOWNNO']))

                            if audit_result:

                                insertLog(app3, "其他入库单", str(i[0]['FGODOWNNO']), "数据同步成功", "1")

                                changeStatus(app3, str(i[0]['FGODOWNNO']), "1")

                                sucess_num=sucess_num+1

                            else:
                                changeStatus(app3, str(i[0]['FGODOWNNO']), "2")
                        else:
                            changeStatus(app3, str(i[0]['FGODOWNNO']), "2")
                    else:

                        insertLog(app3, "其他入库单", str(i[0]['FGODOWNNO']),save_res['Result']['ResponseStatus']['Errors'][0]['Message'],"2")

                        changeStatus(app3, str(i[0]['FGODOWNNO']), "2")

                        erro_num=erro_num+1
                        erro_list.append(save_res)

        except Exception as e:

            insertLog(app3, "其他入库单", str(i[0]['FGODOWNNO']),"数据异常","2")

    dict = {
        "sucessNum": sucess_num,
        "erroNum": erro_num,
        "erroList": erro_list
    }

    return dict

def selectStockKeeper(app2,FName):
    '''
    查看仓管员
    :param app2:
    :param FName:
    :return:
    '''

    sql=f"select FNUMBER from rds_vw_storekeeper where FNAME='{FName}'"

    res=app2.select(sql)

    if res:

        return res[0]['FNUMBER']

    else :

        return ""

def exist_order(api_sdk,FNumber):
    '''
    查看订单是否存在
    :param api_sdk:
    :param FNumber:
    :return:
    '''
    try:

        model = {
            "CreateOrgId": 0,
            "Number": FNumber,
            "Id": "",
            "IsSortBySeq": "false"
        }

        res = json.loads(api_sdk.View("STK_MISCELLANEOUS", model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return True

def ERP_submit(api_sdk,FNumber):

    try:

        model={
            "CreateOrgId": 0,
            "Numbers": [FNumber],
            "Ids": "",
            "SelectedPostId": 0,
            "NetworkCtrl": "",
            "IgnoreInterationFlag": ""
        }

        res=json.loads(api_sdk.Submit("STK_MISCELLANEOUS",model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False

def ERP_Audit(api_sdk,FNumber):
    '''
    将订单审核
    :param api_sdk: API接口对象
    :param FNumber: 订单编码
    :return:
    '''

    try:

        model={
            "CreateOrgId": 0,
            "Numbers": [FNumber],
            "Ids": "",
            "InterationFlags": "",
            "NetworkCtrl": "",
            "IsVerifyProcInst": "",
            "IgnoreInterationFlag": ""
        }

        res = json.loads(api_sdk.Audit("STK_MISCELLANEOUS", model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False

def getCode(app3):
    '''
    查询出表中的编码
    :param app2:
    :return:
    '''

    sql="select distinct FGODOWNNO from RDS_ECS_ODS_pur_storageacct where FIsDo=0 and FIsFree=1"

    res=app3.select(sql)

    return res

def getClassfyData(app3,code):
    '''
    获得分类数据
    :param app2:
    :param code:
    :return:
    '''
    try:

        number=code['FGODOWNNO']

        sql=f"select * from RDS_ECS_ODS_pur_storageacct where FGODOWNNO='{number}'"

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

def changeStatus(app3,fnumber,status):
    '''
    将没有写入的数据状态改为2
    :param app2: 执行sql语句对象
    :param fnumber: 订单编码
    :param status: 数据状态
    :return:
    '''

    sql=f"update a set a.Fisdo={status} from RDS_ECS_ODS_pur_storageacct a where FGODOWNNO='{fnumber}'"

    app3.update(sql)

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


def data_splicing(app2,data):
    '''
    将订单内的物料进行遍历组成一个列表，然后将结果返回给
    :param data:
    :return:
    '''

    list=[]

    for i in data:

        list.append(json_model(app2,i))

    return list

def json_model(app2, model_data):

    try:

        model = {
            "FMATERIALID": {
                "FNumber": "7.1.000001" if model_data['FGOODSID'] == '1' else code_conversion_org(app2,"rds_vw_material","F_SZSP_SKUNUMBER",model_data['FGOODSID'],                                                                                         "104", "FNUMBER")
            },
            # "FUnitID": {
            #     "FNumber": "01"
            # },
            "FSTOCKID": {
                "FNumber": "SK01"
            },
            "FSTOCKSTATUSID": {
                "FNumber": "KCZT01_SYS"
            },
            "FLOT": {
                "FNumber": str(model_data['FLOT'])
            },
            "FQty": str(model_data['FINSTOCKQTY']),
            # "FPRODUCEDATE": "2022-11-04 00:00:00",
            "FOWNERTYPEID": "BD_OwnerOrg",
            "FOWNERID": {
                "FNumber": "104"
            },
            "FKEEPERTYPEID": "BD_KeeperOrg",
            "FKEEPERID": {
                "FNumber": "104"
            }
        }

        return model

    except Exception as e:

        return {}


def otherInStock(startDate,endDate):

    app2 = RdClient(token='57DEDF26-5C00-4CA9-BBF7-57ECE07E179B')
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')

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

        msg=erp_save(app2, api_sdk, option1, res,app3)

        return msg
    else:

        return {"message":"无订单需要同步"}



