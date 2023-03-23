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

        return 0




def associated(app2,api_sdk,option,data,app3):


    erro_list = []
    sucess_num = 0
    erro_num = 0

    api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                       option['app_sec'], option['server_url'])

    for i in data:

        try:

            purchase = ""

            if ERP_view(api_sdk, i[0]['FBILLNO'])['Result']['ResponseStatus']['IsSuccess']:
                purchase = ERP_view(api_sdk, i[0]['FBILLNO'])['Result']['Result']['PurchaserId']['Number']

            if check_order_exists(api_sdk,str(i[0]['FGODOWNNO']))!=True:

                    model={
                        "Model": {
                            "FID": 0,
                            "FBillTypeID": {
                                "FNUMBER": "RKD01_SYS"
                            },
                            "FBillNo": str(i[0]['FGODOWNNO']),
                            "FDate": str(i[0]['FBUSINESSDATE']),
                            "FStockOrgId": {
                                "FNumber": "104"
                            },
                            "FStockDeptId": {
                                "FNumber": "BM000040"
                            },
                            "FStockerGroupId": {
                                "FNumber": "SKCKZ01"
                            },
                            "FStockerId": {
                                "FNumber": selectStockKeeper(app2,str(i[0]['FLIBRARYSIGN']))
                            },
                            "FDemandOrgId": {
                                "FNumber": "104"
                            },
                            "FCorrespondOrgId": {
                                "FNumber": code_conversion_org(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'],"104","FNUMBER")
                            },
                            "FPurchaseOrgId": {
                                "FNumber": "104"
                            },
                            "FPurchaseDeptId": {
                                "FNumber": "BM000040"
                            },
                            "FPurchaserGroupId": {
                                "FNumber": "SKYX02"
                            },
                            "FPurchaserId": {
                                "FNumber": purchase
                            },
                            "FSupplierId": {
                                "FNumber": code_conversion_org(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'],"104","FNUMBER")
                            },
                            "FSupplyId": {
                                "FNumber": code_conversion_org(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'],"104","FNUMBER")
                            },
                            "FSettleId": {
                                "FNumber": code_conversion_org(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'],"104","FNUMBER")
                            },
                            "FChargeId": {
                                "FNumber": code_conversion_org(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'],"104","FNUMBER")
                            },
                            "FOwnerTypeIdHead": "BD_OwnerOrg",
                            "FOwnerIdHead": {
                                "FNumber": "104"
                            },
                            "FSplitBillType": "A",
                            "F_SZSP_Assistant": {
                                "FNumber": "LX07"
                            },
                            "FInStockFin": {
                                "FSettleOrgId": {
                                    "FNumber": "104"
                                },
                                "FSettleTypeId": {
                                    "FNumber": "JSFS04_SYS"
                                },
                                "FSettleCurrId": {
                                    "FNumber": "PRE001"
                                },
                                "FIsIncludedTax": True,
                                "FPriceTimePoint": "1",
                                "FLocalCurrId": {
                                    "FNumber": "PRE001"
                                },
                                "FExchangeTypeId": {
                                    "FNumber": "HLTX01_SYS"
                                },
                                "FExchangeRate": 1.0,
                                "FISPRICEEXCLUDETAX": True
                            },
                            "FInStockEntry": data_splicing(app2,api_sdk,i,i[0]['FGODOWNNO'])
                        }
                    }

                    save_result=json.loads(api_sdk.Save("STK_InStock",model))

                    if save_result['Result']['ResponseStatus']['IsSuccess']:

                        submit_result = ERP_submit(api_sdk, str(i[0]['FGODOWNNO']))

                        if submit_result:

                            audit_result = ERP_Audit(api_sdk, str(i[0]['FGODOWNNO']))

                            if audit_result:

                                insertLog(app3, "采购入库单", str(i[0]['FGODOWNNO']), "数据同步成功", "1")

                                changeStatus(app3,str(i[0]['FGODOWNNO']),"1")

                                sucess_num=sucess_num+1

                            else:
                                changeStatus(app3,str(i[0]['FGODOWNNO']),"2")
                        else:
                            changeStatus(app3,str(i[0]['FGODOWNNO']),"2")
                    else:

                        insertLog(app3, "采购入库单",str(i[0]['FGODOWNNO']), save_result['Result']['ResponseStatus']['Errors'][0]['Message'],"2")

                        changeStatus(app3, str(i[0]['FGODOWNNO']), "2")

                        erro_num=erro_num+1

                        erro_list.append(save_result)

            else:
                changeStatus(app3, str(i[0]['FGODOWNNO']), "1")

        except Exception as e:

            insertLog(app3, "采购入库单",str(i[0]['FGODOWNNO']),"数据异常","2")

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

        res=json.loads(api_sdk.Submit("STK_InStock",model))

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

        res = json.loads(api_sdk.Audit("STK_InStock", model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False

def Order_view(api_sdk,value):
    '''
    收料通知单单据查询
    :param value: 订单编码
    :return:
    '''

    res=json.loads(api_sdk.ExecuteBillQuery({"FormId": "PUR_ReceiveBill", "FieldKeys": "FDate,FBillNo,FId,FDetailEntity_FEntryID", "FilterString": [{"Left":"(","FieldName":"FBillNo","Compare":">=","Value":value,"Right":")","Logic":"AND"},{"Left":"(","FieldName":"FBillNo","Compare":"<=","Value":value,"Right":")","Logic":""}], "TopRowCount": 0}))

    return res

def ERP_view(api_sdk,FNumber):


    model={
        "CreateOrgId": 0,
        "Number": FNumber,
        "Id": "",
        "IsSortBySeq": "false"
    }

    res=json.loads(api_sdk.View("PUR_PurchaseOrder",model))

    return res

def check_order_exists(api_sdk,FNumber):
    '''
    查看订单是否在ERP系统存在
    :param api: API接口对象
    :param FNumber: 订单编码
    :return:
    '''

    try:

        model={
                "CreateOrgId": 0,
                "Number": FNumber,
                "Id": "",
                "IsSortBySeq": "false"
            }

        res=json.loads(api_sdk.View("STK_InStock",model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return True

def getClassfyData(app3,code):
    '''
    获得分类数据
    :param app2:
    :param code:
    :return:
    '''

    try:

        number=code['FGODOWNNO']

        sql=f"select FGODOWNNO,FBILLNO,FPOORDERSEQ,FBILLTYPEID,FDOCUMENTSTATUS,FSUPPLIERFIELD,FCUSTOMERNUMBER,FSUPPLIERNAME,FSUPPLIERABBR,FSTOCKID,FLIBRARYSIGN,FBUSINESSDATE,FBARCODE,FGOODSID,FPRDNAME,FINSTOCKQTY,FPURCHASEPRICE,FAMOUNT,FTAXRATE,FLOT,FCHECKSTATUS,FDESCRIPTION,FUPDATETIME,FInstockId,FArrivalDate,FUPDATETIME,FIsFree,FPRODUCEDATE,FEFFECTIVEDATE from RDS_ECS_ODS_pur_storageacct where FGODOWNNO='{number}'"

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

    sql=f"update a set a.Fisdo={status} from RDS_ECS_ODS_pur_storageacct a where FGODOWNNO='{fnumber}'"

    app3.update(sql)



def checkDataExist(app2, FInstockId):
    '''
    通过FSEQ字段判断数据是否在表中存在
    :param app2:
    :param FSEQ:
    :return:
    '''
    sql = f"select FInstockId from RDS_ECS_SRC_pur_storageacct where FInstockId='{FInstockId}'"

    res = app2.select(sql)

    if res == []:

        return True

    else:

        return False


def insert_procurement_storage(app2, data):
    '''
    采购入库
    :param app2:
    :param data:
    :return:
    '''



    for i in data.index:

        if checkDataExist(app2, data.iloc[i]['FInstockId']):

            try:

                sql=f"""insert into RDS_ECS_SRC_pur_storageacct(FGODOWNNO,FBILLNO,FPOORDERSEQ,FBILLTYPEID,FDOCUMENTSTATUS,FSUPPLIERFIELD,FCUSTOMERNUMBER,FSUPPLIERNAME,FSUPPLIERABBR,FSTOCKID,FLIBRARYSIGN,FBUSINESSDATE,FBARCODE,FGOODSID,FPRDNAME,FINSTOCKQTY,FPURCHASEPRICE,FAMOUNT,FTAXRATE,FLOT,FCHECKSTATUS,FDESCRIPTION,FUPDATETIME,FInstockId,FPRODUCEDATE,FEFFECTIVEDATE) values('{data.iloc[i]['FGODOWNNO']}','{data.iloc[i]['FBILLNO']}','{data.iloc[i]['FPOORDERSEQ']}','{data.iloc[i]['FBILLTYPEID']}','{data.iloc[i]['FDOCUMENTSTATUS']}','{data.iloc[i]['FSUPPLIERFIELD']}','{data.iloc[i]['FCUSTOMERNUMBER']}','{data.iloc[i]['FSUPPLIERNAME']}','{data.iloc[i]['FSUPPLIERABBR']}','{data.iloc[i]['FSTOCKID']}','{data.iloc[i]['FLIBRARYSIGN']}','{data.iloc[i]['FBUSINESSDATE']}','{data.iloc[i]['FBARCODE']}','{data.iloc[i]['FGOODSID']}','{data.iloc[i]['FPRDNAME']}','{data.iloc[i]['FINSTOCKQTY']}','{data.iloc[i]['FPURCHASEPRICE']}','{data.iloc[i]['FAMOUNT']}','{data.iloc[i]['FTAXRATE']}','{data.iloc[i]['FLOT']}','{data.iloc[i]['FCHECKSTATUS']}','',getdate(),'{data.iloc[i]['FInstockId']}','{data.iloc[i]['FPRODUCEDATE']}','{data.iloc[i]['FEFFECTIVEDATE']}')"""

                app2.insert(sql)

            except Exception as e:

                insertLog(app2, "采购入库单数据插入SRC", data.iloc[i]['FGODOWNNO'], "数据异常，请检查数据")

        else:

            insertLog(app2, "采购入库单数据插入SRC", data.iloc[i]['FGODOWNNO'], "订单重复")




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


def iskfperiod(app2,FNumber):
    '''
    查看物料是否启用保质期
    :param app2:
    :param FNumber:
    :return:
    '''

    sql=f"select FISKFPERIOD from rds_vw_fiskfperiod where F_SZSP_SKUNUMBER='{FNumber}'"

    res=app2.select(sql)

    if res==[]:

        return ""

    else:

        return res[0]['FISKFPERIOD']

def isbatch(app2,FNumber):

    sql=f"select FISBATCHMANAGE from rds_vw_fisbatch where F_SZSP_SKUNUMBER='{FNumber}'"

    res = app2.select(sql)

    if res == []:

        return ""

    else:

        return res[0]['FISBATCHMANAGE']


def getCode(app3):
    '''
    查询出表中的编码
    :param app2:
    :return:
    '''

    sql="select distinct FGODOWNNO from RDS_ECS_ODS_pur_storageacct where FIsdo=3 and FIsFree!=1"

    res=app3.select(sql)

    return res

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

def data_splicing(app2,api_sdk,data,FNumber):
    '''
    将订单内的物料进行遍历组成一个列表，然后将结果返回给
    :param data:
    :return:
    '''

    result = Order_view(api_sdk, FNumber)

    list = []

    if result != [] and len(result) == len(data):

        index = 0

        for i in data:

            list.append(json_model(app2, i, result[index]))

            index = index + 1

        return list
    else:
        return []

def json_model(app2,model_data,value):

    try:

        if model_data['FGOODSID']=="1" or code_conversion_org(app2,"rds_vw_material","F_SZSP_SKUNUMBER",str(model_data['FGOODSID']),"104","FNUMBER")!="":

            model={
                    "FRowType": "Service" if model_data['FGOODSID']=="1" else "Standard",
                    "FMaterialId": {
                        "FNumber": "7.1.000001" if model_data['FGOODSID']=="1" else code_conversion_org(app2,"rds_vw_material","F_SZSP_SKUNUMBER",str(model_data['FGOODSID']),"104","FNUMBER")
                    },
                    # "FMaterialDesc": str(model_data['FPRDNAME']),
                    # "FUnitID": {
                    #     "FNumber": "01"
                    # },
                    "FRealQty": str(model_data['FINSTOCKQTY']),
                    # "FPriceUnitID": {
                    #     "FNumber": "01"
                    # },
                    # str(model_data['FLOT']) if db.isbatch(app2,model_data['FPRDNUMBER'])=='1' else ""
                    "FLot": {
                        "FNumber": str(model_data['FLOT']) if isbatch(app2,model_data['FGOODSID'])=='1' else ""
                    },
                    "FStockId": {
                        "FNumber": "SK01"
                    },
                    "FStockStatusId": {
                        "FNumber": "KCZT01_SYS"
                    },
                    "FGiveAway": True if float(model_data['FIsFree'])== 1 else False,
                    "FProduceDate": str(model_data['FPRODUCEDATE']) if iskfperiod(app2,model_data['FGOODSID'])=='1' else "",
                    "FNote": str(model_data['FDESCRIPTION']),
                    # "FProduceDate": "2022-10-31 00:00:00",
                    "FOWNERTYPEID": "BD_OwnerOrg",
                    "FCheckInComing": False,
                    "FIsReceiveUpdateStock": False,
                    "FPriceBaseQty": str(model_data['FINSTOCKQTY']),
                    # "FRemainInStockUnitId": {
                    #     "FNumber": "01"
                    # },
                    "FBILLINGCLOSE": False,
                    "FRemainInStockQty": str(model_data['FINSTOCKQTY']),
                    "FAPNotJoinQty": str(model_data['FINSTOCKQTY']),
                    "FRemainInStockBaseQty": str(model_data['FINSTOCKQTY']),
                    "FTaxPrice": str(model_data['FPURCHASEPRICE']),
                    "FEntryTaxRate": float(model_data['FTAXRATE'])*100,
                    "FExpiryDate": str(model_data['FEFFECTIVEDATE']) if iskfperiod(app2,model_data['FGOODSID'])=='1' else "",
                    "FOWNERID": {
                        "FNumber": "104"
                    },
                    "F_SZSP_GYSSHD":  str(model_data['FLOT']),
                    "FInStockEntry_Link": [{
                        "FInStockEntry_Link_FRuleId": "PUR_ReceiveBill-STK_InStock",
                        "FInStockEntry_Link_FSTableName": "T_PUR_ReceiveEntry",
                        "FInStockEntry_Link_FSBillId": value[2],
                        "FInStockEntry_Link_FSId": value[3],
                        "FInStockEntry_Link_FBaseUnitQtyOld": str(model_data['FINSTOCKQTY']),
                        "FInStockEntry_Link_FBaseUnitQty": str(model_data['FINSTOCKQTY']),
                        "FInStockEntry_Link_FRemainInStockBaseQtyOld": str(model_data['FINSTOCKQTY']),
                        "FInStockEntry_Link_FRemainInStockBaseQty": str(model_data['FINSTOCKQTY']),
                    }]
                }

            return model

        else:

            return {}

    except Exception as e:

        return {}

def writeSRC(startDate, endDate, app3):
    '''
    将ECS数据取过来插入SRC表中
    :param startDate:
    :param endDate:
    :return:
    '''

    url = "https://kingdee-api.bioyx.cn/dynamic/query"

    page = viewPage(url, 1, 1000, "ge", "le", "v_procurement_storage", startDate, endDate, "FBUSINESSDATE")

    for i in range(1, page + 1):
        df = ECS_post_info2(url, i, 1000, "ge", "le", "v_procurement_storage", startDate, endDate, "FBUSINESSDATE")

        insert_procurement_storage(app3, df)

    pass


def purchaseStorage(startDate,endDate):
    app2 = RdClient(token='57DEDF26-5C00-4CA9-BBF7-57ECE07E179B')
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')


    # ut.writeSRC(startDate,endDate)

    data = getCode(app3)

    if data!=[]:

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



