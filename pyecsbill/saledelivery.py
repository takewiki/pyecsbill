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

            if check_outstock_exists(api_sdk,i[0]['FDELIVERYNO'])!=True:

                    model = {
                        "Model": {
                            "FID": 0,
                            "FBillTypeID": {
                                "FNUMBER": "XSCKD01_SYS"
                            },
                            "FBillNo": str(i[0]['FDELIVERYNO']),
                            "FDate": str(i[0]['FDELIVERDATE']),
                            "FSaleOrgId": {
                                "FNumber": "104"
                            },
                            "FCustomerID": {
                                "FNumber": "C003142"if i[0]['FCUSTOMNAME']=="苏州亚通生物医疗科技有限公司" else code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                            },
                            "FSaleDeptID": {
                                "FNumber": "BM000036"
                            },
                            "FSalesManID": {
                                "FNumber": code_conversion_org(app2,"rds_vw_salesman","FNAME",i[0]['FSALER'],'104',"FNUMBER")
                            },
                            "FReceiverID": {
                                "FNumber": "C003142" if i[0]['FCUSTOMNAME']=="苏州亚通生物医疗科技有限公司" else code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                            },
                            "FSalesGroupID": {
                                "FNumber": "SKYX01"
                            },
                            "FStockOrgId": {
                                "FNumber": "104"
                            },
                            "FDeliveryDeptID": {
                                "FNumber": "BM000040"
                            },
                            "FStockerGroupID": {
                                "FNumber": "SKCKZ01"
                            },
                            "FStockerID": {
                                "FNumber": "BSP00040"
                            },
                            "FSettleID": {
                                "FNumber": "C003142" if i[0]['FCUSTOMNAME']=="苏州亚通生物医疗科技有限公司" else code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                            },
                            "FPayerID": {
                                "FNumber": "C003142" if i[0]['FCUSTOMNAME']=="苏州亚通生物医疗科技有限公司" else code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                            },
                            "FOwnerTypeIdHead": "BD_OwnerOrg",
                            "FIsTotalServiceOrCost": False,
                            "F_SZSP_XSLX": {
                                "FNumber": "1"
                            },
                            "SubHeadEntity": {
                                "FSettleCurrID": {
                                    "FNumber": "PRE001" if i[0]['FCurrencyName']=="" else code_conversion(app2,"rds_vw_currency","FNAME",i[0]['FCurrencyName'])
                                },
                                "FSettleOrgID": {
                                    "FNumber": "104"
                                },
                                "FIsIncludedTax": True,
                                "FLocalCurrID": {
                                    "FNumber": "PRE001"
                                },
                                "FExchangeTypeID": {
                                    "FNumber": "HLTX01_SYS"
                                },
                                "FExchangeRate": 1.0,
                                "FIsPriceExcludeTax": True
                            },
                            "FEntity": data_splicing(app2, api_sdk,i,str(i[0]['FDELIVERYNO']))
                        }
                    }

                    res = json.loads(api_sdk.Save("SAL_OUTSTOCK", model))

                    if res['Result']['ResponseStatus']['IsSuccess']:

                        submit_res = ERP_submit(api_sdk, str(i[0]['FDELIVERYNO']))

                        if submit_res:

                            audit_res = ERP_Audit(api_sdk, str(i[0]['FDELIVERYNO']))

                            if audit_res:

                                insertLog(app3, "销售出库单", str(i[0]['FDELIVERYNO']), "数据同步成功", "1")

                                changeStatus(app3,str(i[0]['FDELIVERYNO']),"1")

                                sucess_num=sucess_num+1

                            else:
                                changeStatus(app3,str(i[0]['FDELIVERYNO']),"2")

                        else:
                            changeStatus(app3,str(i[0]['FDELIVERYNO']),"2")

                    else:

                        insertLog(app3, "销售出库单", str(i[0]['FDELIVERYNO']),res['Result']['ResponseStatus']['Errors'][0]['Message'],"2")

                        changeStatus(app3,str(i[0]['FDELIVERYNO']),"2")

                        erro_num=erro_num+1

                        erro_list.append(res)


            else:

                changeStatus(app3, str(i[0]['FDELIVERYNO']), "1")

        except Exception as e:

            insertLog(app3, "销售出库单", str(i[0]['FDELIVERYNO']),"数据异常","2")

    dict = {
        "sucessNum": sucess_num,
        "erroNum": erro_num,
        "erroList": erro_list
    }
    return dict


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

        res=json.loads(api_sdk.Submit("SAL_OUTSTOCK",model))

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
            "InterationFlags": "STK_InvCheckResult",
            "NetworkCtrl": "",
            "IsVerifyProcInst": "",
            "IgnoreInterationFlag": ""
        }

        res = json.loads(api_sdk.Audit("SAL_OUTSTOCK", model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False



def check_deliveryExist(api_sdk, FNumber):

    try:

        model = {
            "CreateOrgId": 0,
            "Number": FNumber,
            "Id": "",
            "IsSortBySeq": "false"
        }

        res = json.loads(api_sdk.View("SAL_DELIVERYNOTICE", model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return True

def delivery_view(api_sdk,value):
    '''
    销售订单单据查询
    :param value: 订单编码
    :return:
    '''

    res=json.loads(api_sdk.ExecuteBillQuery({"FormId": "SAL_DELIVERYNOTICE", "FieldKeys": "FDate,FBillNo,FId,FEntity_FEntryID", "FilterString": [{"Left":"(","FieldName":"FBillNo","Compare":"=","Value":value,"Right":")","Logic":"AND"}], "TopRowCount": 0}))

    return res

def check_outstock_exists(api_sdk,FNumber):
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

        res=json.loads(api_sdk.View("SAL_OUTSTOCK",model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return True

def getCode(app3):
    '''
    查询出表中的编码
    :param app2:
    :return:
    '''

    sql="select distinct FDELIVERYNO from RDS_ECS_ODS_sal_delivery where FIsdo=3 and FIsFree!=1"

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
        number=code['FDELIVERYNO']

        sql=f"select FInterID,FDELIVERYNO,FTRADENO,FBILLTYPE,FDELIVERYSTATUS,FDELIVERDATE,FSTOCK,FCUSTNUMBER,FCUSTOMNAME,FORDERTYPE,FPRDNUMBER,FPRDNAME,FPRICE,FNBASEUNITQTY,FLOT,FSUMSUPPLIERLOT,FPRODUCEDATE,FEFFECTIVEDATE,FMEASUREUNIT,DELIVERYAMOUNT,FTAXRATE,FSALER,FAUXSALER,Fisdo,FArStatus,FIsfree,UPDATETIME,FOUTID,FCurrencyName from RDS_ECS_ODS_sal_delivery where FDELIVERYNO='{number}'"

        res=app3.select(sql)

        return res
    except Exception as e:

        return []

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


def changeStatus(app3,fnumber,status):
    '''
    将没有写入的数据状态改为2
    :param app2: 执行sql语句对象
    :param fnumber: 订单编码
    :param status: 数据状态
    :return:
    '''

    sql=f"update a set a.Fisdo={status} from RDS_ECS_ODS_sal_delivery a where FDELIVERYNO='{fnumber}'"

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

def getFinterId(app2, tableName):
    '''
    在两张表中找到最后一列数据的索引值
    :param app2: sql语句执行对象
    :param tableName: 要查询数据对应的表名表名
    :return:
    '''

    try:

        sql = f"select isnull(max(FInterId),0) as FMaxId from {tableName}"

        res = app2.select(sql)

        return res[0]['FMaxId']

    except Exception as e:

        return 0


def checkDataExist(app2, FOUTID):
    '''
    通过FSEQ字段判断数据是否在表中存在
    :param app2:
    :param FSEQ:
    :return:
    '''
    sql = f"select FOUTID from RDS_ECS_SRC_sal_delivery where FOUTID='{FOUTID}'"

    res = app2.select(sql)

    if res == []:

        return True

    else:

        return False

def insert_sales_delivery(app2,app3,data):
    '''
    销售发货
    :param app2:
    :param data:数据源
    :return:
    '''


    for i in data.index:

        if data.loc[i]['FNBASEUNITQTY']!=0 and checkDataExist(app3,data.loc[i]['FOUTID']):

            if judgementData(app2,app3,data[data['FDELIVERYNO']==data.loc[i]['FDELIVERYNO']]):

                inert_data(app3,data[data['FDELIVERYNO']==data.loc[i]['FDELIVERYNO']])




def isbatch(app2,FNumber):

    sql=f"select FISBATCHMANAGE from rds_vw_fisbatch where F_SZSP_SKUNUMBER='{FNumber}'"

    res = app2.select(sql)

    if res == []:

        return ""

    else:

        return res[0]['FISBATCHMANAGE']


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

def judgementData(app2, app3, data):
    '''
    判断数据是否合规
    :param app2:
    :param data:
    :return:
    '''

    flag = True

    for i in data.index:
        if code_conversion(app2, "rds_vw_customer", "FNAME", data.loc[i]['FCUSTOMNAME']) != "":

            if code_conversion(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", data.loc[i]['FPRDNUMBER']) != "" or \
                    data.loc[i]['FPRDNUMBER'] == "1":

                if (iskfperiod(app2, data.loc[i]['FPRDNUMBER']) == "1" and data.loc[i]['FPRODUCEDATE'] != "") or \
                        data.loc[i]['FPRDNUMBER'] == "1" or (iskfperiod(app2, data.loc[i]['FGOODSID']) == "0"):

                    continue

                else:

                    insertLog(app3, "销售出库单", data.loc[i]['FDELIVERYNO'], "生产日期和有效期不能为空","2")

                    flag = False

                    break

            else:

                insertLog(app3, "销售出库单", data.loc[i]['FDELIVERYNO'], "物料不存在","2")

                flag = False

                break
        else:

            insertLog(app3, "销售出库单", data.loc[i]['FDELIVERYNO'], "客户不存在","2")

            flag = False

            break

    return flag


def inert_data(app3,data):

    for i in data.index:

        try:

            sql = f"insert into RDS_ECS_SRC_sal_delivery(FInterID,FDELIVERYNO,FTRADENO,FBILLTYPE,FDELIVERYSTATUS,FDELIVERDATE,FSTOCK,FCUSTNUMBER,FCUSTOMNAME,FORDERTYPE,FPRDNUMBER,FPRDNAME,FPRICE,FNBASEUNITQTY,FLOT,FSUMSUPPLIERLOT,FPRODUCEDATE,FEFFECTIVEDATE,FMEASUREUNIT,DELIVERYAMOUNT,FTAXRATE,FSALER,FAUXSALER,Fisdo,FArStatus,FIsfree,UPDATETIME,FOUTID,FCurrencyName) values({int(getFinterId(app3, 'RDS_ECS_SRC_sal_delivery')) + 1},'{data.loc[i]['FDELIVERYNO']}','{data.loc[i]['FTRADENO']}','{data.loc[i]['FBILLTYPEID']}','{data.loc[i]['FDELIVERYSTATUS']}','{data.loc[i]['FDELIVERDATE']}','{data.loc[i]['FSTOCKID']}','{data.loc[i]['FCUSTNUMBER']}','{data.loc[i]['FCUSTOMNAME']}','{data.loc[i]['FORDERTYPE']}','{data.loc[i]['FPRDNUMBER']}','{data.loc[i]['FPRDNAME']}','{data.loc[i]['FPRICE']}','{data.loc[i]['FNBASEUNITQTY']}','{data.loc[i]['FLOT']}','{data.loc[i]['FSUMSUPPLIERLOT']}','{data.loc[i]['FPRODUCEDATE']}','{data.loc[i]['FEFFECTIVEDATE']}','{data.loc[i]['FMEASUREUNITID']}','{data.loc[i]['DELIVERYAMOUNT']}','{data.loc[i]['FTAXRATE']}','{data.loc[i]['FSALERID']}','{data.loc[i]['FAUXSALERID']}',0,0,0,getdate(),'{data.loc[i]['FOUTID']}','{data.loc[i]['FCURRENCYID']}')"

            app3.insert(sql)

            insertLog(app3, "销售出库单", data.loc[i]['FDELIVERYNO'], "数据插入成功", "1")

        except Exception as e:

            insertLog(app3, "销售出库单", data.loc[i]['FDELIVERYNO'], "插入SRC数据异常，请检查数据","2")

    pass

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
    将订单内的物料进行遍历组成一个列表，然后将结果返回给 FSaleOrderEntry
    :param data:
    :return:
    '''

    result=delivery_view(api_sdk,FNumber)

    list=[]

    if result != [] and len(result)==len(data):

        index=0

        for i in data:

            list.append(json_model(app2,i,result[index]))

            index=index+1

        return list
    else:
        return []


def json_model(app2,model_data,value):

    try:

        if model_data['FPRDNUMBER'] == '1' or str(code_conversion(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", model_data['FPRDNUMBER']))!="":

            model = {
                "FRowType": "Standard" if model_data['FPRDNUMBER'] != '1' else "Service",
                "FMaterialID": {
                    "FNumber": "7.1.000001" if model_data['FPRDNUMBER'] == '1' else str(code_conversion(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", model_data['FPRDNUMBER']))
                },
                "FRealQty": str(model_data['FNBASEUNITQTY']),
                "FTaxPrice": str(model_data['FPRICE']),
                "FIsFree": True if model_data['FIsfree']==1 else False,
                "FOwnerTypeID": "BD_OwnerOrg",
                "FOwnerID": {
                    "FNumber": "104"
                },
                "FLot": {
                    "FNumber": str(model_data['FLOT']) if isbatch(app2,model_data['FPRDNUMBER'])=='1' else ""
                },
                "FProduceDate":  str(model_data['FPRODUCEDATE']) if iskfperiod(app2,model_data['FPRDNUMBER'])=='1' else "",
                "FExpiryDate": str(model_data['FEFFECTIVEDATE']) if iskfperiod(app2,model_data['FPRDNUMBER'])=='1' else "",
                "FEntryTaxRate": float(model_data['FTAXRATE']) * 100,
                "FStockID": {
                    "FNumber": "SK01"
                },
                "FStockStatusID": {
                    "FNumber": "KCZT01_SYS"
                },
                "FSALUNITQTY": str(model_data['FNBASEUNITQTY']),
                "FSALBASEQTY": str(model_data['FNBASEUNITQTY']),
                "FPRICEBASEQTY": str(model_data['FNBASEUNITQTY']),
                "FOUTCONTROL": False,
                "FIsOverLegalOrg": False,
                "FARNOTJOINQTY": str(model_data['FNBASEUNITQTY']),
                "FCheckDelivery": False,
                "FSettleBySon": False,
                "FMaterialID_Sal": {
                    "FNUMBER": "7.1.000001" if model_data['FPRDNUMBER'] == '1' else str(code_conversion(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", model_data['FPRDNUMBER']))
                },
                "FEntity_Link": [{
                    "FEntity_Link_FRuleId ": "DeliveryNotice-OutStock",
                    "FEntity_Link_FSTableName ": "T_SAL_DELIVERYNOTICEENTRY",
                    "FEntity_Link_FSBillId ": str(value[2]),
                    "FEntity_Link_FSId ": str(value[3]),
                    "FEntity_Link_FBaseUnitQtyOld ": str(model_data['FNBASEUNITQTY']),
                    "FEntity_Link_FBaseUnitQty ": str(model_data['FNBASEUNITQTY']),
                    "FEntity_Link_FSALBASEQTYOld ": str(model_data['FNBASEUNITQTY']),
                    "FEntity_Link_FSALBASEQTY ": str(model_data['FNBASEUNITQTY']),
                    "FEntity_Link_FAuxUnitQtyOld":str(model_data['FNBASEUNITQTY']),
                    "FEntity_Link_FAuxUnitQty":str(model_data['FNBASEUNITQTY'])
                }]
            }

            return model
        else:
            {}

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

    page = viewPage(url, 1, 1000, "ge", "le", "v_sales_delivery", startDate, endDate, "FDELIVERDATE")

    for i in range(1, page + 1):

        df = ECS_post_info2(url, i, 1000, "ge", "le", "v_sales_delivery", startDate, endDate, "FDELIVERDATE")

        df = df.replace("Lab'IN Co.", "")

        df = df.fillna("")

        insert_sales_delivery(app2,app3, df)

def saleOut(startDate,endDate):

    app2 = RdClient(token='57DEDF26-5C00-4CA9-BBF7-57ECE07E179B')
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')


    # ut.writeSRC(startDate,endDate,app2,app3)

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



