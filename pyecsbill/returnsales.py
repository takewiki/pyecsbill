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

            if check_outstock_exists(api_sdk,i[0]['FMRBBILLNO'])!=True:

                    model = {
                            "InterationFlags": "STK_InvCheckResult",
                            "Model": {
                                "FID": 0,
                                "FBillTypeID": {
                                    "FNUMBER": "XSTHD01_SYS"
                                },
                                "FBillNo": str(i[0]['FMRBBILLNO']),
                                "FDate": str(i[0]['OPTRPTENTRYDATE']),
                                "FSaleOrgId": {
                                    "FNumber": "104"
                                },
                                "FRetcustId": {
                                    "FNumber": code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                                },
                                "F_SZSP_Remarks": "其他",
                                "FSalesGroupID": {
                                    "FNumber": "SKYX01"
                                },
                                "FSalesManId": {
                                    "FNumber": code_conversion_org(app2, "rds_vw_salesman", "FNAME", i[0]['FSALER'],
                                                                   '104', "FNUMBER")
                                },
                                # "FHeadLocId": {
                                #     "FNumber": "BIZ202103081651391"
                                # },
                                "FTransferBizType": {
                                    "FNumber": "OverOrgSal"
                                },
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
                                    "FNumber": "BSP00040"
                                },
                                "FReceiveCustId": {
                                    "FNumber": code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                                },
                                # "FReceiveAddress": "江苏生物镇江市京口区丁卯街道经十五路99号科技园江苏金斯瑞生物科技有限公司",
                                "FSettleCustId": {
                                    "FNumber": code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                                },
                                "FPayCustId": {
                                    "FNumber": code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                                },
                                "FOwnerTypeIdHead": "BD_OwnerOrg",
                                "FIsTotalServiceOrCost": False,
                                # "FLinkPhone": "13770535847",
                                "SubHeadEntity": {
                                    "FSettleCurrId": {
                                        "FNumber": "PRE001" if i[0]['FCurrencyName']=="" else code_conversion(app2,"rds_vw_currency","FNAME",i[0]['FCurrencyName'])
                                    },
                                    "FSettleOrgId": {
                                        "FNumber": "104"
                                    },
                                    "FLocalCurrId": {
                                        "FNumber": "PRE001"
                                    },
                                    "FExchangeTypeId": {
                                        "FNumber": "HLTX01_SYS"
                                    },
                                    "FExchangeRate": 1.0
                                },
                                "FEntity": data_splicing(app2,api_sdk,i,i[0]['FMRBBILLNO'])
                            }
                        }
                    res = json.loads(api_sdk.Save("SAL_RETURNSTOCK", model))

                    if res['Result']['ResponseStatus']['IsSuccess']:

                        submit_res = ERP_submit(api_sdk, str(i[0]['FMRBBILLNO']))

                        if submit_res:

                            audit_res = ERP_Audit(api_sdk, str(i[0]['FMRBBILLNO']))

                            if audit_res:

                                insertLog(app3, "销售退货单", i[0]['FMRBBILLNO'], "数据同步成功", "1")

                                changeStatus(app3,str(i[0]['FMRBBILLNO']),"1")

                                sucess_num=sucess_num+1

                            else:
                                changeStatus(app3,str(i[0]['FMRBBILLNO']),"2")

                        else:
                            changeStatus(app3,str(i[0]['FMRBBILLNO']),"2")

                    else:

                        insertLog(app3, "销售退货单", i[0]['FMRBBILLNO'],res['Result']['ResponseStatus']['Errors'][0]['Message'],"2")

                        changeStatus(app3,str(i[0]['FMRBBILLNO']),"2")

                        erro_num=erro_num+1

                        erro_list.append(res)
            else:
                changeStatus(app3, str(i[0]['FMRBBILLNO']), "1")

        except Exception as e:

            insertLog(app3, "销售退货单", i[0]['FMRBBILLNO'],"数据异常","2")


    dict = {
        "sucessNum": sucess_num,
        "erroNum": erro_num,
        "erroList": erro_list
    }

    return dict



def check_outstock_exists(api_sdk,FNumber):
    '''
    查看订单是否在ERP系统存在
    :param api: API接口对象
    :param FNumber: 订单编码
    :return:
    '''

    model={
            "CreateOrgId": 0,
            "Number": FNumber,
            "Id": "",
            "IsSortBySeq": "false"
        }

    res=json.loads(api_sdk.View("SAL_RETURNSTOCK",model))

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

    res=json.loads(api_sdk.Submit("SAL_RETURNSTOCK",model))

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
        "IgnoreInterationFlag": ""
    }

    res = json.loads(api_sdk.Audit("SAL_RETURNSTOCK", model))

    return res['Result']['ResponseStatus']['IsSuccess']

def delivery_view(api_sdk,value):
    '''
    销售订单单据查询
    :param value: 订单编码
    :return:
    '''

    res=json.loads(api_sdk.ExecuteBillQuery({"FormId": "SAL_RETURNNOTICE", "FieldKeys": "FDate,FBillNo,FId,FEntity_FENTRYID", "FilterString": [{"Left":"(","FieldName":"FBillNo","Compare":"=","Value":value,"Right":")","Logic":"AND"}], "TopRowCount": 0}))

    return res

def getCode(app3):
    '''
    查询出表中的编码
    :param app2:
    :return:
    '''

    sql="select distinct FMRBBILLNO from RDS_ECS_ODS_sal_returnstock where FIsdo=3 and FIsFree!=1"

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

        sql=f"select FMRBBILLNO,FTRADENO,FSALEORDERENTRYSEQ,FBILLTYPE,FRETSALESTATE,FPRDRETURNSTATUS,OPTRPTENTRYDATE,FSTOCK,FCUSTNUMBER,FCUSTOMNAME,FCUSTCODE,FPrdNumber,FPrdName,FRETSALEPRICE,FRETURNQTY,FREQUESTTIME,FBUSINESSDATE,FCOSTPRICE,FMEASUREUNIT,FRETAMOUNT,FTAXRATE,FLOT,FSALER,FAUXSALER,FSUMSUPPLIERLOT,FPRODUCEDATE,FEFFECTIVEDATE,FCHECKSTATUS,UPDATETIME,FDELIVERYNO,FIsDo,FIsFree,FADDID,FCurrencyName,FReturnTime from RDS_ECS_ODS_sal_returnstock where FMRBBILLNO='{code['FMRBBILLNO']}'"

        res=app3.select(sql)

        return res

    except Exception as e:

        return []

def changeStatus(app3,fnumber,status):
    '''
    将没有写入的数据状态改为2
    :param app2: 执行sql语句对象
    :param fnumber: 订单编码
    :param status: 数据状态
    :return:
    '''

    sql=f"update a set a.FIsdo={status} from RDS_ECS_ODS_sal_returnstock a where FMRBBILLNO='{fnumber}'"

    app3.update(sql)


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


def isbatch(app2,FNumber):

    sql=f"select FISBATCHMANAGE from rds_vw_fisbatch where F_SZSP_SKUNUMBER='{FNumber}'"

    res = app2.select(sql)

    if res == []:

        return ""

    else:

        return res[0]['FISBATCHMANAGE']


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


def checkDataExist(app2, FADDID):
    '''
    通过FSEQ字段判断数据是否在表中存在
    :param app2:
    :param FSEQ:
    :return:
    '''
    sql = f"select FADDID from RDS_ECS_SRC_sal_returnstock where FADDID='{FADDID}'"

    res = app2.select(sql)

    if res == []:

        return True

    else:

        return False


def insert_sales_return(app2,data):
    '''
    销售退货
    :param app2:
    :param data:
    :return:
    '''


    for i in data.index:

        if checkDataExist(app2,data.iloc[i]['FADDID']):

            try:

                sql = f"insert into RDS_ECS_SRC_sal_returnstock(FMRBBILLNO,FTRADENO,FSALEORDERENTRYSEQ,FBILLTYPE,FRETSALESTATE,FPRDRETURNSTATUS,OPTRPTENTRYDATE,FSTOCK,FCUSTNUMBER,FCUSTOMNAME,FCUSTCODE,FPrdNumber,FPrdName,FRETSALEPRICE,FRETURNQTY,FREQUESTTIME,FBUSINESSDATE,FCOSTPRICE,FMEASUREUNIT,FRETAMOUNT,FTAXRATE,FLOT,FSALER,FAUXSALER,FSUMSUPPLIERLOT,FPRODUCEDATE,FEFFECTIVEDATE,FCHECKSTATUS,UPDATETIME,FDELIVERYNO,FIsDo,FIsFree,FADDID,FCurrencyName) values('{data.iloc[i]['FMRBBILLNO']}','{data.iloc[i]['FTRADENO']}','{data.iloc[i]['FSALEORDERENTRYSEQ']}','{data.iloc[i]['FBILLTYPEID']}','{data.iloc[i]['FRETSALESTATE']}','{data.iloc[i]['FPRDRETURNSTATUS']}','{data.iloc[i]['OPTRPTENTRYDATE']}','{data.iloc[i]['FSTOCKID']}','{data.iloc[i]['FCUSTNUMBER']}','{data.iloc[i]['FCUSTOMNAME']}','{data.iloc[i]['FCUSTCODE']}','{data.iloc[i]['FPRDNUMBER']}','{data.iloc[i]['FPRDNAME']}','{data.iloc[i]['FRETSALEPRICE']}','{data.iloc[i]['FRETURNQTY']}','{data.iloc[i]['FREQUESTTIME']}','{data.iloc[i]['FBUSINESSDATE']}','{data.iloc[i]['FCOSTPRICE']}','{data.iloc[i]['FMEASUREUNITID']}','{data.iloc[i]['FRETAMOUNT']}','{data.iloc[i]['FTAXRATE']}','{data.iloc[i]['FLOT']}','{data.iloc[i]['FSALERID']}','{data.iloc[i]['FAUXSALERID']}','{data.iloc[i]['FSUMSUPPLIERLOT']}','{data.iloc[i]['FPRODUCEDATE']}','{data.iloc[i]['FEFFECTIVEDATE']}','{data.iloc[i]['FCHECKSTATUS']}',getdate(),'{data.iloc[i]['FDELIVERYNO']}',0,0,'{data.iloc[i]['FADDID']}','{data.iloc[i]['FCURRENCYID']}')"

                app2.insert(sql)

            except Exception as e:

                insertLog(app2, "销售退货单数据插入SRC", data.iloc[i]['FMRBBILLNO'], "数据异常，请检查数据")



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

        if model_data['FPrdNumber'] == '1' or code_conversion(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", model_data['FPrdNumber'])!="":

            model = {
                    "FRowType": "Standard" if model_data['FPrdNumber'] != '1' else "Service",
                    "FMaterialId": {
                        "FNumber": "7.1.000001" if model_data['FPrdNumber'] == '1' else str(code_conversion(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", model_data['FPrdNumber']))
                    },
                    # "FUnitID": {
                    #     "FNumber": "01"
                    # },
                    "FRealQty": str(model_data['FRETURNQTY']),
                    "FTaxPrice": str(model_data['FRETSALEPRICE']),
                    "FEntryTaxRate": float(model_data['FTAXRATE']) * 100,
                    "FIsFree": True if float(model_data['FIsFree'])== 1 else False,
                    "FReturnType": {
                        "FNumber": "THLX01_SYS"
                    },
                    "FOwnerTypeId": "BD_OwnerOrg",
                    "FOwnerId": {
                        "FNumber": "104"
                    },
                    "FStockId": {
                        "FNumber": "SK01"
                    },
                    "FStockstatusId": {
                        "FNumber": "KCZT01_SYS"
                    },
                    "FLot": {
                        "FNumber": str(model_data['FLOT']) if isbatch(app2,model_data['FPrdNumber'])=='1' else ""
                    },
                    "FDeliveryDate": str(model_data['FReturnTime']),
                    # "FSalUnitID": {
                    #     "FNumber": "01"
                    # },
                    "FSalUnitQty": str(model_data['FRETURNQTY']),
                    "FSalBaseQty": str(model_data['FRETURNQTY']),
                    "FPriceBaseQty": str(model_data['FRETURNQTY']),
                    "FIsOverLegalOrg": False,
                    "FARNOTJOINQTY": str(model_data['FRETURNQTY']),
                    "FIsReturnCheck": False,
                    "F_SZSP_ReleaseFlag": False,
                    "FSettleBySon": False,
                    "FMaterialID_Sal": {
                        "FNUMBER": "7.1.000001" if model_data['FPrdNumber'] == '1' else str(code_conversion(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", model_data['FPrdNumber']))
                    },
                    "FEntity_Link": [{
                        "FEntity_Link_FRuleId ": "SalReturnNotice-SalReturnStock",
                        "FEntity_Link_FSTableName ": "T_SAL_RETURNNOTICEENTRY",
                        "FEntity_Link_FSBillId ": str(value[2]),
                        "FEntity_Link_FSId ": str(value[3]),
                        "FEntity_Link_FBaseUnitQtyOld ": str(model_data['FRETURNQTY']),
                        "FEntity_Link_FBaseUnitQty ": str(model_data['FRETURNQTY']),
                        "FEntity_Link_FSalBaseQtyOld ": str(model_data['FRETURNQTY']),
                        "FEntity_Link_FSalBaseQty ": str(model_data['FRETURNQTY']),
                        "FEntity_Link_FAuxUnitQtyOld":str(model_data['FRETURNQTY']),
                        "FEntity_Link_FAuxUnitQty":str(model_data['FRETURNQTY'])
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

    page = viewPage(url, 1, 1000, "ge", "le", "v_sales_return", startDate, endDate, "OPTRPTENTRYDATE")

    for i in range(1, page + 1):
        df = ECS_post_info2(url, i, 1000, "ge", "le", "v_sales_return", startDate, endDate, "OPTRPTENTRYDATE")

        insert_sales_return(app3, df)

    pass

def returnSale(startDate,endDate):

    app2 = RdClient(token='57DEDF26-5C00-4CA9-BBF7-57ECE07E179B')
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')

    # ut.writeSRC(startDate,endDate,app3)

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


