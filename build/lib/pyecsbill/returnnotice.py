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

    # token = f'accessId=skyx&accessKey=skyx@0512@1024&pageNum={pageNum}&pageSize={pageSize}&queryList={queryList}&tableName={tableName}'

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

        #url = f"http://10.3.1.99:8107/customer/getCustomerList?startDate={startDate}&endDate={endDate}&token={md5}"

        #url = "https://test-kingdee-api.bioyx.cn/dynamic/query"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(url, headers=headers,data=data)

        info = response.json()
        #
        # print(info)

        # col=["FSALEORDERNO","FBILLTYPEIDNAME","FSALEDATE","FCUSTCODE","FCUSTOMNAME","FSALEORDERENTRYSEQ","FPRDNUMBER","FPRDNAME","FQTY","FPRICE","FMONEY","FTAXRATE","FTAXAMOUNT","FTAXPRICE","FAMOUNT","FSALDEPTID","FSALGROUPID","FSALERID","FDESCRIPTION"]
        #
        # df = pd.DataFrame(info['data']['list'],columns=col)

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

        #url = f"http://10.3.1.99:8107/customer/getCustomerList?startDate={startDate}&endDate={endDate}&token={md5}"

        #url = "https://test-kingdee-api.bioyx.cn/dynamic/query"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(url, headers=headers,data=data)

        info = response.json()

        # print(info)

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

            if check_deliveryExist(api_sdk,i[0]['FMRBBILLNO'])!=True:

                model={

                        "InterationFlags": "STK_InvCheckResult",
                        "Model": {
                            "FID": 0,
                            "FBillTypeID": {
                                "FNUMBER": "THTZD01_SYS"
                            },
                            "FBillNo": str(i[0]['FMRBBILLNO']),
                            "FDate": str(i[0]['OPTRPTENTRYDATE']),
                            "FApproveDate": str(i[0]['OPTRPTENTRYDATE']),
                            "FSaleOrgId": {
                                "FNumber": "104"
                            },
                            "FRetcustId": {
                                "FNumber": code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                            },
                            "F_SZSP_Remarks": "其他",
                            # "FHeadLocId": {
                            #     "FNumber": "BIZ202103081651391"
                            # },
                            "FSalesGroupID": {
                                "FNumber": "SKYX01"
                            },
                            "FSalesManId": {
                                "FNumber": code_conversion_org(app2,"rds_vw_salesman","FNAME",i[0]['FSALER'],'104',"FNUMBER")
                            },
                            "FRetorgId": {
                                "FNumber": "104"
                            },
                            "FRetDeptId": {
                                "FNumber": "BM000040"
                            },
                            "FStockerGroupId": {
                                "FNumber": "SKCKZ01"
                            },
                            "FStockerId": {
                                "FNAME": "刘想良"
                            },
                            "FReceiveCusId": {
                                "FNumber": code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                            },
                            "FSettleCusId": {
                                "FNumber": code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                            },
                            "FPayCusId": {
                                "FNumber": code_conversion(app2,"rds_vw_customer","FNAME",i[0]['FCUSTOMNAME'])
                            },
                            "FOwnerTypeIdHead": "BD_OwnerOrg",
                            "FManualClose": False,
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
                            "FEntity": data_splicing(app2,api_sdk,i)
                        }
                    }


                res=json.loads(api_sdk.Save("SAL_RETURNNOTICE",model))

                if res['Result']['ResponseStatus']['IsSuccess']:

                    FNumber = res['Result']['ResponseStatus']['SuccessEntitys'][0]['Number']

                    submit_res=ERP_submit(api_sdk,FNumber)

                    if submit_res:

                        audit_res=ERP_Audit(api_sdk,FNumber)

                        if audit_res:

                            insertLog(app3, "退货通知单", i[0]['FMRBBILLNO'], "数据同步成功", "1")

                            changeStatus(app3,str(i[0]['FMRBBILLNO']),'3')

                            sucess_num=sucess_num+1
                            pass

                        else:
                            pass
                    else:
                        pass
                else:

                    insertLog(app3, "退货通知单", i[0]['FMRBBILLNO'],res['Result']['ResponseStatus']['Errors'][0]['Message'],"2")

                    changeStatus(app3,str(i[0]['FMRBBILLNO']),'2')

                    erro_num=erro_num+1

                    erro_list.append(res)

        except Exception as e:

            insertLog(app3, "退货通知单", i[0]['FMRBBILLNO'],"数据异常","2")


    dict = {
        "sucessNum": sucess_num,
        "erroNum": erro_num,
        "erroList": erro_list
    }
    return dict


def check_deliveryExist(api_sdk,FNumber):

    try:

        model={
            "CreateOrgId": 0,
            "Number": FNumber,
            "Id": "",
            "IsSortBySeq": "false"
        }

        res=json.loads(api_sdk.View("SAL_RETURNNOTICE",model))

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

        res=json.loads(api_sdk.Submit("SAL_RETURNNOTICE",model))

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
            "IgnoreInterationFlag": "",
        }

        res = json.loads(api_sdk.Audit("SAL_RETURNNOTICE", model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False

def saleOrder_view(api_sdk,value,materialID):
    '''
    销售出库单单据查询
    :param value: 订单编码
    :return:
    '''

    res=json.loads(api_sdk.ExecuteBillQuery({"FormId": "SAL_OUTSTOCK", "FieldKeys": "FDate,FBillNo,FId,FEntity_FENTRYID,FMaterialID", "FilterString": [{"Left":"(","FieldName":"FMaterialID","Compare":"=","Value":materialID,"Right":")","Logic":"AND"},{"Left":"(","FieldName":"FBillNo","Compare":"=","Value":value,"Right":")","Logic":"AND"}], "TopRowCount": 0}))

    return res


def getCode(app3):
    '''
    查询出表中的编码
    :param app2:
    :return:
    '''

    sql="select distinct FMRBBILLNO from RDS_ECS_ODS_sal_returnstock where FIsdo=0 and FIsFree!=1"

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


def code_conversion(app2, tableName, param, param2):
    '''
    通过ECS物料编码来查询系统内的编码
    :param app2: 数据库操作对象
    :param tableName: 表名
    :param param:  参数1
    :param param2: 参数2
    :return:
    '''

    sql = f"select FNumber from {tableName} where {param}='{param2}'"

    res = app2.select(sql)

    if res == []:

        return ""

    else:

        return res[0]['FNumber']


def code_conversion_org(app2, tableName, param, param2, param3, param4):
    '''
    通过ECS物料编码来查询系统内的编码
    :param app2: 数据库操作对象
    :param tableName: 表名
    :param param:  参数1
    :param param2: 参数2
    :return:
    '''

    sql = f"select {param4} from {tableName} where {param}='{param2}' and FOrgNumber='{param3}'"

    res = app2.select(sql)

    if res == []:

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

    sql=f"update a set a.Fisdo={status} from RDS_ECS_ODS_sal_returnstock a where FMRBBILLNO='{fnumber}'"

    app3.update(sql)

def isbatch(app2,FNumber):
    '''
    判断是否启用批号管理
    :param app2:
    :param FNumber:
    :return:
    '''

    sql=f"select FISBATCHMANAGE from rds_vw_fisbatch where F_SZSP_SKUNUMBER='{FNumber}'"

    res = app2.select(sql)

    if res == []:

        return ""

    else:

        return res[0]['FISBATCHMANAGE']


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


def insert_sales_return(app2,app3,data):
    '''
    销售退货
    :param app2:
    :param data:
    :return:
    '''



    for i in data.index:

        if checkDataExist(app3,data.iloc[i]['FADDID']):

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
        if code_conversion(app2, "rds_vw_customer", "FNAME", data.loc[i]['FCUSTOMNAME']) != "":

            if code_conversion(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", data.loc[i]['FPRDNUMBER']) != "" or \
                    data.loc[i]['FPRDNUMBER'] == "1":

                if (iskfperiod(app2, data.loc[i]['FPRDNUMBER']) == "1" and data.loc[i]['FPRODUCEDATE'] != "") or \
                        (data.loc[i]['FPRDNUMBER'] == "1") or (iskfperiod(app2, data.loc[i]['FPRDNUMBER']) == "0"):

                    continue

                else:

                    insertLog(app3, "退货通知单",data.loc[i]['FMRBBILLNO'], "生产日期和有效期不能为空","2")

                    flag = False

                    break

            else:

                insertLog(app3, "退货通知单",data.loc[i]['FMRBBILLNO'], "物料不存在","2")

                flag = False

                break
        else:

            insertLog(app3, "退货通知单",data.loc[i]['FMRBBILLNO'], "客户不存在","2")

            flag = False

            break

    return flag


def inert_data(app3,data):

    for i in data.index:

        try:

            sql = f"""insert into RDS_ECS_SRC_sal_returnstock(FMRBBILLNO,FTRADENO,FSALEORDERENTRYSEQ,FBILLTYPE,FRETSALESTATE,FPRDRETURNSTATUS,OPTRPTENTRYDATE,FSTOCK,FCUSTNUMBER,FCUSTOMNAME,FCUSTCODE,FPrdNumber,FPrdName,FRETSALEPRICE,FRETURNQTY,FREQUESTTIME,FBUSINESSDATE,FCOSTPRICE,FMEASUREUNIT,FRETAMOUNT,FTAXRATE,FLOT,FSALER,FAUXSALER,FSUMSUPPLIERLOT,FPRODUCEDATE,FEFFECTIVEDATE,FCHECKSTATUS,UPDATETIME,FDELIVERYNO,FIsDo,FIsFree,FADDID,FCurrencyName) values('{data.loc[i]['FMRBBILLNO']}','{data.loc[i]['FTRADENO']}','{data.loc[i]['FSALEORDERENTRYSEQ']}','{data.loc[i]['FBILLTYPEID']}','{data.loc[i]['FRETSALESTATE']}','{data.loc[i]['FPRDRETURNSTATUS']}','{data.loc[i]['OPTRPTENTRYDATE']}','{data.loc[i]['FSTOCKID']}','{data.loc[i]['FCUSTNUMBER']}','{data.loc[i]['FCUSTOMNAME']}','{data.loc[i]['FCUSTCODE']}','{data.loc[i]['FPRDNUMBER']}','{data.loc[i]['FPRDNAME']}','{data.loc[i]['FRETSALEPRICE']}','{data.loc[i]['FRETURNQTY']}','{data.loc[i]['FREQUESTTIME']}','{data.loc[i]['FBUSINESSDATE']}','{data.loc[i]['FCOSTPRICE']}','{data.loc[i]['FMEASUREUNITID']}','{data.loc[i]['FRETAMOUNT']}','{data.loc[i]['FTAXRATE']}','{data.loc[i]['FLOT']}','{data.loc[i]['FSALERID']}','{data.loc[i]['FAUXSALERID']}','{data.loc[i]['FSUMSUPPLIERLOT']}','{data.loc[i]['FPRODUCEDATE']}','{data.loc[i]['FEFFECTIVEDATE']}','{data.loc[i]['FCHECKSTATUS']}','{data.loc[i]['UPDATETIME']}','{data.loc[i]['FDELIVERYNO']}',0,0,'{data.loc[i]['FADDID']}','{data.loc[i]['FCURRENCYID']}')"""

            app3.insert(sql)

            insertLog(app3, "退货通知单", data.loc[i]['FMRBBILLNO'], "数据插入成功", "1")

        except Exception as e:

            insertLog(app3, "退货通知单",data.loc[i]['FMRBBILLNO'], "插入SRC数据异常，请检查数据","2")

    pass



def checkFlot(app2,FBillNo,FLot,REALQTY,FSKUNUM):
    '''
    查看批号
    :return:
    '''

    try:

        sql=f"""
            select a.Fid,b.FENTRYID,b.FLOT,b.FLOT_TEXT,c.F_SZSP_SKUNUMBER,d.FTAXPRICE from T_SAL_OUTSTOCK a
            inner join T_SAL_OUTSTOCKENTRY b
            on a.FID=b.FID
            inner join T_SAL_OUTSTOCKENTRY_F d
            on d.FENTRYID=b.FENTRYID
            inner join rds_vw_material c
            on c.FMATERIALID=b.FMATERIALID
            where a.FBILLNO='{FBillNo}' and FLOT_TEXT='{FLot}'and c.F_SZSP_SKUNUMBER='{FSKUNUM}' 
        """

        res=app2.select(sql)

        if res:

            return res

        else:

            return []

    except Exception as e:

        return []

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

    index = 0

    for i in data:

        materialSKU = "" if str(i['FPrdNumber']) == '1' else str(i['FPrdNumber'])

        result = checkFlot(app2, str(i['FDELIVERYNO']), str(i['FLOT']),
                              str(i['FRETURNQTY']), str(materialSKU))

        res=json_model(app2,i,api_sdk,index, result, materialSKU)

        if res:

            list.append(res)
        else:
            return []

    return list


def json_model(app2,model_data,api_sdk,index, result, materialSKU):

    try:

        # materialSKU="7.1.000001" if str(model_data['FPrdNumber'])=='1' else str(model_data['FPrdNumber'])
        materialId=code_conversion_org(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", materialSKU,"104","FMATERIALID")

        if materialSKU=="7.1.000001":

            materialId="466653"

        # result=mt.saleOrder_view(api_sdk,str(model_data['FDELIVERYNO']),materialId)

        if result!=[] and materialId!="":

            model={
                    "FRowType": "Standard" if model_data['FPrdNumber']!='1' else "Service",
                    "FMaterialId": {
                        "FNumber": "7.1.000001" if model_data['FPrdNumber']=='1' else str(code_conversion(app2,"rds_vw_material","F_SZSP_SKUNUMBER",model_data['FPrdNumber']))
                    },
                    # "FUnitID": {
                    #     "FNumber": "01"
                    # },
                    "FQty": str(model_data['FRETURNQTY']),
                    "FPRODUCEDATE": str(model_data['FPRODUCEDATE']) if iskfperiod(app2,model_data['FPrdNumber'])=='1' else "",
                    "FExpiryDate": str(model_data['FEFFECTIVEDATE']) if iskfperiod(app2,model_data['FPrdNumber'])=='1' else "",
                    "FTaxPrice": str(model_data['FRETSALEPRICE']),
                    "FEntryTaxRate": float(model_data['FTAXRATE']) * 100,
                    "FLot": {
                        "FNumber": str(model_data['FLOT']) if isbatch(app2,model_data['FPrdNumber'])=='1' else ""
                    },
                    "FPriceBaseQty": str(model_data['FRETURNQTY']),
                    # "FASEUNITID": {
                    #     "FNumber": "01"
                    # },
                    "FDeliverydate": str(model_data['FReturnTime']),
                    "FStockId": {
                        "FNumber": "SK01"
                    },
                    "FRmType": {
                        "FNumber": "THLX01_SYS"
                    },
                    "FIsReturnCheck": True,
                    # "FStockUnitID": {
                    #     "FNumber": "01"
                    # },
                    "FStockQty": str(model_data['FRETURNQTY']),
                    "FStockBaseQty": str(model_data['FRETURNQTY']),
                    "FOwnerTypeID": "BD_OwnerOrg",
                    "FOwnerID": {
                        "FNumber": "104"
                    },
                    "FRefuseFlag": False,
                    "FEntity_Link": [{
                        "FEntity_Link_FRuleId":"OutStock-SalReturnNotice",
                        "FEntity_Link_FSTableName": "T_SAL_OUTSTOCKENTRY",
                        "FEntity_Link_FSBillId": result[index]['Fid'],
                        "FEntity_Link_FSId": result[index]['FENTRYID'],
                        "FEntity_Link_FBaseUnitQtyOld ": str(model_data['FRETURNQTY']),
                        "FEntity_Link_FBaseUnitQty ": str(model_data['FRETURNQTY']),
                        "FEntity_Link_FStockBaseQtyOld ": str(model_data['FRETURNQTY']),
                        "FEntity_Link_FStockBaseQty ": str(model_data['FRETURNQTY']),
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

    page = viewPage(url, 1, 1000, "ge", "le", "v_sales_return", startDate, endDate, "UPDATETIME")

    for i in range(1, page + 1):
        df = ECS_post_info2(url, i, 1000, "ge", "le", "v_sales_return", startDate, endDate, "UPDATETIME")

        df=df.fillna("")

        insert_sales_return(app2,app3, df)

    pass


def returnNotice(startDate,endDate):
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



