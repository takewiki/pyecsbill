#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import requests
import hashlib
from pyrda.dbms.rds import RdClient
from k3cloud_webapi_sdk.main import K3CloudApiSdk
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


def ERP_Save(api_sdk,data,option,app2,app3):

    '''
    调用ERP保存接口
    :param api_sdk: 调用ERP对象
    :param data:  要插入的数据
    :param option: ERP密钥
    :param app2: 数据库执行对象
    :return:
    '''



    api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                       option['app_sec'], option['server_url'])

    erro_list = []
    sucess_num = 0
    erro_num = 0

    for i in data:

        try:

            FNo = str(i[0]['FINVOICENO'])[0:8]

            if check_order_exists(api_sdk,FNo)!=True:

                    model={
                        "Model": {
                            "FID": 0,
                            "FBillTypeID": {
                                "FNUMBER": "YFD01_SYS"
                            },
                            "FBillNo": FNo,
                            "FISINIT": False,
                            "FDATE": str(i[0]['FINVOICEDATE']),
                            "FDOCUMENTSTATUS": "Z",
                            "FSUPPLIERID": {
                                "FNumber": code_conversion_org(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'],"104","FNUMBER")
                            },
                            "FCURRENCYID": {
                                "FNumber": "PRE001"
                            },
                            "FPayConditon": {
                                "FNumber": "003"
                            },
                            "FISPRICEEXCLUDETAX": True,
                            "FBUSINESSTYPE": "CG",
                            "FISTAX": True,
                            "FSETTLEORGID": {
                                "FNumber": "104"
                            },
                            "FPAYORGID": {
                                "FNumber": "104"
                            },
                            "FSetAccountType": "1",
                            "FISTAXINCOST": False,
                            "FISHookMatch": False,
                            "FPURCHASEDEPTID": {
                                "FNumber": "BM000040"
                            },
                            "FPURCHASERID": {
                                "FNumber": code_conversion(app2,"rds_vw_buyer","FNAME",str(i[0]['FPURCHASERINAME']))
                            },
                            "FCancelStatus": "A",
                            "FISBYIV": False,
                            "FISGENHSADJ": False,
                            "FISINVOICEARLIER": False,
                            "FWBOPENQTY": False,
                            "FIsGeneratePlanByCostItem": False,
                            "F_SZSP_FPHM": FNo,
                            "FsubHeadSuppiler": {
                                "FORDERID": {
                                    "FNumber": code_conversion_org(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'],"104","FNUMBER")
                                },
                                "FTRANSFERID": {
                                    "FNumber": code_conversion_org(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'],"104","FNUMBER")
                                },
                                "FChargeId": {
                                    "FNumber": code_conversion_org(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'],"104","FNUMBER")
                                }
                            },
                            "FsubHeadFinc": {
                                "FMAINBOOKSTDCURRID": {
                                    "FNumber": "PRE001"
                                },
                                "FEXCHANGETYPE": {
                                    "FNumber": "HLTX01_SYS"
                                },
                                "FExchangeRate": 1.0,
                                "FISCARRIEDDATE": False
                            },
                            "FEntityDetail": data_splicing(app2,api_sdk,i)
                        }
                    }

                    save_result=json.loads(api_sdk.Save("AP_Payable",model))
                    if save_result['Result']['ResponseStatus']['IsSuccess']:

                        FNumber = save_result['Result']['ResponseStatus']['SuccessEntitys'][0]['Number']

                        submit_res = ERP_submit(api_sdk, FNumber)

                        insertLog(app3, "应付单", str(i[0]['FINVOICENO']), "数据同步成功", "1")

                        changeStatus(app3, str(i[0]['FINVOICENO']), "1")

                        sucess_num=sucess_num+1

                        # if submit_res:
                        #
                        #     audit_res = ERP_Audit(api_sdk, FNumber)
                        #
                        #     if audit_res:
                        #
                        #         db.changeStatus(app3, str(i[0]['FBILLNO']), "3")
                        #
                        #     else:
                        #         pass
                        # else:
                        #     pass
                    else:

                        insertLog(app3, "应付单", str(i[0]['FINVOICENO']), save_result['Result']['ResponseStatus']['Errors'][0]['Message'],"2")

                        changeStatus(app3, str(i[0]['FINVOICENO']), "2")
                        erro_num=erro_num+1
                        erro_list.append(save_result)

        except Exception as e:

            insertLog(app3, "应付单", str(i[0]['FINVOICENO']), "数据异常","2")

    dict = {
        "sucessNum": sucess_num,
        "erroNum": erro_num,
        "erroList": erro_list
    }

    return dict



def json_model(app2,model_data,api_sdk):

    try:
        materialSKU = "" if str(model_data['FPRDNUMBER']) == '1' else str(model_data['FPRDNUMBER'])
        materialId = code_conversion_org(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", materialSKU, "104", "FMATERIALID")

        # if materialSKU == "7.1.000001":
        #     materialId = "466653"

        # result = Order_view(api_sdk, str(model_data['FGODOWNNO']), materialId)str(model_data['FQTY'])

        result=checkCode(app2,str(model_data['FGODOWNNO']),str(model_data['FQTY']),str(model_data['FLot']),materialSKU)

        if result != [] and materialId != "":

            model={
                    "FMATERIALID": {
                        "FNumber": "7.1.000001" if str(model_data['FPRDNUMBER']) == '1' else code_conversion(app2,"rds_vw_material","F_SZSP_SKUNUMBER",str(model_data['FPRDNUMBER']))
                    },
                    "FPrice": str(model_data['FUNITPRICE']),
                    "FPriceQty": str(model_data['FQTY']),
                    "FTaxPrice": str(model_data['FTAXPRICE']),
                    "FEntryTaxRate": float(model_data['FTAXRATE'])*100,
                    "FNoTaxAmountFor_D": str(model_data['FSUMVALUE']),
                    "FINCLUDECOST": False,
                    "FISOUTSTOCK": False,
                    "FLot": {
                        "FNumber": str(model_data['FLot'])
                    },
                    "FIsFree": False,
                    "FStockQty": str(model_data['FQTY']),
                    "FStockBaseQty": str(model_data['FQTY']),
                    "FPriceBaseDen": 1.0,
                    "FStockBaseNum": 1.0,
                    "FNOINVOICEQTY": str(model_data['FQTY']),
                    "FTAILDIFFFLAG": False,
                    "FEntityDetail_Link": [{
                        "FEntityDetail_Link_FRuleId": "AP_InStockToPayableMap",
                        "FEntityDetail_Link_FSTableName": "T_STK_INSTOCKENTRY",
                        "FEntityDetail_Link_FSBillId ": result['FID'],
                        "FEntityDetail_Link_FSId": result['FENTRYID'],
                        "FEntityDetail_Link_FBASICUNITQTYOld": str(model_data['FQTY']),
                        "FEntityDetail_Link_FBASICUNITQTY": str(model_data['FQTY']),
                        "FEntityDetail_Link_FStockBaseQtyOld": str(model_data['FQTY']),
                        "FEntityDetail_Link_FStockBaseQty": str(model_data['FQTY']),
                    }]
                }

            return model

        else:

            return {}

    except Exception as e:

        return {}


def Order_view(api_sdk,value,materialID):
    '''
    采购入库单据查询
    :param value: 订单编码
    :return:
    '''

    res=json.loads(api_sdk.ExecuteBillQuery({"FormId": "STK_InStock", "FieldKeys": "FDate,FBillNo,FId,FInStockEntry_FEntryID,FMaterialId", "FilterString": [{"Left":"(","FieldName":"FMaterialId","Compare":"=","Value":materialID,"Right":")","Logic":"AND"},{"Left":"(","FieldName":"FBillNo","Compare":"=","Value":value,"Right":")","Logic":"AND"}], "TopRowCount": 0}))

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

        res=json.loads(api_sdk.View("AP_Payable",model))

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

        res=json.loads(api_sdk.Submit("AP_Payable",model))

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

        res = json.loads(api_sdk.Audit("AP_Payable", model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False

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

        return ""


def checkDataExist(app2, FInvoiceid):
    '''
    通过FSEQ字段判断数据是否在表中存在
    :param app2:
    :param FSEQ:
    :return:
    '''
    sql = f"select FInvoiceid from RDS_ECS_SRC_pur_invoice where FInvoiceid='{FInvoiceid}'"

    res = app2.select(sql)

    if res == []:

        return True

    else:

        return False


def insert_procurement_contract(app2,app3,data):
    '''
    采购开票
    :param app2:
    :param data:
    :return:
    '''



    for i in data.index:

        if checkDataExist(app3,data.loc[i]['FInvoiceid']):

            if judgementData(app2, app3, data[data['FINVOICENO'] == data.loc[i]['FINVOICENO']]):

                inert_data(app3, data[data['FINVOICENO'] == data.loc[i]['FINVOICENO']])


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

            if code_conversion(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", data.loc[i]['FPRDNUMBER']) != "" or \
                    data.loc[i]['FPRDNUMBER'] == "1":

                continue

            else:

                insertLog(app3, "应付单", data.loc[i]['FINVOICENO'], "物料不存在","2")

                flag = False

                break
        else:

            insertLog(app3, "应付单", data.loc[i]['FINVOICENO'], "客户不存在","2")

            flag = False

            break

    return flag


def inert_data(app3,data):

    for i in data.index:

        try:

            sql=f"""insert into RDS_ECS_SRC_pur_invoice(FPURORDERNO,FGODOWNNO,FBILLTYPEINAME,FINVOICEDATE,FINVOICETYPE,FINVOICENO,FDATE,FCUSTOMERNUMBER,FSUPPLIERNAME,FPOORDERSEQ,FPRDNUMBER,FPRDNAME,FQTY,FUNITPRICE,FSUMVALUE,FTAXRATE,FTAXAMOUNT,FTAXPRICE,FAMOUNTALL,FPURCHASEDEPTNAME,FPURCHASEGROUPNAME,FPURCHASERINAME,FDESCRIPTION,FUPLOADDATE,FISDO,FInvoiceid,FLot) values('{data.loc[i]['FPURORDERNO']}','{data.loc[i]['FGODOWNNO']}','{data.loc[i]['FBILLTYPEID']}','{data.loc[i]['FINVOICEDATE']}','{data.loc[i]['FINVOICETYPE']}','{data.loc[i]['FINVOICENO']}','{data.loc[i]['FDATE']}','{data.loc[i]['FCUSTOMERNUMBER']}','{data.loc[i]['FSUPPLIERNAME']}','{data.loc[i]['FPOORDERSEQ']}','{data.loc[i]['FPRDNUMBER']}','{data.loc[i]['FPRDNAME']}','{data.loc[i]['FQTY']}','{data.loc[i]['FUNITPRICE']}','{data.loc[i]['FSUMVALUE']}','{data.loc[i]['FTAXRATE']}','{data.loc[i]['FTAXAMOUNT']}','{data.loc[i]['FTAXPRICE']}','{data.loc[i]['FAMOUNT']}','{data.loc[i]['FPURCHASEDEPTID']}','{data.loc[i]['FPURCHASEGROUPID']}','{data.loc[i]['FPURCHASERID']}','{data.loc[i]['FDESCRIPTION']}',getdate(),0,'{data.loc[i]['FInvoiceid']}','{data.loc[i]['FLOT']}')"""

            app3.insert(sql)

            insertLog(app3, "应付单", data.loc[i]['FINVOICENO'], "数据插入成功", "1")

        except Exception as e:

            insertLog(app3, "应付单", data.loc[i]['FINVOICENO'], "插入SRC数据异常，请检查数据","2")

    pass


def getCode(app3):
    '''
    查询出表中的编码
    :param app2:
    :return:
    '''

    sql = "select distinct FINVOICENO from RDS_ECS_ODS_pur_invoice where FIsDo=0"

    res = app3.select(sql)

    return res

def getClassfyData(app3, code):
    '''
    获得分类数据
    :param app2:
    :param code:
    :return:
    '''

    try:

        number=code['FINVOICENO']

        sql = f"select FPURORDERNO,FGODOWNNO,FBILLTYPEINAME,FINVOICEDATE,FINVOICETYPE,FINVOICENO,FDATE,FCUSTOMERNUMBER,FSUPPLIERNAME,FPOORDERSEQ,FPRDNUMBER,FPRDNAME,FQTY,FUNITPRICE,FSUMVALUE,FTAXRATE,FTAXAMOUNT,FTAXPRICE,FAMOUNTALL,FPURCHASEDEPTNAME,FPURCHASEGROUPNAME,FPURCHASERINAME,FDESCRIPTION,FUPLOADDATE,FISDO,FInvoiceid,FLot from RDS_ECS_ODS_pur_invoice where FINVOICENO='{number}'"

        res = app3.select(sql)

        return res

    except Exception as e:

        return 0


def changeStatus(app3,fnumber,status):
    '''
    将没有写入的数据状态改为2
    :param app2: 执行sql语句对象
    :param fnumber: 订单编码
    :param status: 数据状态
    :return:
    '''

    sql=f"update a set a.Fisdo={status} from RDS_ECS_ODS_pur_invoice a where FINVOICENO='{fnumber}'"

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


def checkCode(app2,FNumber,FralQty,FLot,FSKU):
    '''
    查看单据内码
    :param app2:
    :param FNumber: 单据编码
    :param FralQty: 实际数量
    :param FLot: 批号
    :param FSKU: SKU编码
    :return:
    '''

    try:

        sql=f"""
            select a.FID,b.FENTRYID,b.FLOT,b.FLOT_TEXT,c.FNUMBER from t_STK_InStock a
            inner join T_STK_INSTOCKENTRY b
            on a.FID=b.FID
            inner join rds_vw_material c
            on c.FMATERIALID=b.FMATERIALID
            where a.FBILLNO='{FNumber}' and FREALQTY='{FralQty}' and b.FLOT_TEXT='{FLot}' and c.F_SZSP_SKUNUMBER='{FSKU}'          
            """

        res=app2.select(sql)

        if res:

            return res[0]

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


def writeSRC(startDate, endDate, app2,app3):
    '''
    将ECS数据取过来插入SRC表中
    :param startDate:
    :param endDate:
    :return:
    '''

    url = "https://kingdee-api.bioyx.cn/dynamic/query"

    page = viewPage(url, 1, 1000, "ge", "le", "v_procurement_contract", startDate, endDate, "FINVOICEDATE")

    for i in range(1, page + 1):
        df = ECS_post_info2(url, i, 1000, "ge", "le", "v_procurement_contract", startDate, endDate, "FINVOICEDATE")

        df=df.fillna("")

        insert_procurement_contract(app2,app3, df)

    pass

def classification_process(app2, data):
    '''
    将编码进行去重，然后进行分类
    :param data:
    :return:
    '''


    res = fuz(app2, data)

    return res

def fuz(app2, codeList):
    '''
    通过编码分类，将分类好的数据装入列表
    :param app2:
    :param codeList:
    :return:
    '''

    singleList = []

    for i in codeList:
        data = getClassfyData(app2, i)
        singleList.append(data)

    return singleList

def data_splicing(app2, api_sdk, data):
    '''
    将订单内的物料进行遍历组成一个列表，然后将结果返回给
    :param data:
    :return:
    '''

    list = []

    for i in data:

        result=json_model(app2, i, api_sdk)

        if result:

            list.append(result)
        else:
            return []

    return list

def purchasesBilling(startDate, endDate):

    app2 = RdClient(token='57DEDF26-5C00-4CA9-BBF7-57ECE07E179B')
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')

    writeSRC(startDate, endDate, app2,app3)

    data = getCode(app3)

    if data:
        res = classification_process(app3, data)

        api_sdk = K3CloudApiSdk()

        # 新账套
        option1 = {
            "acct_id": '62777efb5510ce',
            "user_name": 'DMS',
            "app_id": '235685_4e6vScvJUlAf4eyGRd3P078v7h0ZQCPH',
            # "app_sec": 'd019b038bc3c4b02b962e1756f49e179',
            "app_sec": 'b105890b343b40ba908ed51453940935',
            "server_url": 'http://192.168.1.13/K3Cloud',
        }

        res=ERP_Save(api_sdk=api_sdk, data=res, option=option1, app2=app2, app3=app3)

        return res

    else:
        return {"message":"无订单需要同步"}



