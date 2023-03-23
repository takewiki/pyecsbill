#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import requests
import hashlib
import pandas as pd

from pyrda.dbms.rds import RdClient
from k3cloud_webapi_sdk.main import K3CloudApiSdk


def encryption(pageNum, pageSize, queryList, tableName):
    '''
    ECS的token加密
    :param pageNum:
    :param pageSize:
    :param queryList:
    :param tableName:
    :return:
    '''

    m = hashlib.md5()

    token = f'accessId=skyx@prod&accessKey=skyx@0512@1024@prod&pageNum={pageNum}&pageSize={pageSize}&queryList={queryList}&tableName={tableName}'

    # token = f'accessId=skyx&accessKey=skyx@0512@1024&pageNum={pageNum}&pageSize={pageSize}&queryList={queryList}&tableName={tableName}'

    m.update(token.encode())

    md5 = m.hexdigest()

    return md5


def ECS_post_info2(url, pageNum, pageSize, qw, qw2, tableName, updateTime, updateTime2, key):
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

        queryList = '[{"qw":' + f'"{qw}"' + ',"value":' + f'"{updateTime}"' + ',"key":' + f'"{key}"' + '},{"qw":' + f'"{qw2}"' + ',"value":' + f'"{updateTime2}"' + ',"key":' + f'"{key}"' + '}]'

        # 查询条件
        queryList1 = [{"qw": qw, "value": updateTime, "key": key}, {"qw": qw2, "value": updateTime2, "key": key}]

        # 查询的表名
        tableName = tableName

        data = {
            "tableName": tableName,
            "pageNum": pageNum,
            "pageSize": pageSize,
            "token": encryption(pageNum, pageSize, queryList, tableName),
            "queryList": queryList1
        }
        data = json.dumps(data)

        # url = f"http://10.3.1.99:8107/customer/getCustomerList?startDate={startDate}&endDate={endDate}&token={md5}"

        # url = "https://test-kingdee-api.bioyx.cn/dynamic/query"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(url, headers=headers, data=data)

        info = response.json()

        df = pd.DataFrame(info['data']['list'])

        return df

    except Exception as e:

        return pd.DataFrame()


def viewPage(url, pageNum, pageSize, qw, qw2, tableName, updateTime, updateTime2, key):
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

        queryList = '[{"qw":' + f'"{qw}"' + ',"value":' + f'"{updateTime}"' + ',"key":' + f'"{key}"' + '},{"qw":' + f'"{qw2}"' + ',"value":' + f'"{updateTime2}"' + ',"key":' + f'"{key}"' + '}]'

        # 查询条件
        queryList1 = [{"qw": qw, "value": updateTime, "key": key}, {"qw": qw2, "value": updateTime2, "key": key}]

        # 查询的表名
        tableName = tableName

        data = {
            "tableName": tableName,
            "pageNum": pageNum,
            "pageSize": pageSize,
            "token": encryption(pageNum, pageSize, queryList, tableName),
            "queryList": queryList1
        }
        data = json.dumps(data)

        # url = f"http://10.3.1.99:8107/customer/getCustomerList?startDate={startDate}&endDate={endDate}&token={md5}"

        # url = "https://test-kingdee-api.bioyx.cn/dynamic/query"

        headers = {
            'Content-Type': 'application/json',
        }

        response = requests.post(url, headers=headers, data=data)

        info = response.json()

        # print(info)

        return info['data']['pages']

    except Exception as e:

        return 0


def ERP_Save(api_sdk, data, option, app2, app3):
    '''
    调用ERP保存接口
    :param api_sdk: 调用ERP对象
    :param data:  要插入的数据
    :param option: ERP密钥
    :param app2: 数据库执行对象
    :return:
    '''


    erro_list = []
    sucess_num = 0
    erro_num = 0

    api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                       option['app_sec'], option['server_url'])

    for i in data:

        try:

            if check_order_exists(api_sdk, i[0]['FBILLNO']) != True:

                model = {
                    "Model": {
                        "FID": 0,
                        "FBillTypeID": {
                            "FNUMBER": "YSD01_SYS"
                        },
                        "FBillNo": str(i[0]['FBILLNO']),
                        "FDATE": str(i[0]['FINVOICEDATE']),
                        "FISINIT": False,
                        # "FENDDATE_H": str(i[0]['FINVOICEDATE']),
                        "FCUSTOMERID": {
                            "FNumber": "C003142" if i[0]['FCUSTOMNAME'] == "苏州亚通生物医疗科技有限公司" else code_conversion(app2,
                                                                                                                    "rds_vw_customer",
                                                                                                                    "FNAME",
                                                                                                                    i[0][
                                                                                                                        'FCUSTOMNAME'])
                        },
                        "FCURRENCYID": {
                            "FNumber": "PRE001" if i[0]['FCurrencyName'] == '' else code_conversion(app2,
                                                                                                       "rds_vw_currency",
                                                                                                       "FNAME", i[0][
                                                                                                           'FCurrencyName'])
                        },
                        "FPayConditon": {
                            "FNumber": "SKTJ05_SP"
                        },
                        "FISPRICEEXCLUDETAX": True,
                        "FSETTLEORGID": {
                            "FNumber": "104"
                        },
                        "FPAYORGID": {
                            "FNumber": "104"
                        },
                        "FSALEORGID": {
                            "FNumber": "104"
                        },
                        "FISTAX": True,
                        "FSALEDEPTID": {
                            "FNumber": view(api_sdk, i[0]['FOUTSTOCKBILLNO'], "SaleDeptID")
                        },
                        "FSALEERID": {
                            "FNumber": findSalesNo(app2,i[0]['FOUTSTOCKBILLNO'])
                        },
                        # "FSALEDEPTID": {
                        #     "FNumber": "BM000036"
                        # },
                        "FCancelStatus": "A",
                        "FBUSINESSTYPE": "BZ",
                        "FSetAccountType": "1",
                        "FISHookMatch": False,
                        "FISINVOICEARLIER": False,
                        "FWBOPENQTY": False,
                        "FISGENERATEPLANBYCOSTITEM": False,
                        "F_SZSP_FPHM": str(i[0]['FINVOICENO']),
                        "F_SZSP_XSLX": {
                            "FNumber": "1"
                        },
                        "FsubHeadSuppiler": {
                            "FORDERID": {
                                "FNumber": "C003142" if i[0]['FCUSTOMNAME'] == "苏州亚通生物医疗科技有限公司" else code_conversion(
                                    app2, "rds_vw_customer", "FNAME", i[0]['FCUSTOMNAME'])
                            },
                            "FTRANSFERID": {
                                "FNumber": "C003142" if i[0]['FCUSTOMNAME'] == "苏州亚通生物医疗科技有限公司" else code_conversion(
                                    app2, "rds_vw_customer", "FNAME", i[0]['FCUSTOMNAME'])
                            },
                            "FChargeId": {
                                "FNumber": "C003142" if i[0]['FCUSTOMNAME'] == "苏州亚通生物医疗科技有限公司" else code_conversion(
                                    app2, "rds_vw_customer", "FNAME", i[0]['FCUSTOMNAME'])
                            }
                        },
                        "FsubHeadFinc": {
                            # "FACCNTTIMEJUDGETIME": str(i[0]['FINVOICEDATE']),
                            "FMAINBOOKSTDCURRID": {
                                "FNumber": "PRE001" if i[0]['FCurrencyName'] == '' else code_conversion(app2,
                                                                                                           "rds_vw_currency",
                                                                                                           "FNAME", i[0][
                                                                                                               'FCurrencyName'])
                            },
                            "FEXCHANGETYPE": {
                                "FNumber": "HLTX01_SYS"
                            },
                            "FExchangeRate": 1.0,
                            "FISCARRIEDDATE": False
                        },
                        "FEntityDetail": data_splicing(app2, api_sdk, i),
                        "FEntityPlan": [
                            {
                                # "FENDDATE": str(i[0]['FINVOICEDATE']),
                                "FPAYRATE": 100.0,
                            }
                        ]
                    }
                }
                save_result = json.loads(api_sdk.Save("AR_receivable", model))
                if save_result['Result']['ResponseStatus']['IsSuccess']:

                    FNumber = save_result['Result']['ResponseStatus']['SuccessEntitys'][0]['Number']

                    submit_res = ERP_submit(api_sdk, FNumber)

                    if submit_res:

                        # audit_res = ERP_Audit(api_sdk, FNumber)
                        insertLog(app3, "应收单", str(i[0]['FBILLNO']), "数据同步成功", "1")

                        changeStatus(app3, str(i[0]['FBILLNO']), "1")

                        sucess_num = sucess_num + 1

                        # if audit_res:
                        #
                        #     db.changeStatus(app3, str(i[0]['FBILLNO']), "3")
                        #
                        #     sucess_num=sucess_num+1
                        #
                        # else:
                        #     pass
                    else:
                        pass
                else:

                    insertLog(app3, "应收单", str(i[0]['FBILLNO']),save_result['Result']['ResponseStatus']['Errors'][0]['Message'],"2")

                    changeStatus(app3, str(i[0]['FBILLNO']), "2")
                    erro_num = erro_num + 1
                    erro_list.append(save_result)

        except Exception as e:

            insertLog(app3, "应收单", str(i[0]['FBILLNO']),"数据异常","2")

    dict = {
        "sucessNum": sucess_num,
        "erroNum": erro_num,
        "erroList": erro_list
    }

    return dict



def ERP_submit(api_sdk, FNumber):

    try:
        model = {
            "CreateOrgId": 0,
            "Numbers": [FNumber],
            "Ids": "",
            "SelectedPostId": 0,
            "NetworkCtrl": "",
            "IgnoreInterationFlag": ""
        }

        res = json.loads(api_sdk.Submit("AR_receivable", model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False


def ERP_Audit(api_sdk, FNumber):
    '''
    将订单审核
    :param api_sdk: API接口对象
    :param FNumber: 订单编码
    :return:
    '''

    try:

        model = {
            "CreateOrgId": 0,
            "Numbers": [FNumber],
            "Ids": "",
            "InterationFlags": "",
            "NetworkCtrl": "",
            "IsVerifyProcInst": "",
            "IgnoreInterationFlag": ""
        }

        res = json.loads(api_sdk.Audit("AR_receivable", model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False


def json_model(app2, model_data, api_sdk,index,result,materialSKU):

    try:

        materialId = code_conversion_org(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", materialSKU, "104", "FMATERIALID")

        if materialSKU == "7.1.000001":
            materialId = "466653"


        if result != [] and materialId != "":

            model = {
                "FMATERIALID": {
                    "FNumber": "7.1.000001" if model_data['FPrdNumber'] == '1' else str(
                        code_conversion(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", model_data['FPrdNumber']))
                },
                "FPriceQty": str(model_data['FQUANTITY']),
                "FTaxPrice": str(result[index]['FTAXPRICE']),
                "FPrice": str(model_data['FUNITPRICE']),
                "FEntryTaxRate": float(model_data['FTAXRATE']) * 100,
                "FNoTaxAmountFor_D": str(model_data['FSUMVALUE']),
                "FDeliveryControl": False,
                "FLot": {
                    "FNumber": str(model_data['FLot'])
                },
                "FStockQty": str(model_data['FQUANTITY']),
                # "FIsFree": False,
                "FStockBaseQty": str(model_data['FQUANTITY']),
                "FSalQty": str(model_data['FQUANTITY']),
                "FSalBaseQty": str(model_data['FQUANTITY']),
                "FPriceBaseDen": 1.0,
                "FSalBaseNum": 1.0,
                "FStockBaseNum": 1.0,
                "FNOINVOICEQTY": str(model_data['FQUANTITY']),
                "FTAILDIFFFLAG": False,
                "FEntityDetail_Link": [{
                    "FEntityDetail_Link_FRuleId": "AR_OutStockToReceivableMap" if int(model_data['FQUANTITY'])>0 else "",
                    "FEntityDetail_Link_FSTableName": "T_SAL_OUTSTOCKENTRY" if int(model_data['FQUANTITY'])>0 else "",
                    "FEntityDetail_Link_FSBillId ": result[index]['Fid'],
                    "FEntityDetail_Link_FSId": result[index]['FENTRYID'],
                    "FEntityDetail_Link_FBASICUNITQTYOld": str(model_data['FQUANTITY']),
                    "FEntityDetail_Link_FBASICUNITQTY": str(model_data['FQUANTITY']),
                    "FEntityDetail_Link_FStockBaseQtyOld": str(model_data['FQUANTITY']),
                    "FEntityDetail_Link_FStockBaseQty": str(model_data['FQUANTITY']),
                }]
            }

            return model

        else:

            return {}

    except Exception as e:

        return {}


def check_order_exists(api_sdk, FNumber):
    '''
    查看订单是否在ERP系统存在
    :param api: API接口对象
    :param FNumber: 订单编码
    :return:
    '''

    model = {
        "CreateOrgId": 0,
        "Number": FNumber,
        "Id": "",
        "IsSortBySeq": "false"
    }

    res = json.loads(api_sdk.View("AR_receivable", model))

    return res['Result']['ResponseStatus']['IsSuccess']


def outOrder_view(api_sdk, value, materialID,qtyValue,dlotValue):
    '''
    销售订单单据查询
    :param value: 订单编码
    :return:
    '''

    res = json.loads(api_sdk.ExecuteBillQuery(
        {"FormId": "SAL_OUTSTOCK", "FieldKeys": "FDate,FBillNo,FId,FEntity_FENTRYID,FMaterialID,FTaxPrice",
         "FilterString": [{"Left": "(", "FieldName": "FMaterialID", "Compare": "=", "Value": materialID, "Right": ")",
                           "Logic": "AND"},
                          {"Left": "(", "FieldName": "FBillNo", "Compare": "=", "Value": value, "Right": ")",
                           "Logic": "AND"},
                            {"Left": "(", "FieldName": "FRealQty", "Compare": "=", "Value": qtyValue, "Right": ")",
                           "Logic": "AND"},
                          {"Left": "(", "FieldName": "FLot", "Compare": "=", "Value": dlotValue, "Right": ")",
                           "Logic": "AND"}], "TopRowCount": 0}))

    return res


def view(api_sdk, FNumber, param):
    '''
    通过查询接口，查询销售员和销售部门
    :param api_sdk:
    :param FNumber:
    :param param:
    :return:
    '''

    try:

        model = {
            "CreateOrgId": 0,
            "Number": FNumber,
            "Id": "",
            "IsSortBySeq": "false"
        }

        res = json.loads(api_sdk.View("SAL_SaleOrder", model))

        if res['Result']['ResponseStatus']['IsSuccess']:

            return res['Result']['Result'][param]['Number']

        else:
            return ""

    except Exception as e:

        return "BSP00068_GW000159_111785"

def getCode(app3):
    '''
    查询出表中的编码
    :param app2:
    :return:
    '''

    sql = "select distinct FBILLNO from RDS_ECS_ODS_sal_billreceivable where FIsDo=0"

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

        number=code['FBILLNO']

        sql = f"select FInterID,FCUSTNUMBER,FOUTSTOCKBILLNO,FSALEORDERENTRYSEQ,FBILLTYPEID,FCUSTOMNAME,FBILLNO,FPrdNumber,FPrdName,FQUANTITY,FUNITPRICE,FSUMVALUE,FTAXRATE,FTRADENO,FNOTETYPE,FISPACKINGBILLNO,FBILLCODE,FINVOICENO,FINVOICEDATE,UPDATETIME,Fisdo,FCurrencyName,FInvoiceid,FLot from RDS_ECS_ODS_sal_billreceivable where FBILLNO='{number}'"

        res = app3.select(sql)

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


def checkDataExist(app2, FInvoiceid):
    '''
    判断数据是否在表中存在
    :param app2:
    :param FSEQ:
    :return:
    '''
    sql = f"select FInvoiceid from RDS_ECS_SRC_sal_billreceivable where FInvoiceid='{FInvoiceid}'"

    res = app2.select(sql)

    if res == []:

        return True

    else:

        return False


def changeStatus(app3, fnumber, status):
    '''
    将没有写入的数据状态改为2
    :param app2: 执行sql语句对象
    :param fnumber: 订单编码
    :param status: 数据状态
    :return:
    '''

    sql = f"update a set a.Fisdo={status} from RDS_ECS_ODS_sal_billreceivable a where FBILLNO='{fnumber}'"

    app3.update(sql)


def insert_sales_invoice(app2,app3, data):
    '''
    销售开票
    :param app2:
    :param data:
    :return:
    '''



    for i in data.index:

        if checkDataExist(app3, data.iloc[i]['FInvoiceid']) and data.iloc[i]['FQUANTITY'] != '':

            if judgementData(app2, app3, data[data['FBILLNO'] == data.loc[i]['FBILLNO']]):

                inert_data(app3, data[data['FBILLNO'] == data.loc[i]['FBILLNO']])




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
            where a.FBILLNO='{FBillNo}' and FLOT_TEXT='{FLot}' and b.FREALQTY='{REALQTY}' and c.F_SZSP_SKUNUMBER='{FSKUNUM}' 
        """

        res=app2.select(sql)

        if res:

            return res

        else:

            return []

    except Exception as e:

        return []


def findSalesNo(app3,fnumber):
    '''
    通过销售出库单号查出对应的销售员编码
    :param app3:
    :param fnumber:
    :return:
    '''

    sql=f"""
    select a.FSALESMANID,b.FNAME,b.FNUMBER from T_SAL_OUTSTOCK a
    inner join rds_vw_salesman b
    on a.FSALESMANID=b.fid
    where a.FBILLNO='{fnumber}'
    """

    res=app3.select(sql)

    if res:

        return res[0]['FNUMBER']

    else:

        return ""

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


def findRreturnID(app2,FBILLNO,F_SZSP_SKUNUMBER,FLOT_TEXT,FREALQTY):
    '''
    找到退货单id
    :param app2:
    :return:
    '''

    sql=f"""
        select a.FID,b.FENTRYID,b.FLOT_TEXT,b.FREALQTY,c.F_SZSP_SKUNUMBER,d.FTAXPRICE from T_SAL_RETURNSTOCK a
        inner join T_SAL_RETURNSTOCKENTRY b
        on a.FID=b.FID
        inner join rds_vw_material c
        on c.FMATERIALID=b.FMATERIALID
        inner join T_SAL_RETURNSTOCKENTRY_F d
        on d.FENTRYID=b.FENTRYID
        where a.FBILLNO='{FBILLNO}'and c.F_SZSP_SKUNUMBER='{F_SZSP_SKUNUMBER}' and b.FLOT_TEXT='{FLOT_TEXT}' and b.FREALQTY='{FREALQTY}'"""

    res=app2.select(sql)

    if res:

        return res

    else:

        return []


def judgementData(app2, app3, data):
    '''
    判断数据是否合规
    :param app2:
    :param data:
    :return:
    '''

    flag = True

    for i in data.index:

        if data.loc[i]['FQUANTITY'] != "":

            if code_conversion(app2, "rds_vw_customer", "FNAME", data.loc[i]['FCUSTOMNAME']) != "":

                if code_conversion(app2, "rds_vw_material", "F_SZSP_SKUNUMBER", data.loc[i]['FPrdNumber']) != "" or \
                        data.loc[i]['FPrdNumber'] == "1":

                    continue

                else:

                    insertLog(app3, "应收单", data.loc[i]['FBILLNO'], "物料不存在","2")

                    flag = False

                    break
            else:

                insertLog(app3, "应收单", data.loc[i]['FBILLNO'], "客户不存在","2")

                flag = False

                break

        else:

            insertLog(app3, "应收单", data.loc[i]['FBILLNO'], "数量为空","2")

            flag = False

            break

    return flag


def inert_data(app3,data):

    for i in range(0,len(data)):

        try:

            sql = f"""insert into RDS_ECS_SRC_sal_billreceivable(FInterID,FCUSTNUMBER,FOUTSTOCKBILLNO,FSALEORDERENTRYSEQ,FBILLTYPEID,FCUSTOMNAME,FBILLNO,FPrdNumber,FPrdName,FQUANTITY,FUNITPRICE,FSUMVALUE,FTAXRATE,FTRADENO,FNOTETYPE,FISPACKINGBILLNO,FBILLCODE,FINVOICENO,FINVOICEDATE,UPDATETIME,Fisdo,FCurrencyName,FInvoiceid,FLot) values({int(getFinterId(app3, 'RDS_ECS_SRC_sal_billreceivable')) + 1},'{data.iloc[i]['FCUSTNUMBER']}','{data.iloc[i]['FOUTSTOCKBILLNO']}','{data.iloc[i]['FSALEORDERENTRYSEQ']}','{data.iloc[i]['FBILLTYPEID']}','{data.iloc[i]['FCUSTOMNAME']}','{data.iloc[i]['FBILLNO']}','{data.iloc[i]['FPrdNumber']}','{data.iloc[i]['FPrdName']}','{int(data.iloc[i]['FQUANTITY'])}','{data.iloc[i]['FUNITPRICE']}','{data.iloc[i]['FSUMVALUE']}','{data.iloc[i]['FTAXRATE']}','{data.iloc[i]['FTRADENO']}','{data.iloc[i]['FNOTETYPE']}','{data.iloc[i]['FISPACKINGBILLNO']}','{data.iloc[i]['FBILLCODE']}','{data.iloc[i]['FINVOICENO']}','{data.iloc[i]['FINVOICEDATE']}',getdate(),0,'{data.iloc[i]['FCURRENCYID']}','{data.iloc[i]['FInvoiceid']}','{data.iloc[i]['FLOT']}')"""

            app3.insert(sql)

            insertLog(app3, "应收单", data.iloc[i]['FBILLNO'], "数据插入成功", "1")

        except Exception as e:

            insertLog(app3, "应收单", data.iloc[i]['FBILLNO'], "插入SRC数据异常，请检查数据","2")

    pass

def writeSRC(startDate, endDate, app2,app3):
    '''
    将ECS数据取过来插入SRC表中
    :param startDate:
    :param endDate:
    :return:
    '''

    url = "https://kingdee-api.bioyx.cn/dynamic/query"

    page = viewPage(url, 1, 1000, "ge", "le", "v_sales_invoice", startDate, endDate, "FINVOICEDATE")

    for i in range(1, page + 1):
        df = ECS_post_info2(url, i, 1000, "ge", "le", "v_sales_invoice", startDate, endDate, "FINVOICEDATE")

        df = df.fillna("")

        insert_sales_invoice(app2,app3, df)

# FBILLNO

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


def classification_process(app2, data):
    '''
    将编码进行去重，然后进行分类
    :param data:
    :return:
    '''

    res = fuz(app2, data)

    return res


def data_splicing(app2, api_sdk, data):
    '''
    将订单内的物料进行遍历组成一个列表，然后将结果返回给 FEntity
    :param data:
    :return:
    '''

    list = []

    index = 0

    for i in data:

        materialSKU = "" if str(i['FPrdNumber']) == '1' else str(i['FPrdNumber'])

        result=[]

        if int(i['FQUANTITY'])>0:

            result = checkFlot(app2, str(i['FOUTSTOCKBILLNO']), str(i['FLot']),
                                  str(i['FQUANTITY']), str(materialSKU))
        else:

            result=findRreturnID(app2,str(i['FOUTSTOCKBILLNO']),str(materialSKU),str(i['FLot']),-int(i['FQUANTITY']))

        if index == len(result):

            index = 0

        res = json_model(app2, i, api_sdk, index, result, materialSKU)

        if res:

            list.append(res)

            index = index + 1

            if index == len(result):
                index = 0

        else:

            return []

    return list


def salesBilling(startDate, endDate):
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

        msg = ERP_Save(api_sdk=api_sdk, data=res, option=option1, app2=app2, app3=app3)

        return msg

    else:

        return {"message": "无订单需要同步"}



