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




def json_model(app2,model_data):

    '''
    物料单元model
    :param model_data: 物料信息
    :return:
    '''

    try:

        if code_conversion_org(app2,"rds_vw_material","F_SZSP_SKUNUMBER",str(model_data['FPRDNUMBER']),"104")!="":

            model={

                "FProductType": "1",
                "FMaterialId": {
                    "FNumber": "7.1.000001" if str(model_data['FPRDNUMBER'])=='1' else code_conversion_org(app2,"rds_vw_material","F_SZSP_SKUNUMBER",str(model_data['FPRDNUMBER']),"104")
                },
                "FMaterialDesc": str(model_data['FPRDNAME']),
                # "FUnitId": {
                #     "FNumber": "01"
                # },
                "FQty": str(model_data['FQTY']),
                # "FPriceUnitId": {
                #     "FNumber": "01"
                # },
                "FPriceUnitQty": str(model_data['FQTY']),
                "FPriceBaseQty": str(model_data['FQTY']),
                "FDeliveryDate": str(model_data['FDeliveryDate']),
                "FPrice": str(model_data['FPRICE']),
                "FTaxPrice": str(model_data['FTAXPRICE']),
                "FEntryTaxRate": float(model_data['FTAXRATE'])*100,
                "FRequireOrgId": {
                    "FNumber": "104"
                },
                "FReceiveOrgId": {
                    "FNumber": "104"
                },
                "FEntrySettleOrgId": {
                    "FNumber": "104"
                },
                "FGiveAway": True if model_data['FIsFree']==1 else False,
                "FEntryNote": str(model_data['FDESCRIPTION']),
                # "FStockUnitID": {
                #     "FNumber": "01"
                # },
                "FStockQty": str(model_data['FQTY']),
                "FStockBaseQty": str(model_data['FQTY']),
                "FDeliveryControl": True,
                "FTimeControl": False,
                "FDeliveryMaxQty": str(model_data['FQTY']),
                "FDeliveryMinQty": str(model_data['FQTY']),
                "FDeliveryEarlyDate": str(model_data['FDeliveryDate']),
                "FDeliveryLastDate": str(model_data['FDeliveryDate']),
                "FPriceCoefficient": 1.0,
                "FEntrySettleModeId": {
                    "FNumber": "JSFS04_SYS"
                },
                "FPlanConfirm": True,
                # "FSalUnitID": {
                #     "FNumber": "01"
                # },
                "FSalQty": str(model_data['FQTY']),
                "FCentSettleOrgId": {
                    "FNumber": "104"
                },
                "FDispSettleOrgId": {
                    "FNumber": "104"
                },
                "FDeliveryStockStatus": {
                    "FNumber": "KCZT02_SYS"
                },
                "FIsStock": False,
                "FSalBaseQty": str(model_data['FQTY']),
                "FEntryPayOrgId": {
                    "FNumber": "104"
                },
                "FBASESTOCKRETQTY":str(model_data['FQTY']),
                # "FAllAmountExceptDisCount": 54520.0,
                "FEntryDeliveryPlan": [
                    {
                        "FDeliveryDate_Plan": str(model_data['FDeliveryDate']),
                        "FPlanQty": str(model_data['FQTY']),
                        "FPREARRIVALDATE": str(model_data['FDeliveryDate'])
                    }
                ]
            }

            return model

        else:

            return {}

    except Exception as e:

        return {}


def data_splicing(app2,data):
    '''
    将订单内的物料进行遍历组成一个列表，然后将结果返回给 FPOOrderEntry
    :param data:
    :return:
    '''
    list=[]

    for i in data:

        result=json_model(app2,i)

        if result:

            list.append(result)

        else:

            return []

    return list


def ERP_Save(api_sdk,data,option,app2,app3):
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

            if check_order_exists(api_sdk,str(i[0]['FPURORDERNO']))!=True:

                    model={
                            "Model": {
                                "FID": 0,
                                "FBillTypeID": {
                                    "FNUMBER": "CGDD01_SYS"
                                },
                                "FBillNo": str(i[0]['FPURORDERNO']),
                                "FDate": str(i[0]['FPURCHASEDATE']),
                                "FSupplierId": {
                                    "FNumber": code_conversion_org(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'],"104")
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
                                    "FNumber": code_conversion(app2,"rds_vw_buyer","FNAME",str(i[0]['FPURCHASERID']))
                                },
                                "FProviderId": {
                                    "FNumber": code_conversion_org(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'],"104")
                                },
                                "FSettleId": {
                                    "FNumber": code_conversion_org(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'],"104")
                                },
                                "FChargeId": {
                                    "FNumber": code_conversion_org(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'],"104")
                                },
                                "FCorrespondOrgId": {
                                    "FNumber": code_conversion_org(app2,"rds_vw_supplier","FNAME",i[0]['FSUPPLIERNAME'],"104")
                                },
                                "FIsModificationOperator": False,
                                "FChangeStatus": "A",
                                "FACCTYPE": "Q",
                                "F_SZSP_CGLX": {
                                    "FNumber": "LX07"
                                },
                                "F_SZSP_SHR": {
                                    "FSTAFFNUMBER": "BSP00040"
                                },
                                "FIsMobBill": False,
                                "FIsUseDrpSalePOPush": False,
                                "F_SZSP_ReceCloseFlag": False,
                                "F_SZSP_FPCloseFlag": False,
                                "F_SZSP_PayCloseFlag": False,
                                "F_SZSP_initComfirmation": False,
                                "FPOOrderFinance": {
                                    "FSettleModeId": {
                                        "FNumber": "JSFS04_SYS"
                                    },
                                    "FPayConditionId": {
                                        "FNumber": "FKTJ03_SP"
                                    },
                                    "FSettleCurrId": {
                                        "FNumber": "PRE001"
                                    },
                                    "FExchangeTypeId": {
                                        "FNumber": "HLTX01_SYS"
                                    },
                                    "FExchangeRate": 1.0,
                                    "FPriceTimePoint": "1",
                                    "FFOCUSSETTLEORGID": {
                                        "FNumber": "104"
                                    },
                                    "FIsIncludedTax": True,
                                    "FISPRICEEXCLUDETAX": True,
                                    "FLocalCurrId": {
                                        "FNumber": "PRE001"
                                    },
                                    "FSupToOderExchangeBusRate": 1.0,
                                    "FSEPSETTLE": False
                                },
                                "FPOOrderEntry": data_splicing(app2,i)
                            }
                        }

                    result=json.loads(api_sdk.Save("PUR_PurchaseOrder",model))

                    if result['Result']['ResponseStatus']['IsSuccess']:

                        res_submit=ERP_Submit(api_sdk,str(i[0]['FPURORDERNO']))

                        if res_submit:

                            res_audit = ERP_Audit(api_sdk, str(i[0]['FPURORDERNO']))

                            if res_audit:

                                insertLog(app3, "采购订单", str(i[0]['FPURORDERNO']), "数据同步成功", "1")

                                changeStatus(app3, str(i[0]['FPURORDERNO']), "1")

                                sucess_num=sucess_num+1

                            else:

                                pass

                        else:

                            changeStatus(app3, str(i[0]['FPURORDERNO']), "2")

                    else:

                        insertLog(app3,"采购订单",str(i[0]['FPURORDERNO']),result['Result']['ResponseStatus']['Errors'][0]['Message'],"2")
                        changeStatus(app3,str(i[0]['FPURORDERNO']),"2")

                        erro_num=erro_num+1
                        erro_list.append(result)

            else:
                changeStatus(app3, str(i[0]['FPURORDERNO']), "1")

        except Exception as e:

            insertLog(app3,"采购订单",str(i[0]['FPURORDERNO']),"数据异常","2")



    dict = {
        "sucessNum": sucess_num,
        "erroNum": erro_num,
        "erroList": erro_list
    }

    return dict



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

        res=json.loads(api_sdk.View("PUR_PurchaseOrder",model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return True

def ERP_Submit(api_sdk,FNumber):
    '''
    将订单进行提交
    :param api_sdk: API接口对象
    :param FNumber: 订单编码
    :return:
    '''

    try:

        model={
            "CreateOrgId": 0,
            "Numbers": [FNumber],
            "Ids": "",
            "SelectedPostId": 0,
            "NetworkCtrl": "",
            "IgnoreInterationFlag": ""
        }

        res=json.loads(api_sdk.Submit("PUR_PurchaseOrder",model))

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

        res = json.loads(api_sdk.Audit("PUR_PurchaseOrder", model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False


def approvalFlow(api_sdk,fnumber,approver):
    '''
    处理审批流的问题
    :param fnumber: 订单编号
    :param approver: 审核人
    :return:
    '''

    try:

        model={"FormId":"PUR_PurchaseOrder","Ids":[],"Numbers":[fnumber],"UserId":0,"UserName":approver ,"ApprovalType":1,"ActionResultId":"","PostId":0,"PostNumber":"","Disposition":""}

        res=json.loads(api_sdk.WorkflowAudit(model))


        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False


def checkBillAmount(api_sdk,fnumber):

    res = json.loads(api_sdk.ExecuteBillQuery(
        {"FormId": "PUR_PurchaseOrder", "FieldKeys": "FBillAllAmount", "FilterString": [
            {"Left": "(", "FieldName": "FBillNo", "Compare": "=", "Value": fnumber, "Right": ")", "Logic": "AND"}],
         "TopRowCount": 0}))

    return res


def unAudit(api_sdk,FNumber,option):
    '''
    将单据反审核
    :param api_sdk:
    :param FNumber:
    :return:
    '''

    try:

        api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                           option['app_sec'], option['server_url'])

        model={
                "CreateOrgId": 0,
                "Numbers": [FNumber],
                "Ids": "",
                "InterationFlags": "",
                "IgnoreInterationFlag": "",
                "NetworkCtrl": "",
                "IsVerifyProcInst": ""
            }


        res=json.loads(api_sdk.UnAudit("PUR_PurchaseOrder",model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False

def delete(api_sdk,FNumber,option):
    '''
    将单据删除
    :param api_sdk:
    :param FNumber:
    :return:
    '''

    try:

        api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                           option['app_sec'], option['server_url'])

        model={
            "CreateOrgId": 0,
            "Numbers": [FNumber],
            "Ids": "",
            "NetworkCtrl": ""
        }

        res=json.loads(api_sdk.Delete("PUR_PurchaseOrder",model))

        return res['Result']['ResponseStatus']['IsSuccess']

    except Exception as e:

        return False



def getCode(app3):
    '''
    查询出表中的编码
    :param app2:
    :return:
    '''

    sql="select distinct FPURORDERNO from RDS_ECS_ODS_pur_poorder where FIsdo=0 and FIsFree!=1"

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

        number=code['FPURORDERNO']

        sql=f"select FPURORDERNO,FBILLTYPENAME,FPURCHASEDATE,FCUSTOMERNUMBER,FSUPPLIERNAME,FPOORDERSEQ,FPRDNUMBER,FPRDNAME,FQTY,FPRICE,FAMOUNT,FTAXRATE,FTAXAMOUNT,FTAXPRICE,FORAMOUNTFALL,FPURCHASEDEPTID,FPURCHASEGROUPID,FPURCHASERID,FDESCRIPTION,FUploadDate,FIsDo,FDeliveryDate,FIsFree from RDS_ECS_ODS_pur_poorder where FPURORDERNO='{number}'"

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

def code_conversion_org(app2,tableName,param,param2,param3):
    '''
    通过ECS物料编码来查询系统内的编码
    :param app2: 数据库操作对象
    :param tableName: 表名
    :param param:  参数1
    :param param2: 参数2
    :return:
    '''

    sql=f"select FNumber from {tableName} where {param}='{param2}' and FORGNUMBER='{param3}'"

    res=app2.select(sql)

    if res==[]:

        return ""

    else:

        return res[0]['FNumber']

def changeStatus(app2,fnumber,status):
    '''
    将没有写入的数据状态改为2
    :param app2: 执行sql语句对象
    :param fnumber: 订单编码
    :param status: 数据状态
    :return:
    '''

    sql=f"update a set a.FIsDo={status} from RDS_ECS_ODS_pur_poorder a where FPURORDERNO='{fnumber}'"

    app2.update(sql)


def checkDataExist(app2, FOrderId):
    '''
    通过FSEQ字段判断数据是否在表中存在
    :param app2:
    :param FSEQ:
    :return:
    '''
    sql = f"select FOrderId from RDS_ECS_SRC_pur_poorder where FOrderId='{FOrderId}'"

    res = app2.select(sql)

    if res == []:

        return True

    else:

        return False




def deleteOldDate(app3,FNumber):
    '''
    删除旧数据
    :param app3:
    :param FNumber:
    :return:
    '''

    sql1=f"delete from RDS_ECS_SRC_pur_poorder where FPURORDERNO='{FNumber}'"

    app3.delete(sql1)

    sql2 = f"delete from RDS_ECS_ODS_pur_poorder where FPURORDERNO='{FNumber}'"

    app3.delete(sql2)

    return True


def IsUpdate(app3,seq,updateTime,api_sdk,option,FStatus):

    # 待完结

    flag=False

    sql=f"select FPURORDERNO,FUpDateTime,FStatus from RDS_ECS_SRC_pur_poorder where FOrderId='{seq}'"

    res=app3.select(sql)

    if res:

        try:

            if str(res[0]['FStatus']) == "待完结":

                if str(res[0]['FUpDateTime'])!=updateTime:

                    flag=True

                    deleteResult=deleteOldDate(app3,res[0]['FPURORDERNO'])

                    if deleteResult:

                        unAuditResult=unAudit(api_sdk,res[0]['FPURORDERNO'],option)

                        if unAuditResult:

                            delete(api_sdk,res[0]['FPURORDERNO'],option)

                    else:

                        insertLog(app3, "采购订单",res[0]['FPURORDERNO'], "反审核失败", "2")

                else:

                    return False


        except Exception as e:

            insertLog(app3, "采购订单", res[0]['FPURORDERNO'], "更新数据失败", "2")

            return False

    return flag


def insert_procurement_order(app2,app3,data,api_sdk,option):
    '''
    采购订单
    :param app2:
    :param data:
    :return:
    '''


    for i in data.index:

        if checkDataExist(app3,data.loc[i]['FOrderId']) or IsUpdate(app3,data.loc[i]['FOrderId'],data.loc[i]['UPDATETIME'],api_sdk,option,data.loc[i]['FStatus']):

            if judgementData(app2, app3, data[data['FPURORDERNO'] == data.loc[i]['FPURORDERNO']]):

                inert_data(app3, data[data['FPURORDERNO'] == data.loc[i]['FPURORDERNO']])



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

                insertLog(app3, "采购订单",  data.loc[i]['FPURORDERNO'], "物料不存在","2")

                flag = False

                break
        else:

            insertLog(app3, "采购订单",  data.loc[i]['FPURORDERNO'], "供应商不存在","2")

            flag = False

            break

    return flag


def inert_data(app3,data):

    for i in data.index:

        try:

            sql = f"""insert into RDS_ECS_SRC_pur_poorder(FPURORDERNO,FBILLTYPENAME,FPURCHASEDATE,FCUSTOMERNUMBER,FSUPPLIERNAME,FPOORDERSEQ,FPRDNUMBER,FPRDNAME,FQTY,FPRICE,FAMOUNT,FTAXRATE,FTAXAMOUNT,FTAXPRICE,FORAMOUNTFALL,FPURCHASEDEPTID,FPURCHASEGROUPID,FPURCHASERID,FUploadDate,FIsDo,FIsFree,FUpDateTime,FOrderId,FDESCRIPTION,FStatus) values('{data.loc[i]['FPURORDERNO']}','{data.loc[i]['FBILLTYPENAME']}','{data.loc[i]['FPURCHASEDATE']}','{data.loc[i]['FCUSTOMERNUMBER']}','{data.loc[i]['FSUPPLIERNAME']}','{data.loc[i]['FPOORDERSEQ']}','{data.loc[i]['FPRDNUMBER']}','{data.loc[i]['FPRDNAME']}','{data.loc[i]['FQTY']}','{data.loc[i]['FPRICE']}','{data.loc[i]['FAMOUNT']}','{data.loc[i]['FTAXRATE']}','{data.loc[i]['FTAXAMOUNT']}','{data.loc[i]['FTAXPRICE']}','{data.loc[i]['FAMOUNT']}','{data.loc[i]['FPURCHASEDEPTID']}','{data.loc[i]['FPURCHASEGROUPID']}','{data.loc[i]['FPURCHASERID']}',getdate(),0,0,'{data.loc[i]['UPDATETIME']}','{data.loc[i]['FOrderId']}','{data.loc[i]['FDESCRIPTION']}','{data.loc[i]['FStatus']}')"""

            app3.insert(sql)

            insertLog(app3, "采购订单", data.loc[i]['FPURORDERNO'], "数据插入成功", "1")

        except Exception as e:

            insertLog(app3, "采购订单", data.loc[i]['FPURORDERNO'], "插入SRC数据异常，请检查数据","2")

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


def classification_process(app2,data):
    '''
    将编码进行去重，然后进行分类
    :param data:
    :return:
    '''

    res=fuz(app2,data)

    return res

def fuz(app2,codeList):
    '''
    通过编码分类，将分类好的数据装入列表
    :param app2:
    :param codeList:
    :return:
    '''

    singleList=[]

    for i in codeList:

        data=getClassfyData(app2,i)
        singleList.append(data)

    return singleList


def writeSRC(startDate, endDate,app2, app3,api_sdk,option):
    '''
    将ECS数据取过来插入SRC表中
    :param startDate:
    :param endDate:
    :return:
    '''

    url = "https://kingdee-api.bioyx.cn/dynamic/query"

    page = viewPage(url, 1, 1000, "ge", "le", "v_procurement_order", startDate, endDate, "UPDATETIME")

    for i in range(1, page + 1):
        df = ECS_post_info2(url, i, 1000, "ge", "le", "v_procurement_order", startDate, endDate, "UPDATETIME")

        insert_procurement_order(app2,app3, df,api_sdk,option)

    pass


def purchaseOrder(startDate,endDate):
    '''
    接口入口
    :return:
    '''

    api_sdk = K3CloudApiSdk()

    app2 = RdClient(token='57DEDF26-5C00-4CA9-BBF7-57ECE07E179B')
    app3 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')

    option1 = {
        "acct_id": '62777efb5510ce',
        "user_name": 'DMS',
        "app_id": '235685_4e6vScvJUlAf4eyGRd3P078v7h0ZQCPH',
        # "app_sec": 'd019b038bc3c4b02b962e1756f49e179',
        "app_sec": 'b105890b343b40ba908ed51453940935',
        "server_url": 'http://192.168.1.13/K3Cloud',
    }



    writeSRC(startDate,endDate,app2,app3,api_sdk,option1)

    data=getCode(app3)

    if data:

        res = classification_process(app3, data)

        api_sdk = K3CloudApiSdk()

        #新账套


        msg=ERP_Save(api_sdk=api_sdk, data=res, option=option1, app2=app2,app3=app3)

        return msg
    else:

        return {"message":"无订单需要同步"}


