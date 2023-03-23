#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyrdo.sys import Sys

from . import salesorder
from . import purchaseorder
from . import receiptnotice
from . import noticeshipment
from . import purchasestorage
from . import saledelivery
from . import salesbilling
from . import purchasesbilling
from . import otherinstock
from . import otherout
from . import returnrequest
from . import returnnotice
from . import returnsales
from . import returnpurchase
from pyrda.dbms.rds import RdClient

def getdate():
    app2 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')

    mydate = Sys().date()

    sql = f"SELECT FStartDate,FEndDate,rdsalesorder_FIsDo FROM [dbo].[RDS_ECS_ODS_FDateTime] WHERE rdsalesorder_FIsDo=0 and FStartDate<='{mydate}' ORDER  BY FStartDate ASC"

    res = app2.select(sql)

    if res:

        return res

    else:

        return []

    pass
def ecsbill_syncBody(startDate, endDate):
    app2 = RdClient(token='9B6F803F-9D37-41A2-BDA0-70A7179AF0F3')


    res = salesorder.salesOrder(startDate, endDate)
    print(res)

    res = purchaseorder.purchaseOrder(startDate, endDate)
    print(res)

    res = noticeshipment.noticeShipment(startDate, endDate)
    print(res)

    res = receiptnotice.receiptNotice(startDate, endDate)
    print(res)

    res = saledelivery.saleOut(startDate, endDate)
    print(res)

    res = purchasestorage.purchaseStorage(startDate, endDate)
    print(res)

    res = otherout.otherOut(startDate, endDate)
    print(res)

    res = otherinstock.otherInStock(startDate, endDate)
    print(res)

    res = salesbilling.salesBilling(startDate, endDate)
    print(res)

    res = purchasesbilling.purchasesBilling(startDate, endDate)
    print(res)

    res = returnnotice.returnNotice(startDate, endDate)
    print(res)

    res = returnrequest.returnRequest(startDate, endDate)
    print(res)

    res = returnsales.returnSale(startDate, endDate)
    print(res)

    res = returnpurchase.returnPurchase(startDate, endDate)
    print(res)

    sql1 = f"update a set a.rdsalesorder_FIsDo=1 from RDS_ECS_ODS_FDateTime a where a.FStartDate='{startDate}'"

    app2.update(sql1)

    print("同步完成")
def ecsbill_sync():
    date = getdate()
    for i in date:
        ecsbill_syncBody(i['FStartDate'], i['FEndDate'])


if __name__ == '__main__':
    pass







