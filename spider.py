# -*- coding: utf-8 -*-

import sqlite3
import urllib.request
import re
import threading
import logging
import time

DB_NAME = 'xuequfang.db'
TIMEOUT = 60

def getURL(url):
    page = ''
    retry = 3
    while retry > 0:
        try:
            with urllib.request.urlopen(url) as f:
                page = f.read().decode('utf-8')
        except:
           retry -= 1
           continue
        break
    if retry <= 0:
        logger.warning('Cannot retrieve URL: ' + url)
    return page

def querySQL(sql):
    conn = sqlite3.connect(DB_NAME)
    try:
        for row in conn.execute(sql):
            yield row
    finally:
        conn.close()

def execSQL(sql):
    conn = sqlite3.connect(DB_NAME)
    retry = 2
    try:
        while retry > 0:
            try:
                conn.execute(sql)
            except:
                retry -= 1
                continue
            break
    finally:
        conn.commit()
        conn.close()

def storePrices():
    sql = "replace into price values " 
    fmt = "(%s, '%s', '%s', '%s', '%s', '%s')"
    start = True
    for h in local.prices:
        if not start:
            sql += ', '
        else:
            start = False
        sql += fmt % ('CURRENT_DATE', h[0], h[1], h[2], h[3], h[4])
    execSQL(sql) 

def storeHouses():
    sql = "insert or ignore into house values " 
    fmt = "('%s', '%s', '%s', '%s', '%s', '%s', '%s')"  
    start = True
    for h in local.houses:
        if not start:
            sql += ', '
        else:
            start = False
        sql += fmt % (h[0], h[1], h[2], '0', h[3], h[4], h[5])
    execSQL(sql) 


def storeOneXiaoqu(xid, xname, sid):
    sql = "insert or ignore into xiaoqu values('%s', '%s', '%s')" 
    execSQL(sql % (xid, xname, sid))

def oneHouse(xid, hid):
    prefix = "http://bj.lianjia.com/ershoufang/"
    pat = r"售价：</dt><dd><span class=\"em-text\"><strong class=\"ft-num\">(\d+)</strong><span class=\"sub-text\">[^<]*</span><i>/ (\d+)㎡</i></span></dd></dl><dl><dt>单价：</dt><dd class=\"short\">(\d+) 元/平米</dd></dl><dl><dt>首付：</dt><dd class=\"short\">([^<]+)</dd></dl><dl><dt>月供：</dt><dd class=\"short\">([^<]+)</dd></dl><dl><dt>户型：</dt><dd>([^<]+)</dd></dl><dl><dt>朝向：</dt><dd>([^<]+)</dd></dl><dl><dt>楼层：</dt><dd>([^<]+)</dd></dl><dl class=\"clear\"><dt>小区：</dt><dd><a class=\"zone-name laisuzhou\" data-bl=\"area\" data-el=\"community\" href=\"http://bj.lianjia.com/xiaoqu/\d+/\">[^<]+</a>"
    for m in re.finditer(pat, getURL(prefix + hid + ".html")):
        price = m.group(1)
        mianji = m.group(2)
        danjia = m.group(3)
        shoufu = m.group(4)
        yuegong = m.group(5)
        huxing = m.group(6)
        chaoxiang = m.group(7)
        louceng = m.group(8)
        local.houses.append((hid, xid, mianji, huxing, chaoxiang, louceng))
        local.prices.append((hid, price, danjia, shoufu, yuegong))

def oneXiaoquPage(xid, xname, pg):
    houseSet = set()
    prefix = "http://bj.lianjia.com/ershoufang/"
    pattern = r"(BJ\w+\d+)\.html.*?<div class=\"price\"><span class=\"num\">(\d+)</span>万</div>"
    for m in re.finditer(pattern, getURL(prefix + pg)):
        if int(m.group(2)) < 600:
            houseSet.add(m.group(1))
    local.houses = []
    local.prices = []
    for h in houseSet:
        oneHouse(xid, h)
    storeHouses()
    storePrices()
    
def oneXiaoqu(sid, xid, xname):
    logger.info('    processing xiaoqu: %s', xname)
    storeOneXiaoqu(xid, xname, sid)
    threads = [] 
    prefix = "http://bj.lianjia.com/ershoufang/"
    pattern = "pg\d+%s" % (xid)
    for pg in re.findall(pattern, getURL(prefix + xid)):
        t = threading.Thread(target = oneXiaoquPage, 
            args = (xid, xname, pg))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    if not threads:
        oneXiaoquPage(xid, xname, xid)
    logger.info('    done with xiaoqu: %s', xname)

def oneSchool(sid):
    prefix = "http://bj.lianjia.com/xuequfang/" 
    threads = [] 
    pattern = r"(sch" + sid + "c\d+)[^<]+<span class=\"sp01\">([^<]+)"
    for m in re.finditer(pattern, getURL(prefix + sid + ".html")):
        xid = m.group(1)
        xname = m.group(2)
        t = threading.Thread(target = oneXiaoqu, args = (sid, xid, xname))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

logger = logging.getLogger()
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)

local = threading.local()

rows = [] 
for row in querySQL("select sid, name from school"):
    rows.append([row[0], row[1]])
threads = []
for row in rows:
    logger.info('processing school: %s', row[1])
    t = threading.Thread(target = oneSchool, args = [row[0]])
    t.start()
    threads.append(t)
start = time.time()
for t in threads:
    t.join()
logger.info("Finished. Time spent: %f " % (time.time() - start))
