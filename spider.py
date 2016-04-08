# -*- coding: utf-8 -*-

import sqlite3
import urllib.request
import re
import threading
import logging
import time

DB_NAME = 'xuequfang.db'
TIMEOUT = 60
PRE_XQF = "http://bj.lianjia.com/xuequfang/"
PRE_ESF = "http://bj.lianjia.com/ershoufang/"

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
    logger.info(sql)
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

def storePrices(prices):
    sql = "insert or ignore into price values " 
    fmt = "(%s, '%s', '%s')"
    start = True
    for h in prices:
        if not start:
            sql += ', '
        else:
            start = False
        sql += fmt % ('CURRENT_DATE', h[0], h[1])
    execSQL(sql) 

def storeHouses(houses):
    sql = "insert or ignore into house values " 
    fmt = "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"  
    start = True
    for h in houses:
        if not start:
            sql += ', '
        else:
            start = False
        sql += fmt % (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7])
    execSQL(sql) 


def storeOneXiaoqu(xid, xname, sid):
    #sql = "insert or ignore into xiaoqu values('%s', '%s', '%s')" 
    sql = "replace into xiaoqu values('%s', '%s', '%s')" 
    execSQL(sql % (xid, xname, sid))

def oneXiaoquPage(xid, xname, pg):
    houses = []
    prices = []
    pattern = r"(BJ\w+\d+)\.html.*?data-el=\"xiaoqu\"><span class=\"\">[^<]+</a></span>\&nbsp;\&nbsp;<span class=\"\"><span>([^<]+)\&nbsp;\&nbsp;</span></span><span class=\"\">([\d.]+)平米\&nbsp;\&nbsp;</span><span>([^<]+)</span></div>(.*?)<div class=\"chanquan\"><div class=\"left agency\"><div class=\"view-label left\">(.*?)<div class=\"col-3\"><div class=\"price\"><span class=\"num\">(\d+)</span>万"
    handled = False
    for m in re.finditer(pattern, getURL(PRE_ESF + pg)):
        handled = True
        hid = m.group(1)
        huxing = m.group(2)
        mianji = m.group(3)
        chaoxiang = m.group(4)
        others = m.group(5)
        labels = m.group(6)
        price = m.group(7)
        if int(price) > 500:
            continue
        m1 = re.search(r"data-el=\"region\">[^<]+</a><span>/</span>([^<]+)<", others)
        louceng = ''
        if m1 is not None:
            louceng = m1.group(1)
        m2 = re.search(r">(\d+)年建", others)
        niandai = ''
        if m2 is not None:
            niandai = m2.group(1)
        if niandai != '' and int(niandai) < 1980:
            continue
        m3 = re.search(r"<span class=\"five-ex\"><span>([^<]+)<", labels)
        label = ''
        if m3 is not None:
            label = m3.group(1)
        m4 = re.search(r"<span class=\"taxfree-ex\"><span>([^<]+)<", labels)
        if m4 is not None:
            label = m4.group(1)
        if label == '':
            continue
        houses.append((hid, xid, mianji, niandai, huxing, chaoxiang, 
            louceng, label))
        prices.append((hid, price))
    if not handled:
        logger.info("%s not handled!" % (pg))
    if houses:
        storeHouses(houses)
    if prices:
        storePrices(prices)
    
def oneXiaoqu(sid, xid, xname):
    logger.info('    processing xiaoqu: %s', xname)
    storeOneXiaoqu(xid, xname, sid)
    threads = [] 
    pattern = "pg\d+%s" % (xid)
    for pg in re.findall(pattern, getURL(PRE_ESF + xid)):
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
    threads = [] 
    #pattern = r"(sch%sc\d+)[^<]+<span class=\"sp01\">([^<]+)" % (sid)
    pattern = r"<span class=\"names\">[^>]+(sch%sc\d+)[^>]+>([^<]+)<" % (sid)
    for m in re.finditer(pattern, getURL("%s%s.html" % (PRE_XQF, sid))):
        xid = m.group(1)
        xname = m.group(2).strip(" \t\n")
        t = threading.Thread(target = oneXiaoqu, args = (sid, xid, xname))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

def setupLogging():
    #logger.setLevel(logging.WARNING)
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    logger.addHandler(ch)

def allSchools():
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

logger = logging.getLogger()
setupLogging()
allSchools()
