# -*- coding: utf-8 -*-

import sqlite3
import urllib.request
import re
import threading
import logging

DB_NAME = 'xuequfang.db'
TIMEOUT = 60

def querySQL(sql):
    #conn = getattr(threading.local(), 'conn', sqlite3.connect(DB_NAME))
    conn = sqlite3.connect(DB_NAME)
    try:
        for row in conn.execute(sql):
            yield row
    finally:
        conn.close()

def execSQL(sql):
    conn = sqlite3.connect(DB_NAME)
    done = False
    try:
        while not done:
            try:
                conn.execute(sql)
            except:
                print('aaaa')
            else:
                done = True
    finally:
        conn.commit()
        conn.close()

def storeOneXiaoqu(xid, xname, sid):
    sql = "replace into xiaoqu values('%s', '%s', '%s')" 
    execSQL(sql % (xid, xname, sid))

def storePrice(hid, price, danjia, shoufu, yuegong):
    sql = "replace into price values(%s, '%s', '%s', '%s', '%s', '%s')" 
    execSQL(sql % ('CURRENT_DATE', hid, price, danjia, shoufu, yuegong))

def storeOneHouse(hid, sid, xid, mianji, huxing, chaoxiang, louceng):
    sql = "replace into house values('%s', '%s', '%s', '%s', '%s', '%s', "
    sql += "'%s', '%s')" 
    execSQL(sql % (hid, sid, xid, mianji, '0', huxing, chaoxiang, louceng))

def oneHouse(sid, xid, hid):
    prefix = "http://bj.lianjia.com/ershoufang/"
    pat = r"售价：</dt><dd><span class=\"em-text\"><strong class=\"ft-num\">(\d+)</strong><span class=\"sub-text\">[^<]*</span><i>/ (\d+)㎡</i></span></dd></dl><dl><dt>单价：</dt><dd class=\"short\">(\d+) 元/平米</dd></dl><dl><dt>首付：</dt><dd class=\"short\">([^<]+)</dd></dl><dl><dt>月供：</dt><dd class=\"short\">([^<]+)</dd></dl><dl><dt>户型：</dt><dd>([^<]+)</dd></dl><dl><dt>朝向：</dt><dd>([^<]+)</dd></dl><dl><dt>楼层：</dt><dd>([^<]+)</dd></dl><dl class=\"clear\"><dt>小区：</dt><dd><a class=\"zone-name laisuzhou\" data-bl=\"area\" data-el=\"community\" href=\"http://bj.lianjia.com/xiaoqu/\d+/\">[^<]+</a>"
    with urllib.request.urlopen(prefix + hid + ".html", None, TIMEOUT) as f:
        for m in re.finditer(pat, f.read().decode('utf-8')):
            price = m.group(1)
            mianji = m.group(2)
            danjia = m.group(3)
            shoufu = m.group(4)
            yuegong = m.group(5)
            huxing = m.group(6)
            chaoxiang = m.group(7)
            louceng = m.group(8)
            storeOneHouse(hid, sid, xid, mianji, huxing, chaoxiang, louceng)
            storePrice(hid, price, danjia, shoufu, yuegong)

def oneXiaoqu(sid, xid, xname):
    logger.info('    processing xiaoqu: %s', xname)
    storeOneXiaoqu(xid, xname, sid)
    houseSet = set()
    prefix = "http://bj.lianjia.com/ershoufang/"
    with urllib.request.urlopen(prefix + xid, None, TIMEOUT) as f:
        for m in re.finditer(r"(BJ\w+\d+).html", f.read().decode('utf-8')):
            houseSet.add(m.group(1))
    for h in houseSet:
        oneHouse(sid, xid, h)
    logger.info('    done with xiaoqu: %s', xname)

def oneSchool(sid):
    prefix = "http://bj.lianjia.com/xuequfang/" 
    threads = [] 
    with urllib.request.urlopen(prefix + sid + ".html", None, TIMEOUT) as f:
        pattern = r"(sch" + sid + "c\d+)[^<]+<span class=\"sp01\">([^<]+)"
        for m in re.finditer(pattern, f.read().decode('utf-8')):
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

rows = [] 
for row in querySQL("select sid, name from school"):
    rows.append([row[0], row[1]])
threads = []
for row in rows:
    logger.info('processing school: %s', row[1])
    t = threading.Thread(target = oneSchool, args = [row[0]])
    t.start()
    threads.append(t)
for t in threads:
    t.join()
