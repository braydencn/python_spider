# -*- coding: utf-8 -*-

import sqlite3

c = sqlite3.connect('xuequfang.db')

sql = """ 
select h.hid, p.riqi, p.price, x.name, s.name 
from school s, xiaoqu x, house h, price p
where s.sid = x.sid and x.xid = h.xid and h.hid = p.hid
order by p.hid, p.riqi
"""

try:
    """
        c.execute("create table school(sid varchar primary key, name varchar)");
        c.execute("create table xiaoqu(xid varchar primary key, " +
	    "name varchar,sid varchar, foreign key(sid) references " +
	    "school(sid) on delete cascade on update cascade)");
	c.execute("create table house(hid varchar primary key, sid varchar, " + 
	    "xid varchar,miaoji int, niandai int, huxing varchar, " +
	    "chaoxiang varchar, louceng varchar)");
	c.execute("create table price(riqi date, hid varchar, price int, " +
	    "danjia int, shoufu int, yuegong int, primary key (riqi, hid))");
    """
    for r in c.execute(sql):
        print("%s, %s, %s, %s, %s" % (r[0], r[1], r[2], r[3], r[4]))

finally:
    c.close()
