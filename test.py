# -*- coding: utf-8 -*-

import sqlite3

c = sqlite3.connect('xuequfang.db')

#sql = "select * from school"
#sql = "select * from xiaoqu"
sql = "select hid, xid from house"

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
        #print("%s, %s, %s, %s, %s" % (r[0], r[1], r[2], r[3], r[4]))
        print("%s, %s" % (r[0], r[1]))
        #print("%s, %s, %s" % (r[0], r[1], r[2]))

finally:
    c.close()
