# -*- coding: utf-8 -*-

import sqlite3

sql = """
create table school(sid varchar primary key, name varchar);

create table xiaoqu(xid varchar primary key, name varchar,sid varchar, 
    foreign key(sid) references school(sid) on delete cascade on update 
    cascade);

create table house(hid varchar primary key, sid varchar, xid varchar, miaoji 
    int, niandai int, huxing varchar, chaoxiang varchar, louceng varchar);

create table price(riqi date, hid varchar, price int, danjia int, shoufu int, 
    yuegong int, primary key (riqi, hid)) 
"""

c = sqlite3.connect('xuequfang.db')
try:
    with c:
        c.executescript(sql)
finally:
    c.close()
