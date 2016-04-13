# -*- coding: utf-8 -*-

import sqlite3

sql = """
create table school(sid varchar primary key, name varchar, zone varchar);

create table xiaoqu(xid varchar primary key, name varchar,sid varchar, 
    foreign key(sid) references school(sid) on delete cascade on update 
    cascade);

create table house(hid varchar primary key, xid varchar, miaoji int, niandai 
    int, huxing varchar, chaoxiang varchar, louceng varchar, label varchar,
    foreign key(xid) references xiaoqu(xid) on delete cascade on update 
    cascade);

create table price(riqi date, hid varchar, price int, primary key (riqi, hid), 
    foreign key(hid) references house(hid) on delete cascade on update cascade) 
"""

c = sqlite3.connect('xuequfang.db')
try:
    with c:
        c.executescript(sql)
finally:
    c.close()
