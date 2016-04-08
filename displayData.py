# -*- coding: utf-8 -*-

import sqlite3

c = sqlite3.connect('xuequfang.db')

sql = """ 
select h.hid, p.riqi, p.price, x.name, s.name 
from school s, xiaoqu x, house h, price p
where s.sid = x.sid and x.xid = h.xid and h.hid = p.hid
order by p.price
"""

try:
    for r in c.execute(sql):
        print("%s, %s, %s, %s, %s" % (r[0], r[1], r[2], r[3], r[4]))

finally:
    c.close()
