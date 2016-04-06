# -*- coding: utf-8 -*-

import sqlite3

sql = """
replace into school 
values ('4000000671', '西师附小'), 
       ('4000001020', '中关村一小'), 
       ('4000001023', '上地实验小学'), 
       ('47031207',   '朝阳外国语'), 
       ('47715631',   '朝阳外国语分校')
"""

c = sqlite3.connect('xuequfang.db')
try:
    with c:
        c.executescript(sql)
finally:
    c.close()
