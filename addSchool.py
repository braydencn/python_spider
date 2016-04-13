﻿# -*- coding: utf-8 -*-

import sqlite3

sql = """
replace into school values 
    ('4000001020',   '中关村一小',   '海淀'), 
    ('4000001023',   '上地实验小学', '海淀'), 

    ('47031207',     '朝阳外国语',   '朝阳'),

    ('4000000640',   '青年湖小学',   '东城'),

    ('4000000683',   '育翔小学',     '西城'),
    ('4000000666',   '裕中小学',     '西城'),
    ('4000000686',   '五路通小学',   '西城'),
    ('47715332',     '陶然亭小学',   '西城'),
    ('47715578',     '八中附小',     '西城'),
    ('4000000736',   '椿树馆小学',   '西城'),
    ('4000000671',   '西师附小',     '西城'), 
    ('4000000705',   '展一小',       '西城')
"""

c = sqlite3.connect('xuequfang.db')
try:
    with c:
        c.executescript(sql)
finally:
    c.close()
