#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# copyright (c) 2015 luffae@gmail.com
#

import config
import re
import time
import MySQLdb

from pyrrd.rrd import RRD
from datetime import datetime
from warnings import filterwarnings, resetwarnings


#
# get <current timestamp> and <crontab interval>
# calculate the <start time> <end time>
#

epoch = int(time.time())

interval = config.interval

te = epoch / interval * interval
ts = te - interval

resolution = config.resolution


#
# get <data source host> <data type> <rra path> from cacti database
#

db_cacti = MySQLdb.connect(**config.cacti_db)

cur_cacti = db_cacti.cursor(MySQLdb.cursors.DictCursor)

cur_cacti.execute(
  'SELECT name_cache, data_source_path'
  ' FROM data_template_data'
  ' WHERE data_source_path IS NOT NULL'
)
res = cur_cacti.fetchall()

db_cacti.close()

for r in res:
  s = re.split('\s-\s', r['name_cache'])
  r['host'] = s.pop(0).replace('.', '_').replace('-', '_')
  r['name'] = '_'.join(s)
  r['path'] = r['data_source_path'].replace('<path_rra>', config.cacti_rra)
  r.pop('name_cache', None)
  r.pop('data_source_path', None)


#
# dump data from rra files to mysql
#

db_myrra = MySQLdb.connect(**config.dump_to_db)

cur_myrra = db_myrra.cursor(MySQLdb.cursors.DictCursor)

# temporarily disable the "Table already exists" warning
filterwarnings('ignore', category = MySQLdb.Warning)

for host in set(r['host'] for r in res):
  cur_myrra.execute(
    'CREATE TABLE IF NOT EXISTS `' + host + '` ('
    '  name varchar(64) default NULL,'
    '  type varchar(64) default NULL,'
    '  timestamp timestamp default "0000-00-00 00:00:00",'
    '  value double default NULL,'
    '  max7d double default NULL,'
    '  min7d double default NULL,'
    '  PRIMARY KEY (name, type, timestamp)'
    ') ENGINE=MyISAM default charset=utf8'
  )

# re-enable the warning
resetwarnings()

for r in res:
  rra = RRD(r['path']).fetch(start=ts-61, end=te-61)

  for k in rra:
    for i in rra[k]:
      host  = r['host']
      name  = r['name']
      type  = k
      time  = str(i[0] / resolution * resolution)
      value = str(i[1])
      time_str = datetime.fromtimestamp(int(time)).strftime('%H:%M:%S')

      cur_myrra.execute(
        'INSERT IGNORE INTO `' + host + '`('
        '   name, type, timestamp, value, min7d, max7d'
        ')'
        ' SELECT'
        '   "' + name + '",'
        '   "' + type + '",'
        '   FROM_UNIXTIME("' + time + '"),'
        '   "' + value + '",'
        '   MIN(T.value),'
        '   MAX(T.value)'
        ' FROM ('
        '   SELECT value'
        '   FROM ' + host +
        '   WHERE time(timestamp) LIKE "' + time_str + '"'
        '     AND name="' + name + '"'
        '     AND type="' + type + '"'
        '   ORDER BY timestamp DESC LIMIT 7) AS T'
      )

db_myrra.commit()

db_myrra.close()

