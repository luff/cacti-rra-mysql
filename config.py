
interval = 300

resolution = 300

cacti_rra = '/var/lib/cacti/rra'

cacti_db = {
  'host'    : 'localhost',
  'port'    : 3306,
  'db'      : 'cacti',
  'user'    : 'cactiuser',
  'passwd'  : 'cactiuser',
  'charset' : 'utf8',
}

dump_to_db = {
  'host'    : 'localhost',
  'port'    : 3306,
  'db'      : 'rra',
  'user'    : 'ops',
  'passwd'  : 'ops',
  'charset' : 'utf8',
}

