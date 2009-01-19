import sys
import MySQLdb

db = MySQLdb.connect(unix_socket='/tmp/mysql-ib.sock', db='stats')

file = sys.argv[1]

c = db.cursor()
c.execute("""CREATE TEMPORARY TABLE loading (
unixtime INT UNSIGNED,
ts TIMESTAMP,
date_year SMALLINT UNSIGNED,
date_month TINYINT UNSIGNED NOT NULL,
date_day TINYINT UNSIGNED NOT NULL,
date_hour TINYINT UNSIGNED NOT NULL,
date_minute  TINYINT UNSIGNED NOT NULL,
date_second  TINYINT UNSIGNED NOT NULL,
date_dayofweek TINYINT UNSIGNED,
date_week TINYINT UNSIGNED,
subseconds SMALLINT UNSIGNED,
server VARCHAR( 15 ),
view VARCHAR( 512 ),
xsl VARCHAR( 512 ),
application VARCHAR( 256 ),
clientip INT UNSIGNED,
session CHAR( 32 ),
userid VARCHAR( 32 ),
useragent VARCHAR( 512 ),
languages VARCHAR( 512 ),
type VARCHAR( 512 ),
path VARCHAR( 8192 ),
slot VARCHAR( 512 ),
language VARCHAR( 512 ),
referer TEXT,
campaign VARCHAR( 512 ),
query_what VARCHAR( 512 ),
query_where VARCHAR( 512 ),
query_when VARCHAR( 512 ),
query_raw VARCHAR( 1024 ),
results INT UNSIGNED,
backend INT UNSIGNED,
frontend INT UNSIGNED,
frontend_total INT UNSIGNED,
useragent_hash CHAR( 32 ),
referer_hash CHAR( 32 )
) engine=myisam DATA DIRECTORY='/tmp/';""")

c.execute("""load data infile %s IGNORE into table loading fields terminated by "\Z" ESCAPED BY "\\\\" LINES TERMINATED BY "\r\n" ( unixtime, subseconds, server, view, xsl, application, @client_ip, session, userid, useragent, languages, type, path, slot, language, referer, campaign, query_what, query_where, query_when, query_raw, results, backend, frontend_total )
SET ts = FROM_UNIXTIME( unixtime ),
date_year = YEAR( ts ),
date_month = MONTH( ts ),
date_day = DAYOFMONTH( ts ),
date_hour = HOUR( ts ),
date_minute = MINUTE( ts ),
date_second = SECOND( ts ),
date_dayofweek = DAYOFWEEK( ts ),
date_week = WEEK( ts ),
clientip = INET_ATON( @client_ip ),
useragent_hash = MD5( useragent ),
referer_hash = MD5( referer ),
frontend = frontend_total - backend
;""", file + '.processed')

c.execute("""INSERT IGNORE INTO ips (ip) SELECT clientip FROM loading;""")

c.execute("""INSERT IGNORE INTO referers (hash, referer) SELECT referer_hash, referer FROM loading WHERE referer IS NOT NULL;""")

c.execute("""SELECT unixtime, ts, date_year, date_month, date_day, date_hour, date_minute, date_second, date_dayofweek, date_week, subseconds, server, view, xsl, application, clientip, session, userid, languages, type, path, slot, language, campaign, query_what, query_where, query_when, query_raw, results, backend, frontend, frontend_total, useragent_hash, referer_hash FROM loading INTO OUTFILE %s fields terminated by "\Z" ENCLOSED BY "'";""", file + '.dump' )

# r = c.execute("""load data infile %s into table historical fields terminated by "\Z" enclosed by "'" escaped by "\\\\" lines terminated by "\r\n" (unixtime, ts, date_year, date_month, date_day, date_hour, date_minute, date_second, date_dayofweek, date_week, subseconds, server, view, xsl, application, clientip, session, userid, languages, type, path, slot, language, campaign, query_what, query_where, query_when, query_raw, results, backend, frontend, frontend_total, useragent_hash, referer_hash);""", file + '.dump' )

db.commit()
