import MySQLdb

def load_and_dump( path ):
	db = MySQLdb.connect(user='root', unix_socket='/tmp/mysql-ib.sock', db='stats')
	c = db.cursor()
	c.execute("""CREATE TEMPORARY TABLE loading (
	source VARCHAR( 128 ),
	linenum INT,
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
	useragent TEXT,
	languages VARCHAR( 512 ),
	type VARCHAR( 512 ),
	request_path TEXT,
	slot VARCHAR( 512 ),
	language VARCHAR( 512 ),
	referer TEXT,
	campaign VARCHAR( 512 ),
	query_what VARCHAR( 512 ),
	query_where VARCHAR( 512 ),
	query_when VARCHAR( 512 ),
	query_raw TEXT,
	results INT UNSIGNED,
	backend INT UNSIGNED,
	frontend INT UNSIGNED,
	frontend_total INT UNSIGNED,
	useragent_hash CHAR( 32 ),
	referer_hash CHAR( 32 ),
	request_path_hash CHAR( 32 ),
	query_raw_hash CHAR( 32 )
	) engine=myisam DATA DIRECTORY='/tmp/';""")
	c.execute("""load data infile %s IGNORE into table loading fields terminated by "\Z" ESCAPED BY "\\\\" LINES TERMINATED BY "\r\n" ( unixtime, source, linenum, subseconds, server, view, xsl, application, @client_ip, session, userid, useragent, languages, type, request_path, slot, language, referer, campaign, query_what, query_where, query_when, query_raw, results, backend, frontend_total )
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
	request_path_hash = MD5( request_path ),
	query_raw_hash = MD5( query_raw ),
	frontend = frontend_total - backend
	;""", path + '.processed')
	c.execute("""SELECT DISTINCT( referer_hash ), referer FROM loading WHERE referer IS NOT NULL ORDER BY referer_hash INTO OUTFILE %s FIELDS TERMINATED BY "\Z";""", path + '.referers' )
	c.execute("""SELECT DISTINCT( useragent_hash ), useragent FROM loading WHERE useragent IS NOT NULL ORDER BY useragent_hash INTO OUTFILE %s FIELDS TERMINATED BY "\Z";""", path + '.uas' )
	c.execute("""SELECT DISTINCT( query_raw_hash ), query_raw FROM loading WHERE query_raw IS NOT NULL ORDER BY query_raw_hash INTO OUTFILE %s FIELDS TERMINATED BY "\Z";""", path + '.queries' )
	c.execute("""SELECT DISTINCT( request_path_hash ), request_path FROM loading WHERE request_path IS NOT NULL ORDER BY request_path_hash INTO OUTFILE %s FIELDS TERMINATED BY "\Z";""", path + '.paths' )
	c.execute("""SELECT unixtime, source, linenum, ts, date_year, date_month, date_day, date_hour, date_minute, date_second, date_dayofweek, date_week, subseconds, server, view, xsl, application, clientip, session, userid, languages, type, slot, language, campaign, query_what, query_where, query_when, results, backend, frontend, frontend_total, useragent_hash, referer_hash, request_path_hash, query_raw_hash FROM loading INTO OUTFILE %s fields terminated by "\Z" ENCLOSED BY "'";""", path + '.dump' )
	db.close()
