"""
0: ID
1: TIMESTAMP
2: SERVER
3: INTERNAL_VIEW
4: INTERNAL_XSL
( APPNAME in the .7 logs )
5: CLIENT_IP
6: CLIENT_SESSION_ID
7: CLIENT_USER_ID
8: CLIENT_USERAGENT
9: CLIENT_LANGUAGES
10: CLIENT_COOKIES
11: REQUEST_TYPE
12: REQUEST_PATH
13: REQUEST_SLOT
14: REQUEST_LANGUAGE
15: REQUEST_REFERER
16: REQUEST_CAMPAIGN
17: QUERY_WHAT
18: QUERY_WHERE
19: QUERY_WHEN
20: QUERY_RAW
21: QUERY_RESULTS
22: BACKEND_TOTAL
23: FRONTEND_TOTAL
"""

class FormatError(Exception):
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return repr(self.value)

class FrontendLog:
	def __init__(self, stream, filename ):
		self.stream = stream
		self.stream.readline()
		self.linenum = 0
		self.filename = filename

	def close(self):
		self.f.close()
	
	def second_to_strint( self, input ):
		if input == '':
			return '-'
		else:
			return str( int( float( input ) * 1000000 ) )
		
	def __iter__(self):
		for line in self.stream:
			self.linenum += 1
			data = line.rstrip().split("\t")
			ts = data[1].split('.')
			if len( ts ) == 1:
				ts.append( '0' )

			try:
				if len( data ) == 25:
					res = (
						ts[0],
						self.filename,
						self.linenum,
						ts[1],
						data[2],
						data[3],
						data[4],
						data[5],
						data[6],
						data[7],
						data[8],
						data[9],
						data[10],
						data[12],
						data[13],
						data[14],
						data[15],
						data[16],
						data[17],
						data[18],
						data[19],
						data[20],
						data[21],
						data[22],
						self.second_to_strint( data[23] ),
						self.second_to_strint( data[24] )
					)
					yield res
				elif len( data ) == 24:				
					res = (
						ts[0],
						self.filename,
						self.linenum,
						ts[1],
						data[2],
						data[3],
						data[4],
						'legacy',
						data[5],
						data[6],
						data[7],
						data[8],
						data[9],
						data[11],
						data[12],
						data[13],
						data[14],
						data[15],
						data[16],
						data[17],
						data[18],
						data[19],
						data[20],
						data[21],
						self.second_to_strint( data[22] ),
						self.second_to_strint( data[23] )
					)
					yield res
				elif len( data ) == 21:
					res = (
						ts[0],
						self.filename,
						self.linenum,
						ts[1],
						data[2],
						data[3],
						data[4],
						'legacy',
						data[5],
						data[6],
						data[7],
						data[8],
						data[9],
						data[11],
						data[12],
						data[13],
						data[14],
						data[15],
						data[16],
						data[17],
						data[18],
						data[19],
						data[20],
						'', '', ''
					)
					yield res
				else:
					raise FormatError()
					
			except Exception, e:
				print 'Exception : ' + line