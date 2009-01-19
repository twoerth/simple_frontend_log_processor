import frontend_log
import gzip

def nullify( input ):
	if( input == '' or input == '-' ):
		return '\\N'
	else:
		return str( input )

def dump( input, reference ):
	outfile = input + '.processed'
	dumper = frontend_log.FrontendLog( gzip.open( input ), reference )
	out = open( outfile, 'w' )
	for line in dumper:
		res = map( nullify, line )
		out.write( "\x1a".join( res ) + '\r\n' )
	out.close
	return outfile
