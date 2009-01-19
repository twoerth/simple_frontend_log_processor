import dumper
import dbimporter
import tempfile
import os
import ez_aws

os.environ['TMPDIR'] = '/tmp/'

queue = ez_aws.Queue( 'frontend-logs' )

raw_bucket = ez_aws.Bucket( 'localch-frontend-logs' )
processed_bucket = ez_aws.Bucket( 'localch-frontend-logs-processed' )

for message in queue:
	tmp = tempfile.mkstemp()[1]
	logfile = message.get_body()
	raw_bucket.download_item_to_localfile( logfile, tmp )	
	dumper.dump( tmp, logfile )
	dbimporter.load_and_dump( tmp )
	processed_bucket.upload_localfile_to_item( tmp + '.dump', 'dump_' + logfile )
	processed_bucket.upload_localfile_to_item( tmp + '.paths', 'paths_' + logfile )
	processed_bucket.upload_localfile_to_item( tmp + '.queries', 'queries_' + logfile )
	processed_bucket.upload_localfile_to_item( tmp + '.referers', 'referers_' + logfile )
	processed_bucket.upload_localfile_to_item( tmp + '.uas', 'uas_' + logfile )
#	os.unlink( tmp )
#	os.unlink( tmp + '.processed' )
	queue.delete( message )