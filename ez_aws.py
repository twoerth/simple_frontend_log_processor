"""
Simple abstractions on top of the boto library.

The goal is to encapsulate the AWS operations into simpler concepts (Queue, Bucket, etc.) and
improve the readability of the call calling ez_aws.

There is no attempt to come anywhere near completeness.
"""

import boto
from boto.s3.key import Key

class Queue:
	def __init__( self, queue_name ):
		self.queue = boto.connect_sqs().create_queue( queue_name )
		
	def __iter__( self ):
		while int( self.queue.count() ) > 0:
			yield self.queue.read( visibility_timeout=3600 )
			
	def delete( self, message ):
		self.queue.delete_message( message )
		
class Bucket:
	def __init__( self, bucket_name ):
		self.bucket = boto.connect_s3().get_bucket( bucket_name )
	
	def download_item_to_localfile( self, key, filename ):
		k = Key( self.bucket )
		k.name = key
		k.get_contents_to_filename( filename )		
		
	def upload_localfile_to_item( self, filename, key ):
		k = Key( self.bucket )
		k.name = key
		k.set_contents_from_filename( filename )		
