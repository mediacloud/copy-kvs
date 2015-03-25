use strict;
use warnings;

# Generate random alphanumeric string (password or token) of the specified length
sub random_string($)
{
    my ( $num_bytes ) = @_;
    return join '', map +( 0 .. 9, 'a' .. 'z', 'A' .. 'Z' )[ rand( 10 + 26 * 2 ) ], 1 .. $num_bytes;
}

sub s3_test_configuration_is_set()
{
	return (defined $ENV{ COPY_KVS_S3_ACCESS_KEY_ID }
		and defined $ENV{ COPY_KVS_S3_SECRET_ACCESS_KEY }
		and defined $ENV{ COPY_KVS_S3_BUCKET_NAME });
}

# Get configuration from the environment variable
sub configuration_from_env()
{
	my $test_config = {
		lock_file => "copy-kvs.lock",
		worker_threads => 32,
		job_chunk_size => 512,
		connectors => {
			"mongodb_gridfs_test" => {
				type => "GridFS",
				host => "localhost",
				port => 27017,
				database => "copy-kvs-test-" . random_string(16),
				timeout => -1,
				last_copied_file => "copy-kvs-gridfs-" . random_string(16) . ".last",
			},
			"amazon_s3_test" => {
				type => "AmazonS3",
				access_key_id => $ENV{ COPY_KVS_S3_ACCESS_KEY_ID },
				secret_access_key => $ENV{ COPY_KVS_S3_SECRET_ACCESS_KEY },
				bucket_name => $ENV{ COPY_KVS_S3_BUCKET_NAME },
				directory_name => 'copy-kvs-test',
				timeout => 60,
				use_ssl => 0,
				head_before => {
					put => 0,
					get => 0,
					delete => 0,
				},
				overwrite => 1,
				last_copied_file => "copy-kvs-s3-" . random_string(16) . ".last",
			},
		}
	};

	return $test_config;
}

1;
