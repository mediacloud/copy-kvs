use strict;
use warnings;

require 't/test_helpers.inc.pl';

# NoWarnings test fails because of Net::Amazon::S3:
#
#     Passing a list of values to enum is deprecated. Enum values should be
#     wrapped in an arrayref. at /System/Library/Perl/Extras/5.18/darwin-thread
#     -multi-2level/Moose/Util/TypeConstraints.pm line 442.
#
# use Test::NoWarnings;

use Test::More;
use Test::Deep;

if (s3_test_configuration_is_set()) {
	plan tests => 3;
} else {
	plan skip_all => "S3 test configuration is not set in the environment.";
}

use constant NUMBER_OF_TEST_FILES => 10;

use Data::Dumper;

BEGIN {
	use FindBin;
	use lib "$FindBin::Bin/../lib";

	use GridFSToS3;
	use Storage::Handler::GridFS;
	use Net::Amazon::S3;
	use MongoDB;
}

# Connection configuration
my $config = configuration_from_env();

# Create temporary bucket for unit tests
my $mongodb_connector = $config->{ connectors }->{ "mongodb_gridfs_test" };
my $s3_connector = $config->{ connectors }->{ "amazon_s3_test" };

# Randomize directory name so that multiple tests could run concurrently
$s3_connector->{ directory_name } = $s3_connector->{ directory_name } . '-' . random_string( 16 );

my $native_s3 = Net::Amazon::S3->new({
	aws_access_key_id     => $s3_connector->{ access_key_id },
	aws_secret_access_key => $s3_connector->{ secret_access_key },
	retry                 => 1,
});
my $test_bucket = $native_s3->add_bucket( { bucket => $s3_connector->{ bucket_name } } )
	or die $native_s3->err . ": " . $native_s3->errstr;

# Create temporary databases for unit tests
my $native_mongo_client = MongoDB::MongoClient->new(
	host => $mongodb_connector->{ host },
	port => $mongodb_connector->{ port }
);
# Should auto-create on first write
my $test_source_database_name = $mongodb_connector->{ database } . '_src_' . random_string(16);
say STDERR "Source database name: $test_source_database_name";
my $native_source_mongo_database = $native_mongo_client->get_database( $test_source_database_name );
my $gridfs_source = Storage::Handler::GridFS->new(
    host => $mongodb_connector->{ host },
    port => $mongodb_connector->{ port },
    database => $test_source_database_name
);

my $test_destination_database_name = $mongodb_connector->{ database } . '_dst_' . random_string(16);
say STDERR "Destination database name: $test_destination_database_name";
my $native_destination_mongo_database = $native_mongo_client->get_database( $test_destination_database_name );
my $gridfs_destination = Storage::Handler::GridFS->new(
    host => $mongodb_connector->{ host },
    port => $mongodb_connector->{ port },
    database => $test_destination_database_name
);

# Create test files
my @files;
for (my $x = 0; $x < NUMBER_OF_TEST_FILES; ++$x) {
	push(@files, {filename => 'file-' . random_string(32), contents => random_string(128)});
}

# Store files into the source GridFS database
for my $file (@files) {
	$gridfs_source->put($file->{filename}, $file->{contents});
}

# Copy files from source GridFS database to S3
$config->{ connectors }->{ "mongodb_gridfs_test" }->{database } = $test_source_database_name;
ok( GridFSToS3::copy_kvs( $config, "mongodb_gridfs_test", "amazon_s3_test" ), "Copy from source GridFS to S3" );

# Copy files back from S3 to GridFS
$config->{ connectors }->{ "mongodb_gridfs_test" }->{database } = $test_destination_database_name;
ok( GridFSToS3::copy_kvs( $config, "amazon_s3_test", "mongodb_gridfs_test" ), "Copy from S3 to destination GridFS" );

# Compare files
my $response = $test_bucket->list_all;
my @files_restored_from_s3;
foreach my $key ( @{ $response->{keys} } ) {
	my $file = $test_bucket->get_key($key->{key});
	$file = {
		filename => $key->{key},
		contents => $file->{value}
	};
	if ($s3_connector->{ directory_name }) {
		# Strip directory prefix
		$file->{filename} =~ s/^$s3_connector->{ directory_name }\///;
	}
	push (@files_restored_from_s3, $file);
}
# say STDERR "Expected: " . Dumper(@files);
# say STDERR "Got: " . Dumper(@files_restored_from_s3);
cmp_bag(\@files, \@files_restored_from_s3, 'List of files and their contents match');

# Delete temporary bucket and databases, remove "last filename" files
$response = $test_bucket->list_all;
foreach my $key ( @{ $response->{keys} } ) {
	$test_bucket->delete_key($key->{key});
}

$native_source_mongo_database->drop;
$native_destination_mongo_database->drop;

unlink $config->{ connectors }->{ "mongodb_gridfs_test" }->{ last_copied_file };
unlink $config->{ connectors }->{ "amazon_s3_test" }->{ last_copied_file };
