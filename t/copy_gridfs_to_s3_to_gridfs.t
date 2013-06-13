use strict;
use warnings;

require 't/test_helpers.inc.pl';

use Test::NoWarnings;
use Test::More tests => 8;
use Test::Deep;

use constant NUMBER_OF_TEST_FILES => 10;

use Data::Dumper;

BEGIN { use_ok('GridFSToS3'); }
BEGIN { use_ok('Storage::Handler::GridFS'); }
BEGIN { use_ok('Net::Amazon::S3'); }
BEGIN { use_ok('MongoDB'); }

# Connection configuration
my $config = configuration_from_env();

# Rename backup / restore files to not touch the "production" ones
$config->{lock_file} .= random_string(32);
$config->{file_with_last_filename_copied_from_gridfs_to_s3} .= random_string(32);
$config->{file_with_last_filename_copied_from_s3_to_gridfs} .= random_string(32);

# Create temporary bucket for unit tests
my $test_bucket_name = 'gridfs-to-s3.testing.' . random_string(32);
my $native_s3 = Net::Amazon::S3->new({
	aws_access_key_id     => $config->{amazon_s3}->{access_key_id},
	aws_secret_access_key => $config->{amazon_s3}->{secret_access_key},
	retry                 => 1,
});
my $test_bucket = $native_s3->add_bucket( { bucket => $test_bucket_name } )
	or die $native_s3->err . ": " . $native_s3->errstr;

# Create temporary databases for unit tests
my $native_mongo_client = MongoDB::MongoClient->new(
	host => $config->{mongodb_gridfs}->{host},
	port => $config->{mongodb_gridfs}->{port}
);
# Should auto-create on first write
my $test_source_database_name = 'gridfs-to-s3_testing_source_' . random_string(16);
say STDERR "Source database name: $test_source_database_name";
my $native_source_mongo_database = $native_mongo_client->get_database($test_source_database_name);
my $gridfs_source = Storage::Handler::GridFS->new(
    host => $config->{mongodb_gridfs}->{host} || 'localhost',
    port => $config->{mongodb_gridfs}->{port} || 27017,
    database => $test_source_database_name
);

my $test_destination_database_name = 'gridfs-to-s3_testing_destination_' . random_string(16);
say STDERR "Destination database name: $test_destination_database_name";
my $native_destination_mongo_database = $native_mongo_client->get_database($test_destination_database_name);
my $gridfs_destination = Storage::Handler::GridFS->new(
    host => $config->{mongodb_gridfs}->{host} || 'localhost',
    port => $config->{mongodb_gridfs}->{port} || 27017,
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
$config->{mongodb_gridfs}->{database} = $test_source_database_name;
$config->{amazon_s3}->{bucket_name} = $test_bucket_name;
ok( GridFSToS3::copy_gridfs_to_s3($config), "Copy from source GridFS to S3" );

# Copy files back from S3 to GridFS
$config->{mongodb_gridfs}->{database} = $test_destination_database_name;
ok( GridFSToS3::copy_s3_to_gridfs($config), "Copy from S3 to destination GridFS" );

# Compare files
my $response = $test_bucket->list_all;
my @files_restored_from_s3;
foreach my $key ( @{ $response->{keys} } ) {
	my $file = $test_bucket->get_key($key->{key});
	push (@files_restored_from_s3, {filename => $key->{key}, contents => $file->{value}});
}
# say STDERR "Expected: " . Dumper(@files);
# say STDERR "Got: " . Dumper(@files_restored_from_s3);
cmp_bag(\@files, \@files_restored_from_s3, 'List of files and their contents match');

# Delete temporary bucket and databases, remove "last filename" files
$response = $test_bucket->list_all;
foreach my $key ( @{ $response->{keys} } ) {
	$test_bucket->delete_key($key->{key});
}
$test_bucket->delete_bucket or die $native_s3->err . ": " . $native_s3->errstr;
$native_source_mongo_database->drop;
$native_destination_mongo_database->drop;
unlink $config->{file_with_last_filename_copied_from_gridfs_to_s3};
unlink $config->{file_with_last_filename_copied_from_s3_to_gridfs};
