use strict;
use warnings;

require 't/test_helpers.inc.pl';

use Test::NoWarnings;
use Test::More tests => 3 + 2;
use Test::Deep;

BEGIN { use_ok('GridFSToS3'); }
BEGIN { use_ok('Storage::Handler::GridFS'); }

# Connection configuration
my $config = configuration_from_env();

# Rename backup / restore files to not touch the "production" ones
$config->{lock_file} .= random_string(32);
$config->{file_with_last_filename_copied_from_gridfs_to_s3} .= random_string(32);
$config->{file_with_last_filename_copied_from_s3_to_gridfs} .= random_string(32);

my $test_bucket_name = 'gridfs-to-s3.testing.' . random_string(32);
my $test_source_database_name = 'gridfs-to-s3_testing_source_' . random_string(16);

# Create the lock file, expect subroutines to fail with non-zero exit code
open(TEST_LOCK_FILE, '>' . $config->{lock_file});
print TEST_LOCK_FILE '1';
close (TEST_LOCK_FILE);

# Copy files from source GridFS database to S3
$config->{mongodb_gridfs}->{database} = $test_source_database_name;
$config->{amazon_s3}->{bucket_name} = $test_bucket_name;
eval {
	GridFSToS3::copy_gridfs_to_s3($config);
};
my $error_message = $@;
ok($error_message, "Copy from source GridFS to S3 while lock file is present" );
ok($error_message =~ /^Lock file/, 'Copy subroutine complains about lock file being present');

unlink ($config->{lock_file});
