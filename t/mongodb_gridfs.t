
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

use Test::More tests => 21;
use Test::Deep;

BEGIN
{
    use FindBin;
    use lib "$FindBin::Bin/../lib";

    use CopyKVS::Handler::GridFS;
    use MongoDB;
}

# Connection configuration
my $config = configuration_from_env();

# Create temporary database for unit tests
my $mongodb_connector = $config->{ connectors }->{ "mongodb_gridfs_test" };
say STDERR "Creating temporary database '$mongodb_connector->{ database }'...";
my $native_mongo_client = MongoDB::MongoClient->new(
    host => $mongodb_connector->{ host },
    port => $mongodb_connector->{ port }
);

# Should auto-create on first write
my $native_mongo_database = $native_mongo_client->get_database( $mongodb_connector->{ database } );

my $gridfs = CopyKVS::Handler::GridFS->new(
    host     => $mongodb_connector->{ host },
    port     => $mongodb_connector->{ port },
    database => $mongodb_connector->{ database }
);

my $test_filename = 'xyz';
my $test_content;
my $returned_content;

# Store, fetch content
$test_content = 'Loren ipsum dolor sit amet.';
ok( $gridfs->put( $test_filename, $test_content ), "Storing filename '$test_filename' did not return true" );
ok( $gridfs->head( $test_filename ), "head() does not report that the file exists" );
ok( $returned_content = $gridfs->get( $test_filename ), "Getting filename '$test_filename' did not return contents" );
is( $test_content, $returned_content, "Content doesn't match" );
ok( $gridfs->delete( $test_filename ), "Deleting filename '$test_filename' did not return true" );
ok( !$gridfs->head( $test_filename ),  "head() reports that the file that was just removed still exists" );

# Store content twice
$test_content = 'Loren ipsum dolor sit amet.';
ok( $gridfs->put( $test_filename, $test_content ), "Storing filename '$test_filename' did not return true" );
ok( $gridfs->put( $test_filename, $test_content ), "Storing filename '$test_filename' the second time did not return true" );
ok( $gridfs->head( $test_filename ), "head() does not report that the file exists" );
ok( $returned_content = $gridfs->get( $test_filename ), "Getting filename '$test_filename' did not return contents" );
is( $test_content, $returned_content, "Content doesn't match" );
ok( $gridfs->delete( $test_filename ), "Deleting filename '$test_filename' did not return true" );
ok( !$gridfs->head( $test_filename ),  "head() reports that the file that was just removed still exists" );

# Store, fetch empty file
$test_content = '';
ok( $gridfs->put( $test_filename, $test_content ),
    "Storing filename '$test_filename' without contents did not return true" );
ok( $gridfs->head( $test_filename ), "head() does not report that the file exists" );
$returned_content = $gridfs->get( $test_filename );
ok( defined( $returned_content ), "Getting filename '$test_filename' did not return contents" );
is( $test_content, $returned_content, "Content doesn't match" );
ok( $gridfs->delete( $test_filename ), "Deleting filename '$test_filename' did not return true" );
ok( !$gridfs->head( $test_filename ),  "head() reports that the file that was just removed still exists" );

# Try fetching nonexistent filename
eval { $gridfs->get( 'does-not-exist' ); };
ok( $@,                               "Fetching file that does not exist should have failed" );
ok( !$gridfs->head( $test_filename ), "head() does not report that the nonexistent file exists" );

# Delete temporary database
$native_mongo_database->drop;
