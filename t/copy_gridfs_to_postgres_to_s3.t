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

if ( s3_test_configuration_is_set() or postgres_test_configuration_is_set() )
{
    plan tests => 4;
}
else
{
    plan skip_all => "S3 + PostgreSQL test configurations are not set in the environment.";
}

use Data::Dumper;
use Readonly;

Readonly my $NUMBER_OF_TEST_FILES => 10;

BEGIN
{
    use FindBin;
    use lib "$FindBin::Bin/../lib";

    use CopyKVS;
    use CopyKVS::Handler::GridFS;
    use CopyKVS::Handler::AmazonS3;
    use CopyKVS::Handler::PostgresBLOB;

    use MongoDB;
    use Net::Amazon::S3;
    use DBI;
    use DBIx::Simple;
    use DBD::Pg qw(:pg_types);
}

# Connection configuration
my $config = configuration_from_env();

# Randomize directory / database names so that multiple tests could run concurrently
$config->{ connectors }->{ "mongodb_gridfs_test" }->{ database }  .= '_src_' . random_string( 16 );
$config->{ connectors }->{ "amazon_s3_test" }->{ directory_name } .= '-' . random_string( 16 );

# Create temporary bucket for unit tests
my $mongodb_connector  = $config->{ connectors }->{ "mongodb_gridfs_test" };
my $postgres_connector = $config->{ connectors }->{ "postgres_blob_test" };
my $s3_connector       = $config->{ connectors }->{ "amazon_s3_test" };

my $native_s3 = Net::Amazon::S3->new(
    {
        aws_access_key_id     => $s3_connector->{ access_key_id },
        aws_secret_access_key => $s3_connector->{ secret_access_key },
        retry                 => 1,
    }
);
my $test_bucket = $native_s3->add_bucket( { bucket => $s3_connector->{ bucket_name } } )
  or die $native_s3->err . ": " . $native_s3->errstr;

# Create temporary databases for unit tests
my $native_mongo_client = MongoDB::MongoClient->new(
    host => $mongodb_connector->{ host },
    port => $mongodb_connector->{ port }
);

# Should auto-create on first write
my $native_source_mongo_database = $native_mongo_client->get_database( $mongodb_connector->{ database } );
my $gridfs_source                = CopyKVS::Handler::GridFS->new(
    host     => $mongodb_connector->{ host },
    port     => $mongodb_connector->{ port },
    database => $mongodb_connector->{ database }
);

my $db = initialize_test_postgresql_table( $postgres_connector );
ok( $db, "Connection to database succeeded" );

# Create test files
my @files_src;
for ( my $x = 1 ; $x <= $NUMBER_OF_TEST_FILES ; ++$x )
{
    push(
        @files_src,
        {
            filename => $x . '',
            contents => random_string( 128 )
        }
    );
}

# Store files into the source GridFS database
for my $file ( @files_src )
{
    $gridfs_source->put( $file->{ filename }, $file->{ contents } );
}

# Copy files from source GridFS database to intermediate PostgreSQL
ok( CopyKVS::copy_kvs( $config, "mongodb_gridfs_test", "postgres_blob_test" ), "Copy from GridFS to PostgreSQL" );

# Copy files from source GridFS database to S3
ok( CopyKVS::copy_kvs( $config, "postgres_blob_test", "amazon_s3_test" ), "Copy from PostgreSQL to S3" );

# Fetch the resulting objects from S3 before cleanup
# Delete temporary bucket and databases, remove "last filename" files
my @files_dst;
my $response = $test_bucket->list_all( { prefix => $s3_connector->{ directory_name } } );
foreach my $key ( @{ $response->{ keys } } )
{
    my $filename = $key->{ key };
    my $prefix = $s3_connector->{ directory_name } // '';
    $filename =~ s|^$prefix/?||gm;

    my $file = {
        filename => $filename,
        contents => $test_bucket->get_key( $key->{ key } )->{ value }
    };
    push( @files_dst, $file );

    $test_bucket->delete_key( $key->{ key } );
}

# Cleanup
unlink $config->{ connectors }->{ "mongodb_gridfs_test" }->{ last_copied_file };
unlink $config->{ connectors }->{ "postgres_blob_test" }->{ last_copied_file };
$native_source_mongo_database->drop;
drop_test_postgresql_table( $postgres_connector );

# Compare files between source and destination GridFS databases
cmp_bag( \@files_dst, \@files_src,
    'List of files and their contents match; got: ' . Dumper( \@files_dst ) . '; expected: ' . Dumper( \@files_src ) );
