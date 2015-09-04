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

if ( s3_test_configuration_is_set() and postgres_test_configuration_is_set() )
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
    use CopyKVS::Handler::AmazonS3;
    use CopyKVS::Handler::PostgresBLOB;

    use Net::Amazon::S3;
    use DBI;
    use DBIx::Simple;
    use DBD::Pg qw(:pg_types);
}

# Connection configuration
my $config = configuration_from_env();

# Test *not* overwriting existing files
$config->{ overwrite } = 0;

# Randomize directory / database names so that multiple tests could run concurrently
$config->{ connectors }->{ "amazon_s3_test" }->{ directory_name } .= '-' . random_string( 16 );

# Create temporary bucket for unit tests
my $s3_connector       = $config->{ connectors }->{ "amazon_s3_test" };
my $postgres_connector = $config->{ connectors }->{ "postgres_blob_test" };

my $db = initialize_test_postgresql_table( $postgres_connector );
ok( $db, "Connection to database succeeded" );

# Create temporary databases for unit tests
my $s3_source = CopyKVS::Handler::AmazonS3->new(
    access_key_id        => $s3_connector->{ access_key_id },
    secret_access_key    => $s3_connector->{ secret_access_key },
    bucket_name          => $s3_connector->{ bucket_name },
    directory_name       => $s3_connector->{ directory_name } || '',
    timeout              => int( $s3_connector->{ timeout } ) // 60,
    use_ssl              => $s3_connector->{ use_ssl } // 0,
    head_before_putting  => $s3_connector->{ head_before }->{ put } // 0,
    head_before_getting  => $s3_connector->{ head_before }->{ get } // 0,
    head_before_deleting => $s3_connector->{ head_before }->{ delete } // 0,
);

my $postgres_destination = CopyKVS::Handler::PostgresBLOB->new(
    host => $postgres_connector->{ host } || 'localhost',
    port => $postgres_connector->{ port } || 5432,
    username    => $postgres_connector->{ username },
    password    => $postgres_connector->{ password },
    database    => $postgres_connector->{ database },
    schema      => $postgres_connector->{ schema } || 'public',
    table       => $postgres_connector->{ table },
    id_column   => $postgres_connector->{ id_column },
    data_column => $postgres_connector->{ data_column },
);

# Create two different sets of files (same filename, different contents)
my ( @files_set1_src, @files_set2_src );
for ( my $x = 1 ; $x <= $NUMBER_OF_TEST_FILES ; ++$x )
{
    my $filename = $x . '';
    push( @files_set1_src, { filename => $filename, contents => 'set 1: ' . random_string( 128 ) } );
    push( @files_set2_src, { filename => $filename, contents => 'set 2: ' . random_string( 128 ) } );
}

# Store files into the source S3 bucket
for my $file ( @files_set1_src )
{
    $s3_source->put( $file->{ filename }, $file->{ contents } );
}

# Copy files from source S3 database to destination PostgreSQL
ok( CopyKVS::copy_kvs( $config, "amazon_s3_test", "postgres_blob_test" ),
    "Copy from Amazon S3 to PostgreSQL (files set #1)" );

# Recreate test files with different data
for my $file ( @files_set2_src )
{
    $s3_source->put( $file->{ filename }, $file->{ contents } );
}

# Copy again
unlink $config->{ connectors }->{ "amazon_s3_test" }->{ last_copied_file };
ok( CopyKVS::copy_kvs( $config, "amazon_s3_test", "postgres_blob_test" ),
    "Copy from Amazon S3 to PostgreSQL (files set #2)" );

# Fetch the resulting objects from PostgreSQL before cleanup
# Delete temporary bucket and databases, remove "last filename" files
my @files_dst;
for ( my $x = 1 ; $x <= $NUMBER_OF_TEST_FILES ; ++$x )
{
    my $filename = $x . '';
    push( @files_dst, { filename => $filename, contents => $postgres_destination->get( $filename ) } );
    $s3_source->delete( $filename );
    $postgres_destination->delete( $filename );
}

# Cleanup
unlink $config->{ connectors }->{ "amazon_s3_test" }->{ last_copied_file };
unlink $config->{ connectors }->{ "postgres_blob_test" }->{ last_copied_file };
drop_test_postgresql_table( $postgres_connector );

# Compare files between source (first set of files) and destination PostgreSQL databases
cmp_bag( \@files_dst, \@files_set1_src,
    'List of files and their contents match; got: ' . Dumper( \@files_dst ) . '; expected: ' . Dumper( \@files_set1_src ) );
