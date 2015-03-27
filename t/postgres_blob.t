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
use Readonly;

if ( postgres_test_configuration_is_set() )
{
    plan tests => 161;
}
else
{
    plan skip_all => "PostgreSQL test configuration is not set in the environment.";
}

BEGIN
{
    use FindBin;
    use lib "$FindBin::Bin/../lib";

    use DBI;
    use DBIx::Simple;
    use DBD::Pg qw(:pg_types);
    use CopyKVS::Handler::PostgresBLOB;
}

# Connection configuration
my $config = configuration_from_env();

my $postgres_connector = $config->{ connectors }->{ "postgres_blob_test" };
ok( $postgres_connector, "PostgreSQL test connector is set in the configuration" );

my $db = initialize_test_postgresql_table( $postgres_connector );
ok( $db, "Connection to database succeeded" );

# Initialize handler
my $postgres_handler = CopyKVS::Handler::PostgresBLOB->new(
    host        => $postgres_connector->{ host },
    port        => $postgres_connector->{ port },
    username    => $postgres_connector->{ username },
    password    => $postgres_connector->{ password },
    database    => $postgres_connector->{ database },
    schema      => $postgres_connector->{ schema },
    table       => $postgres_connector->{ table },
    id_column   => $postgres_connector->{ id_column },
    data_column => $postgres_connector->{ data_column },
);
ok( $postgres_handler, "PostgresBLOB handler initialized" );

sub test_store_fetch_object($$)
{
    my ( $postgres_handler, $test_content ) = @_;

    my $test_filename = '12345';
    my $returned_content;

    # Store, fetch content
    ok( $postgres_handler->put( $test_filename, $test_content ), "Storing filename '$test_filename' did not return true" );
    ok( $postgres_handler->head( $test_filename ), "head() does not report that the file exists" );
    $returned_content = $postgres_handler->get( $test_filename );
    ok( defined $returned_content, "Getting filename '$test_filename' did not return contents" );
    is( $test_content, $returned_content, "Content doesn't match" );
    ok( $postgres_handler->delete( $test_filename ), "Deleting filename '$test_filename' did not return true" );
    ok( !$postgres_handler->head( $test_filename ),  "head() reports that the file that was just removed still exists" );

    # Store content twice
    ok( $postgres_handler->put( $test_filename, $test_content ), "Storing filename '$test_filename' did not return true" );
    ok( $postgres_handler->put( $test_filename, $test_content ),
        "Storing filename '$test_filename' the second time did not return true" );
    ok( $postgres_handler->head( $test_filename ), "head() does not report that the file exists" );
    $returned_content = $postgres_handler->get( $test_filename );
    ok( defined $returned_content, "Getting filename '$test_filename' did not return contents" );
    is( $test_content, $returned_content, "Content doesn't match" );
    ok( $postgres_handler->delete( $test_filename ), "Deleting filename '$test_filename' did not return true" );
    ok( !$postgres_handler->head( $test_filename ),  "head() reports that the file that was just removed still exists" );
}

Readonly my @test_strings => (

    # ASCII
    "Media Cloud\r\nMedia Cloud\nMedia Cloud\r\n",

    # UTF-8
    "Media Cloud\r\nąčęėįšųūž\n您好\r\n",

    # Empty string
    "",

    # Invalid UTF-8 sequences
    "\xc3\x28",
    "\xa0\xa1",
    "\xe2\x28\xa1",
    "\xe2\x82\x28",
    "\xf0\x28\x8c\xbc",
    "\xf0\x90\x28\xbc",
    "\xf0\x28\x8c\x28",
    "\xf8\xa1\xa1\xa1\xa1",
    "\xfc\xa1\xa1\xa1\xa1\xa1",

);

foreach my $test_string ( @test_strings )
{
    test_store_fetch_object( $postgres_handler, $test_string );
}

# Try fetching nonexistent filename
eval { $postgres_handler->get( '99999' ); };
ok( $@,                                  "Fetching file that does not exist should have failed" );
ok( !$postgres_handler->head( '99999' ), "head() does not report that the nonexistent file exists" );

# Cleanup
drop_test_postgresql_table( $db, $postgres_connector );
