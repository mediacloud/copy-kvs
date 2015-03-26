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

if ( postgres_test_configuration_is_set() )
{
    plan tests => 3;
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

my $postgres_connector = $config->{ connectors }->{ postgres_blob };
ok( $postgres_connector, "PostgreSQL test connector is set in the configuration" );

my $dsn = sprintf(
    'dbi:Pg:dbname=%s;host=%s;port=%d',    #
    $postgres_connector->{ database },     #
    $postgres_connector->{ host },         #
    $postgres_connector->{ port }          #
);

say STDERR "Connecting to DSN '$dsn'...";
my $db = DBIx::Simple->connect(
    $dsn,                                  #
    $postgres_connector->{ username },     #
    $postgres_connector->{ password }      #
);
ok( $db, "Connection to database succeeded" );

# Create test table
my $schema_name = $postgres_connector->{ schema };
my $table_name  = $postgres_connector->{ table };
my $id_column   = $postgres_connector->{ id_column };
my $data_column = $postgres_connector->{ data_column };

$db->begin_work;

$db->query(
    <<"EOF"
    CREATE TABLE $schema_name.$table_name (
    ${table_name}_id    SERIAL      PRIMARY KEY,
    $id_column          INTEGER     NOT NULL,
    $data_column        BYTEA       NOT NULL
)
EOF
);
$db->query(
    <<"EOF"
    CREATE UNIQUE INDEX ${table_name}_${id_column}
    ON ${schema_name}.${table_name} ($id_column);
EOF
);

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

# Drop test table
$db->query(
    <<"EOF"
    DROP TABLE $schema_name.$table_name
EOF
);

$db->commit;
