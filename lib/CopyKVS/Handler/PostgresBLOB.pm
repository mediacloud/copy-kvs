package CopyKVS::Handler::PostgresBLOB;

# class for storing / loading files from / to PostgreSQL BLOBs

use strict;
use warnings;

use Moose;
with 'CopyKVS::Handler';

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init( { level => $DEBUG, utf8 => 1, layout => "%d{ISO8601} [%P]: %m%n" } );

use DBI;
use DBIx::Simple;
use DBD::Pg qw(:pg_types);

use CopyKVS::Iterator::PostgresBLOB;

# Configuration
has '_config_host'        => ( is => 'rw', isa => 'Str' );
has '_config_port'        => ( is => 'rw', isa => 'Int' );
has '_config_username'    => ( is => 'rw', isa => 'Str' );
has '_config_password'    => ( is => 'rw', isa => 'Str' );
has '_config_database'    => ( is => 'rw', isa => 'Str' );
has '_config_schema'      => ( is => 'rw', isa => 'Str' );
has '_config_table'       => ( is => 'rw', isa => 'Str' );
has '_config_id_column'   => ( is => 'rw', isa => 'Str' );
has '_config_data_column' => ( is => 'rw', isa => 'Str' );

# Database handler (DBIx::Simple instance)
has '_db' => ( is => 'rw' );

# Primary key column name
has '_primary_key_column' => ( is => 'rw' );

# Process PID (to prevent forks from attempting to clone database handler objects)
has '_pid' => ( is => 'rw' );

# Constructor
sub BUILD
{
    my $self = shift;
    my $args = shift;

    $self->_config_host( $args->{ host } ) or LOGDIE( "Host is not set." );
    $self->_config_port( $args->{ port } || 5432 );
    $self->_config_username( $args->{ username } ) or LOGDIE( "Username is not set." );
    $self->_config_password( $args->{ password } ) or LOGDIE( "Password is not set." );
    $self->_config_database( $args->{ database } ) or LOGDIE( "Database name is not set." );
    $self->_config_schema( $args->{ schema } || 'public' );
    $self->_config_table( $args->{ table } )             or LOGDIE( "Table name is not set." );
    $self->_config_id_column( $args->{ id_column } )     or LOGDIE( "ID column name is not set." );
    $self->_config_data_column( $args->{ data_column } ) or LOGDIE( "Data column name is not set." );

    $self->_pid( $$ );
}

sub _db_handler_for_current_pid($)
{
    my ( $self ) = @_;

    if ( $self->_pid == $$ and $self->_db )
    {
        # Already initialized on the very same process
        return $self->_db;
    }

    my $dsn = sprintf(
        'dbi:Pg:dbname=%s;host=%s;port=%d',    #
        $self->_config_database,               #
        $self->_config_host,                   #
        $self->_config_port
    );

    say STDERR "Connecting to DSN '$dsn'...";
    my $db = DBIx::Simple->connect( $dsn, $self->_config_username, $self->_config_password );
    unless ( $db )
    {
        LOGDIE( "Unable to connect to database with DSN '$dsn'" );
    }

    # Check whether the table exists, has the required ID column, and the
    # column is of the correct type
    my @id_column_exists = $db->query(
        <<EOF,
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = ?
              AND table_name = ?
              AND column_name = ?
              AND data_type IN ( 'integer', 'text' )
        )
EOF
        $self->_config_schema, $self->_config_table, $self->_config_id_column
    )->flat;
    unless ( $id_column_exists[ 0 ] + 0 )
    {
        LOGDIE( 'Table "' . $self->_config_table . '" in schema "' . $self->_config_schema .
              '" does not exist or does not have ID column "' . $self->_config_id_column . '" of correct type' );
    }

    # Check whether the table exists, has the required data column, and the
    # column is of type BYTEA
    my @data_column_exists = $db->query(
        <<EOF,
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = ?
              AND table_name = ?
              AND column_name = ?
              AND data_type = 'bytea'
        )
EOF
        $self->_config_schema, $self->_config_table, $self->_config_data_column
    )->flat;
    unless ( $data_column_exists[ 0 ] + 0 )
    {
        LOGDIE( 'Table "' . $self->_config_table . '" in schema "' . $self->_config_schema .
              '" does not exist or does not have ID column "' . $self->_config_data_column . '" of type BYTEA' );
    }

    # Get primary key column name (which will be used by the iterator for resuming)
    my $primary_key_columns = $db->query(
        <<EOF,
            SELECT
                pg_attribute.attname AS name,
                FORMAT_TYPE(pg_attribute.atttypid, pg_attribute.atttypmod) AS type
            FROM pg_index, pg_class, pg_attribute, pg_namespace 
            WHERE indrelid = pg_class.oid
              AND nspname = ?
              AND pg_class.oid = ?::regclass
              AND pg_class.relnamespace = pg_namespace.oid
              AND pg_attribute.attrelid = pg_class.oid
              AND pg_attribute.attnum = ANY(pg_index.indkey)
              AND indisprimary
EOF
        $self->_config_schema, $self->_config_table
    )->hashes;
    if ( scalar @{ $primary_key_columns } == 0 )
    {
        LOGDIE( 'Table "' .
              $self->_config_table . '" in schema "' . $self->_config_schema . '" does not have a primary column' );
    }
    if ( scalar @{ $primary_key_columns } > 1 )
    {
        LOGDIE( 'Table "' .
              $self->_config_table . '" in schema "' . $self->_config_schema . '" has more than one primary column' );
    }
    my $primary_key_column = $primary_key_columns->[ 0 ]->{ name };
    unless ( $primary_key_column )
    {
        LOGDIE( "Unable to determine primary key column name." );
    }
    $self->_primary_key_column( $primary_key_column );

    $self->_db( $db );

    return $self->_db;
}

sub head($$)
{
    my ( $self, $filename ) = @_;

    my $db = $self->_db_handler_for_current_pid();

    my $schema    = $self->_config_schema;
    my $table     = $self->_config_table;
    my $id_column = $self->_config_id_column;

    my $object_exists = $db->query(
        <<"EOF",
        SELECT 1
        FROM $schema.$table
        WHERE $id_column = ?
EOF
        $filename
    )->flat;

    if ( $object_exists->[ 0 ] )
    {
        return 1;
    }
    else
    {
        return 0;
    }
}

sub delete($$)
{
    my ( $self, $filename ) = @_;

    my $db = $self->_db_handler_for_current_pid();

    my $schema    = $self->_config_schema;
    my $table     = $self->_config_table;
    my $id_column = $self->_config_id_column;

    $db->query(
        <<"EOF",
        DELETE FROM $schema.$table
        WHERE $id_column = ?
EOF
        $filename
    );

    return 1;
}

sub put($$$)
{
    my ( $self, $filename, $contents ) = @_;

    my $db = $self->_db_handler_for_current_pid();

    my $schema      = $self->_config_schema;
    my $table       = $self->_config_table;
    my $id_column   = $self->_config_id_column;
    my $data_column = $self->_config_data_column;

    $db->begin_work;

    my $sth = $db->dbh->prepare(
        <<"EOF",
        UPDATE $schema.$table
        SET $data_column = ?
        WHERE $id_column = ?
EOF
    );
    $sth->bind_param( 1, $contents, { pg_type => DBD::Pg::PG_BYTEA } );
    $sth->bind_param( 2, $filename );
    $sth->execute();

    $sth = $db->dbh->prepare(
        <<"EOF",
        INSERT INTO $schema.$table
        ($id_column, $data_column)
            SELECT ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM $schema.$table
                WHERE $id_column = ?
            )
EOF
    );
    $sth->bind_param( 1, $filename );
    $sth->bind_param( 2, $contents, { pg_type => DBD::Pg::PG_BYTEA } );
    $sth->bind_param( 3, $filename );
    $sth->execute();

    $db->commit;

    return 1;
}

sub get($$)
{
    my ( $self, $filename ) = @_;

    my $db = $self->_db_handler_for_current_pid();

    my $schema      = $self->_config_schema;
    my $table       = $self->_config_table;
    my $id_column   = $self->_config_id_column;
    my $data_column = $self->_config_data_column;

    my $contents = $db->query(
        <<"EOF",
        SELECT $data_column
        FROM $schema.$table
        WHERE $id_column = ?
EOF
        $filename
    )->flat;

    unless ( $contents->[ 0 ] )
    {
        LOGDIE( "Object with ID '$filename' was not found" );
    }

    $contents = $contents->[ 0 ];

    return $contents;
}

sub list_iterator($;$)
{
    my ( $self, $filename_offset ) = @_;

    my $db = $self->_db_handler_for_current_pid();

    $filename_offset //= '';

    my $iterator;
    eval {
        $iterator = CopyKVS::Iterator::PostgresBLOB->new(
            db                 => $db,
            schema             => $self->_config_schema,
            table              => $self->_config_table,
            id_column          => $self->_config_id_column,
            primary_key_column => $self->_primary_key_column,
            offset             => $filename_offset,
        );
    };
    if ( $@ or ( !$iterator ) )
    {
        LOGDIE( 'Unable to create Amazon S3 iterator for filename offset "' . ( $filename_offset // 'undef' ) . "': $@" );
        return undef;
    }

    return $iterator;
}

no Moose;    # gets rid of scaffolding

1;
