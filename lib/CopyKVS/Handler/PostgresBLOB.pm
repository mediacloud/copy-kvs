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
has '_config_host'     => ( is => 'rw', isa => 'Str' );
has '_config_port'     => ( is => 'rw', isa => 'Int' );
has '_config_username' => ( is => 'rw', isa => 'Str' );
has '_config_password' => ( is => 'rw', isa => 'Str' );
has '_config_database' => ( is => 'rw', isa => 'Str' );
has '_config_schema'   => ( is => 'rw', isa => 'Str' );
has '_config_table'    => ( is => 'rw', isa => 'Str' );
has '_config_column'   => ( is => 'rw', isa => 'Str' );

# Database handler (DBIx::Simple instance)
has '_db' => ( is => 'rw' );

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
    $self->_config_table( $args->{ table } )   or LOGDIE( "Table name is not set." );
    $self->_config_column( $args->{ column } ) or LOGDIE( "Column name is not set." );

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

    my $dsn =
      sprintf( 'dbi:Pg:dbname=%s;host=%s;port=%d', $self->_config_database, $self->_config_host, $self->_config_port );

    say STDERR "Connecting to DSN '$dsn'...";
    my $db = DBIx::Simple->connect( $dsn, $self->_config_username, $self->_config_password );
    unless ( $db )
    {
        LOGDIE( "Unable to connect to database with DSN '$dsn'" );
    }

    # Check whether the table exists, has the required column, and the column is of type BYTEA
    my @table_column_exists = $db->query(
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
        $self->_config_schema, $self->_config_table, $self->_config_column
    )->flat;
    unless ( $table_column_exists[ 0 ] + 0 )
    {
        LOGDIE( "Table '$self->_config_table' in schema '$self->_config_schema' does not exist " .
              "or does not have column '$self->_config_column' of type BYTEA" );
    }

    $self->_db( $db );

    return $self->_db;
}

sub head($$)
{
    my ( $self, $filename ) = @_;

    my $db = $self->_db_handler_for_current_pid();

    LOGDIE( "Not implemented." );
}

sub delete($$)
{
    my ( $self, $filename ) = @_;

    my $db = $self->_db_handler_for_current_pid();

    LOGDIE( "Not implemented." );
}

sub put($$$)
{
    my ( $self, $filename, $contents ) = @_;

    my $db = $self->_db_handler_for_current_pid();

    LOGDIE( "Not implemented." );
}

sub get($$)
{
    my ( $self, $filename ) = @_;

    my $db = $self->_db_handler_for_current_pid();

    LOGDIE( "Not implemented." );
}

sub list_iterator($;$)
{
    my ( $self, $filename_offset ) = @_;

    my $db = $self->_db_handler_for_current_pid();

    LOGDIE( "Not implemented." );
}

no Moose;    # gets rid of scaffolding

1;
