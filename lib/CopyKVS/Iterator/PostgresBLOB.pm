package CopyKVS::Iterator::PostgresBLOB;

# class for iterating over a list of PostgreSQL BLOBs

use strict;
use warnings;

use Moose;
with 'CopyKVS::Iterator';

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init( { level => $DEBUG, utf8 => 1, layout => "%d{ISO8601} [%P]: %m%n" } );

use DBI;
use DBIx::Simple;
use Readonly;

has '_config_schema'             => ( is => 'rw', isa => 'Str' );
has '_config_table'              => ( is => 'rw', isa => 'Str' );
has '_config_id_column'          => ( is => 'rw', isa => 'Str' );
has '_config_primary_key_column' => ( is => 'rw', isa => 'Str' );

has '_db'     => ( is => 'rw' );    # DBIx::Simple object
has '_offset' => ( is => 'rw' );    # offset (set initially, updated later)

has '_end_of_data' => ( is => 'rw' );
has '_filenames' => ( is => 'rw', default => sub { [] } );

# Number of filenames to cache into $self->_filenames
Readonly my $POSTGRESBLOB_ITERATOR_CHUNK_SIZE => 1000;

# Constructor
sub BUILD
{
    my $self = shift;
    my $args = shift;

    $self->_config_schema( $args->{ schema } )                         or LOGDIE( 'Schema is not set.' );
    $self->_config_table( $args->{ table } )                           or LOGDIE( 'Table is not set.' );
    $self->_config_id_column( $args->{ id_column } )                   or LOGDIE( 'ID column is not set.' );
    $self->_config_primary_key_column( $args->{ primary_key_column } ) or LOGDIE( 'Primary key column is not set.' );

    $self->_db( $args->{ db } ) or LOGDIE( 'Database object is not set' );
    $self->_offset( $args->{ offset } // '' );    # No offset (list from beginning)
}

sub next($)
{
    my ( $self ) = @_;

    die "Not implemented.";
}

no Moose;                                         # gets rid of scaffolding

1;
