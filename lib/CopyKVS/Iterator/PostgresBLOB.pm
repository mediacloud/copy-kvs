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

    if ( scalar( @{ $self->_filenames } ) == 0 )
    {
        if ( $self->_end_of_data )
        {
            # Last fetched chunk was the end of the list
            return undef;
        }

        my $schema             = $self->_config_schema;
        my $table              = $self->_config_table;
        my $id_column          = $self->_config_id_column;
        my $primary_key_column = $self->_config_primary_key_column;
        my $offset             = $self->_offset;

        # Fetch a new chunk
        my $objects;
        if ( $offset ne '' )
        {
            DEBUG( "Will resume from offset $offset" );
            $objects = $self->_db->query(
                <<"EOF",
                SELECT $id_column AS filename
                FROM $schema.$table
                WHERE $primary_key_column > (
                    SELECT $primary_key_column
                    FROM $schema.$table
                    WHERE $id_column = ?
                )
                ORDER BY $primary_key_column
                LIMIT ?
EOF
                $self->_offset, $POSTGRESBLOB_ITERATOR_CHUNK_SIZE
            )->hashes;
        }
        else
        {
            DEBUG( "Will resume from the beginning" );
            $objects = $self->_db->query(
                <<"EOF",
                SELECT $id_column AS filename
                FROM $schema.$table
                ORDER BY $primary_key_column
                LIMIT ?
EOF
                $POSTGRESBLOB_ITERATOR_CHUNK_SIZE
            )->hashes;
        }

        unless ( $objects )
        {
            LOGDIE( "Unable to fetch a chunk of objects for offset $offset" );
        }

        # Store the chunk of filenames locally
        for my $object ( @{ $objects } )
        {
            my $object_filename = $object->{ filename };
            push( @{ $self->_filenames }, $object_filename );
        }

        # Write down the new offset
        if ( scalar @{ $objects } )
        {
            # Use the last filename
            $self->_offset( $self->_filenames->[ -1 ] );
        }
        else
        {
            # No more objects to be fetched
            $self->_end_of_data( 1 );
        }
    }

    return shift( @{ $self->_filenames } );
}

no Moose;    # gets rid of scaffolding

1;
