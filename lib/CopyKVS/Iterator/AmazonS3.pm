package CopyKVS::Iterator::AmazonS3;

# class for iterating over a list of files in Amazon S3

use strict;
use warnings;

use Moose;
with 'CopyKVS::Iterator';

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init( { level => $DEBUG, utf8 => 1, layout => "%d{ISO8601} [%P]: %m%n" } );

# Use old ("legacy") interface because the new one (Net::Amazon::S3::Client::Bucket)
# doesn't seem to support manual markers
use Net::Amazon::S3 0.59;

has '_s3'            => ( is => 'rw' );
has '_bucket_name'   => ( is => 'rw' );
has '_prefix'        => ( is => 'rw' );
has '_offset'        => ( is => 'rw' );
has '_read_attempts' => ( is => 'rw' );

has '_end_of_data' => ( is => 'rw' );
has '_filenames' => ( is => 'rw', default => sub { [] } );

# Constructor
sub BUILD
{
    my $self = shift;
    my $args = shift;

    $self->_s3( $args->{ s3 } )                   or LOGDIE( "Net::Amazon::S3 object is undefined." );
    $self->_bucket_name( $args->{ bucket_name } ) or LOGDIE( "Bucket name is undefined." );
    $self->_prefix( $args->{ prefix } // '' );    # No prefix (directory)
    $self->_offset( $args->{ offset } // '' );    # No offset (list from beginning)
    $self->_read_attempts( $args->{ read_attempts } ) or LOGDIE( "Read attempts count is not defined." );
}

sub _strip_prefix($$)
{
    my ( $string, $prefix ) = @_;

    $string =~ s/^$prefix//gm;
    return $string;
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

        # S3 sometimes times out when reading so we'll try to read several times
        my $attempt_to_read_succeeded = 0;
        my $list;
        for ( my $retry = 0 ; $retry < $self->_read_attempts ; ++$retry )
        {
            if ( $retry > 0 )
            {
                WARN( "Retrying ($retry)..." );
            }

            eval {

                # Fetch a new chunk
                DEBUG( "Will fetch a new chunk of filenames with offset: " . $self->_offset );
                $list = $self->_s3->list_bucket(
                    {
                        bucket => $self->_bucket_name,
                        prefix => $self->_prefix,
                        marker => $self->_prefix . $self->_offset
                    }
                ) or LOGDIE( "Unable to fetch the next list of files." );

                $attempt_to_read_succeeded = 1;
            };

            if ( $@ )
            {
                WARN( "Attempt to read next the filename didn't succeed because: $@" );
            }
            else
            {
                last;
            }
        }

        unless ( $attempt_to_read_succeeded )
        {
            LOGDIE( "Unable to read the next filename from S3 after " . $self->_read_attempts . " retries." );
        }

        # Store the chunk of filenames locally
        for my $filename ( @{ $list->{ keys } } )
        {
            $filename = _strip_prefix( $filename->{ key }, $self->_prefix ) or LOGDIE( "Empty filename." );
            push( @{ $self->_filenames }, $filename );
        }

        # Write down the new offset
        $self->_offset( _strip_prefix( $list->{ next_marker }, $self->_prefix ) );
        if ( $list->{ is_truncated } )
        {
            # More files left to fetch -- use the last filename as a marker
            $self->_offset( $self->_filenames->[ -1 ] );
            DEBUG( "Updated to offset: " . $self->_offset );
        }
        else
        {
            # No more files (as the list is not truncated)
            $self->_end_of_data( 1 );
        }
    }

    return shift( @{ $self->_filenames } );
}

no Moose;    # gets rid of scaffolding

1;
