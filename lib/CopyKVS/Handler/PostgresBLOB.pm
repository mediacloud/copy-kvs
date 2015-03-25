package CopyKVS::Handler::PostgresBLOB;

# class for storing / loading files from / to PostgreSQL BLOBs

use strict;
use warnings;

use Moose;
with 'CopyKVS::Handler';

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init( { level => $DEBUG, utf8 => 1, layout => "%d{ISO8601} [%P]: %m%n" } );

use CopyKVS::Iterator::PostgresBLOB;

# Process PID (to prevent forks attempting to clone the Net::Amazon::S3 accessor objects)
has '_pid' => ( is => 'rw' );

# Constructor
sub BUILD
{
    my $self = shift;
    my $args = shift;

    die "Not implemented.";

    $self->_pid( $$ );
}

sub _initialize_s3_or_die($)
{
    my ( $self ) = @_;

    if ( $self->_pid == $$ and ( $self->_s3 and $self->_s3_bucket ) )
    {
        # Already initialized on the very same process
        return;
    }

    die "Not implemented.";
}

sub head($$)
{
    my ( $self, $filename ) = @_;

    die "Not implemented.";
}

sub delete($$)
{
    my ( $self, $filename ) = @_;

    die "Not implemented.";
}

sub put($$$)
{
    my ( $self, $filename, $contents ) = @_;

    die "Not implemented.";
}

sub get($$)
{
    my ( $self, $filename ) = @_;

    die "Not implemented.";
}

sub list_iterator($;$)
{
    my ( $self, $filename_offset ) = @_;

    die "Not implemented.";
}

no Moose;    # gets rid of scaffolding

1;
