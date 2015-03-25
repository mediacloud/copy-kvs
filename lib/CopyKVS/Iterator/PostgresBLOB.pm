package CopyKVS::Iterator::PostgresBLOB;

# class for iterating over a list of PostgreSQL BLOBs

use strict;
use warnings;

use Moose;
with 'CopyKVS::Iterator';

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init( { level => $DEBUG, utf8 => 1, layout => "%d{ISO8601} [%P]: %m%n" } );

# Constructor
sub BUILD
{
    my $self = shift;
    my $args = shift;

    die "Not implemented.";
}

sub next($)
{
    my ( $self ) = @_;

    die "Not implemented.";
}

no Moose;    # gets rid of scaffolding

1;
