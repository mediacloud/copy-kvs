#!/usr/bin/env perl

use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/../lib";

use CopyKVS;

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init( { level => $DEBUG, utf8 => 1, layout => "%d{ISO8601} [%P]: %m%n" } );

use YAML qw(LoadFile);

sub main
{
    unless ( $ARGV[ 2 ] )
    {
        LOGDIE( "Usage: $0 config.yml from-connector to-connector" );
    }

    my $config_file    = $ARGV[ 0 ];
    my $from_connector = $ARGV[ 1 ];
    my $to_connector   = $ARGV[ 2 ];

    my $config;
    eval { $config = LoadFile( $config_file ); };
    if ( $@ )
    {
        LOGDIE( "Unable to read configuration from '$config_file': $@" );
    }

    CopyKVS::copy_kvs( $config, $from_connector, $to_connector );
}

main();
