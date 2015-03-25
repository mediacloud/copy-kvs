#!/usr/bin/env perl

use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/../lib";

use GridFSToS3;

use YAML qw(LoadFile);

sub main
{
    unless ( $ARGV[ 2 ] )
    {
        die "Usage: $0 config.yml from-connector to-connector";
    }

    my $config_file    = $ARGV[ 0 ];
    my $from_connector = $ARGV[ 1 ];
    my $to_connector   = $ARGV[ 2 ];

    my $config;
    eval { $config = LoadFile( $config_file ); };
    if ( $@ )
    {
        die "Unable to read configuration from '$config_file': $@";
    }

    GridFSToS3::copy_kvs( $config, $from_connector, $to_connector );
}

main();
