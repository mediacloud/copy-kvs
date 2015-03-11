#!/usr/bin/env perl

use strict;
use warnings;

use GridFSToS3;

use YAML qw(LoadFile);


sub main
{
	unless ($ARGV[0])
	{
		die "Usage: $0 config.yml";
	}

	my $config;
	eval {
		$config = LoadFile($ARGV[0]);
	};
	if ( $@ ) {
		die "Unable to read configuration from '$ARGV[0]': $@";
	}

    GridFSToS3::copy_s3_to_gridfs($config);
}

main();
