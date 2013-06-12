#!/usr/bin/env perl

use strict;
use warnings;

use GridFSToS3;

use YAML qw(LoadFile);


sub main
{
	unless ($ARGV[0])
	{
		die("Usage: $0 config.yml");
	}

	my $config = LoadFile($ARGV[0]) or die("Unable to read configuration from '$ARGV[0]': $!\n");

    GridFSToS3::copy_s3_to_gridfs($config);
}

main();
