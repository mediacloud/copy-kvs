use strict;
use warnings;

use YAML qw(LoadFile);


# Generate random alphanumeric string (password or token) of the specified length
sub random_string($)
{
    my ( $num_bytes ) = @_;
    return join '', map +( 0 .. 9, 'a' .. 'z', 'A' .. 'Z' )[ rand( 10 + 26 * 2 ) ], 1 .. $num_bytes;
}

# Get configuration from the environment variable
sub configuration_from_env()
{
	unless ($ENV{GRIDFS_TO_S3_CONFIG}) {
		die "Don't know where to take configuration parameters from; did you set the GRIDFS_TO_S3_CONFIG environment variable?\n";
	}

	my $config = LoadFile($ENV{GRIDFS_TO_S3_CONFIG}) or die "Unable to read configuration from '$ENV{GRIDFS_TO_S3_CONFIG}': $!";

	return $config;
}

1;
