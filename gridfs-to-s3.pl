#!/usr/bin/env perl

use strict;
use warnings;

use Storage::Handler::AmazonS3;
use Storage::Handler::GridFS;

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n"});

use Data::Dumper;

use YAML qw(LoadFile);


sub main
{
	unless ($ARGV[0])
	{
		die "Usage: $0 config.yml\n";
	}

	# Load configuration
	my $config;
	eval {
		$config = LoadFile($ARGV[0]);
	};
	if ($@)
	{
		die "Unable to read configuration from '$ARGV[0]': $!";
	}

	# Initialize storage methods
	my $amazons3 = Storage::Handler::AmazonS3->new(
		access_key_id => $config->{amazon_s3}->{access_key_id},
		secret_access_key => $config->{amazon_s3}->{secret_access_key},
		bucket_name => $config->{amazon_s3}->{bucket_name},
		folder_name => $config->{amazon_s3}->{folder_name} || ''
	);
	my $gridfs = Storage::Handler::GridFS->new(
		host => $config->{mongodb_gridfs}->{host} || 'localhost',
		port => $config->{mongodb_gridfs}->{port} || 27017,
		database => $config->{mongodb_gridfs}->{database}
	);

    # Copy
    my $list_iterator = $gridfs->list_iterator();
    while (my $filename = $list_iterator->next())
    {
        say STDERR "Copying '$filename'...";
        $amazons3->put($filename, $gridfs->get($filename));
    }
}

main();
