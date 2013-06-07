#!/usr/bin/env perl

use strict;
use warnings;

use Storage::Handler::AmazonS3;
use Storage::Handler::GridFS;

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n"});

use Data::Dumper;

use YAML qw(LoadFile);

# Global variable so it can be used by sigint()
my $_config;

sub sigint
{
    unlink $_config->{backup_lock_file};
    exit( 1 );
}

sub main
{
	unless ($ARGV[0])
	{
		die "Usage: $0 config.yml\n";
	}

	$_config = LoadFile($ARGV[0]) or die "Unable to read configuration from '$ARGV[0]': $!";

    # Create lock file
    if (-e $_config->{backup_lock_file}) {
        die "Lock file '$_config->{backup_lock_file}' already exists.";
    }
    open LOCK, ">$_config->{backup_lock_file}";
    print LOCK "$$";
    close LOCK;

    # Catch SIGINTs to clean up the lock file
    $SIG{ 'INT' } = 'sigint';

	# Initialize storage methods
	my $amazons3 = Storage::Handler::AmazonS3->new(
		access_key_id => $_config->{amazon_s3}->{access_key_id},
		secret_access_key => $_config->{amazon_s3}->{secret_access_key},
		bucket_name => $_config->{amazon_s3}->{bucket_name},
		folder_name => $_config->{amazon_s3}->{folder_name} || ''
	);
	my $gridfs = Storage::Handler::GridFS->new(
		host => $_config->{mongodb_gridfs}->{host} || 'localhost',
		port => $_config->{mongodb_gridfs}->{port} || 27017,
		database => $_config->{mongodb_gridfs}->{database}
	);

    # Copy
    my $list_iterator = $gridfs->list_iterator('100');
    while (my $filename = $list_iterator->next())
    {
        say STDERR "Copying '$filename'...";
        $amazons3->put($filename, $gridfs->get($filename));
    }

    # Remove lock file
    unlink $_config->{backup_lock_file};
}

main();
