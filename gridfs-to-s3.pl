#!/usr/bin/env perl

use strict;
use warnings;

use Storage::Handler::AmazonS3;
use Storage::Handler::GridFS;

use YAML qw(LoadFile);

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n"});

# Global variable so it can be used by _sigint() and _log_last_copied_file()
my $_config;

# Global variable so it can be used by _log_last_copied_file()
my $_last_copied_filename;

sub _log_last_copied_file
{
    open LAST, ">$_config->{file_with_last_backed_up_filename}";
    print LAST $_last_copied_filename;
    close LAST;
}

sub _sigint
{
    _log_last_copied_file();
    unlink $_config->{backup_lock_file};
    exit( 1 );
}

sub main
{
	unless ($ARGV[0])
	{
		LOGDIE("Usage: $0 config.yml");
	}

	$_config = LoadFile($ARGV[0]) or LOGDIE("Unable to read configuration from '$ARGV[0]': $!");

    # Create lock file
    if (-e $_config->{backup_lock_file}) {
        LOGDIE("Lock file '$_config->{backup_lock_file}' already exists.");
    }
    open LOCK, ">$_config->{backup_lock_file}";
    print LOCK "$$";
    close LOCK;

    # Catch SIGINTs to clean up the lock file and cleanly write the last copied file
    $SIG{ 'INT' } = '_sigint';

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

    # Read last copied filename
    my $offset_filename;
    if (-e $_config->{file_with_last_backed_up_filename}) {
        open LAST, "<$_config->{file_with_last_backed_up_filename}";
        $offset_filename = <LAST>;
        chomp $offset_filename;
        close LAST;

        INFO("Will resume from '$offset_filename'.");
    }

    # Copy
    my $list_iterator = $gridfs->list_iterator($offset_filename);
    while (my $filename = $list_iterator->next())
    {
        INFO("Copying '$filename'...");
        $amazons3->put($filename, $gridfs->get($filename));

        $_last_copied_filename = $filename;

    }

    # Remove lock file
    unlink $_config->{backup_lock_file};
}

main();
