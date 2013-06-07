#!/usr/bin/env perl

use strict;
use warnings;

use Storage::Handler::AmazonS3;
use Storage::Handler::GridFS;

use YAML qw(LoadFile);

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n"});

# Global variable so it can be used by:
# * _sigint() -- called independently from main()
# * _log_last_copied_file() -- might be called by _sigint()
my $_config;

# Global variable so it can be used by:
# * _log_last_copied_file() -- might be called by _sigint()
my $_last_copied_filename;

# GridFS handlers (PID => $handler)
my %_gridfs_handlers;

# S3 handlers (PID => $handler)
my %_s3_handlers;

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
    my $gridfs = _gridfs_handler_for_pid($$, $_config);
    my $list_iterator = $gridfs->list_iterator($offset_filename);
    while (my $filename = $list_iterator->next())
    {
        upload_file_to_s3({filename => $filename, config => $_config});

        $_last_copied_filename = $filename;

    }

    # Remove lock file
    unlink $_config->{backup_lock_file};
}

sub _gridfs_handler_for_pid($$)
{
    my ($pid, $config) = @_;

    unless (exists $_gridfs_handlers{$pid}) {
        $_gridfs_handlers{$pid} = Storage::Handler::GridFS->new(
            host => $config->{mongodb_gridfs}->{host} || 'localhost',
            port => $config->{mongodb_gridfs}->{port} || 27017,
            database => $config->{mongodb_gridfs}->{database}
        );
        unless ($_gridfs_handlers{$pid}) {
            LOGDIE("Unable to initialize GridFS handler for PID $pid");
        }
    }

    if (scalar keys %_gridfs_handlers > 100) {
        LOGDIE("Too many GridFS handlers initialized. Strange.");
    }

    return $_gridfs_handlers{$pid};
}

sub _s3_handler_for_pid($$)
{
    my ($pid, $config) = @_;

    unless (exists $_s3_handlers{$pid}) {
        $_s3_handlers{$pid} = Storage::Handler::AmazonS3->new(
            access_key_id => $config->{amazon_s3}->{access_key_id},
            secret_access_key => $config->{amazon_s3}->{secret_access_key},
            bucket_name => $config->{amazon_s3}->{bucket_name},
            folder_name => $config->{amazon_s3}->{folder_name} || ''
        );
        unless ($_s3_handlers{$pid}) {
            LOGDIE("Unable to initialize S3 handler for PID $pid");
        }
    }

    if (scalar keys %_s3_handlers > 100) {
        LOGDIE("Too many S3 handlers initialized. Strange.");
    }

    return $_s3_handlers{$pid};
}

sub upload_file_to_s3
{
    my ($job) = @_;

    my $filename = $job->{filename};
    my $config = $job->{config};

    # Get storage handlers for current thread (PID)
    my $gridfs = _gridfs_handler_for_pid($$, $config);
    my $amazons3 = _s3_handler_for_pid($$, $config);

    INFO("Copying '$filename'...");
    $amazons3->put($filename, $gridfs->get($filename));

    return { filename => $filename };
}

main();
