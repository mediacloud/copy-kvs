#!/usr/bin/env perl

use strict;
use warnings;

require 'gridfs-to-s3.inc.pl';

use YAML qw(LoadFile);

use Parallel::Fork::BossWorkerAsync;

# Global variable so it can be used by:
# * _sigint() -- called independently from main()
# * _log_last_copied_file_from_gridfs_to_s3() -- might be called by _sigint()
my $_config;

# Global variable so it can be used by:
# * _log_last_copied_file_from_gridfs_to_s3() -- might be called by _sigint()
my $_last_copied_filename;

# PID of the main process (only the "boss" process will have the
# right to write down the last copied filename)
my $_main_process_pid = 0;


sub _log_last_copied_file_from_gridfs_to_s3
{
    if ($$ == $_main_process_pid)
    {
        if (defined $_last_copied_filename) {
            open LAST, ">$_config->{file_with_last_backed_up_filename}";
            print LAST $_last_copied_filename;
            close LAST;
        }
    }
}

sub _sigint
{
    _log_last_copied_file_from_gridfs_to_s3();
    unlink $_config->{lock_file};
    exit( 1 );
}

sub main
{
	unless ($ARGV[0])
	{
		LOGDIE("Usage: $0 config.yml");
	}

    $_main_process_pid = $$;

	$_config = LoadFile($ARGV[0]) or LOGDIE("Unable to read configuration from '$ARGV[0]': $!");

    # Create lock file
    if (-e $_config->{lock_file}) {
        LOGDIE("Lock file '$_config->{lock_file}' already exists.");
    }
    open LOCK, ">$_config->{lock_file}";
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

    my $worker_timeout = $_config->{worker_timeout} or LOGDIE("Invalid worker timeout ('worker_timeout').");
    my $worker_threads = $_config->{worker_threads} or LOGDIE("Invalid number of worker threads ('worker_threads').");
    my $job_chunk_size = $_config->{job_chunk_size} or LOGDIE("Invalid number of jobs to enqueue at once ('job_chunk_size').");

    # Initialize worker manager
    my $bw = Parallel::Fork::BossWorkerAsync->new(
        work_handler    => \&upload_file_to_s3,
        global_timeout  => $worker_timeout,
        worker_count => $worker_threads,
    );

    # Copy
    my $gridfs = _gridfs_handler_for_pid($$, $_config);
    my $list_iterator = $gridfs->list_iterator($offset_filename);
    my $have_files_left = 1;
    while ($have_files_left)
    {
        my $filename;

        for (my $x = 0; $x < $job_chunk_size; ++$x)
        {
            my $f = $list_iterator->next();
            if ($f) {
                # Filename to copy
                $filename = $f;
            } else {
                # No filenames left to copy, leave $filename at the last filename copied
                # so that _last_copied_filename() can write that down
                $have_files_left = 0;
                last;
            }
            DEBUG("Enqueueing filename '$filename'");
            $bw->add_work({filename => $filename, config => $_config});
        }

        while ($bw->pending()) {
            my $ref = $bw->get_result();
            if ($ref->{ERROR}) {
                LOGDIE("Job error: $ref->{ERROR}");
            } else {
                DEBUG("Backed up file '$ref->{filename}'");
            }
        }

        # Store the last filename from the chunk as the last copied
        if ($filename) {
            $_last_copied_filename = $filename;
            _log_last_copied_file_from_gridfs_to_s3();
        }
    }

    $bw->shut_down();

    # Remove lock file
    unlink $_config->{lock_file};
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
