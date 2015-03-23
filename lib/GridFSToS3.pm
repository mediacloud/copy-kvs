package GridFSToS3;

use strict;
use warnings;

our $VERSION = '0.01';

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n"});

use Storage::Handler::AmazonS3;
use Storage::Handler::GridFS;

use Parallel::Fork::BossWorkerAsync;
use List::Util qw(max);

# Global variable so it can be used by:
# * _sigint() -- called independently from main()
# * _log_last_copied_file_from_gridfs_to_s3() -- might be called by _sigint()
# * _log_last_copied_file_from_s3_to_gridfs() -- might be called by _sigint()
my $_config;

# Global variable so it can be used by:
# * _log_last_copied_file_from_gridfs_to_s3() -- might be called by _sigint()
# * _log_last_copied_file_from_s3_to_gridfs() -- might be called by _sigint()
my $_last_copied_filename;

# PID of the main process (only the "boss" process will have the
# right to write down the last copied filename)
my $_main_process_pid = 0;

# GridFS handlers (PID => $handler)
my %_gridfs_handlers;

# S3 handlers (PID => $handler)
my %_s3_handlers;

if ($0 =~ /\.inc\.pl/) {
    die "Do not run this script directly.\n";
}


sub _gridfs_handler_for_pid($$)
{
    my ($pid, $config) = @_;

    unless (exists $_gridfs_handlers{$pid}) {
        $_gridfs_handlers{$pid} = Storage::Handler::GridFS->new(
            host => $config->{mongodb_gridfs}->{host} || 'localhost',
            port => $config->{mongodb_gridfs}->{port} || 27017,
            database => $config->{mongodb_gridfs}->{database},
            timeout => int($_config->{mongodb_gridfs}->{timeout}) || -1
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
            directory_name => $config->{amazon_s3}->{directory_name} || '',
            timeout => int($_config->{amazon_s3}->{timeout}) // 60,
            use_ssl => $_config->{amazon_s3}->{use_ssl} // 0,
            head_before_putting => $_config->{amazon_s3}->{head_before}->{put} // 0,
            head_before_getting => $_config->{amazon_s3}->{head_before}->{get} // 0,
            head_before_deleting => $_config->{amazon_s3}->{head_before}->{delete} // 0,
            overwrite => $_config->{amazon_s3}->{overwrite} // 1,
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

sub _create_lock_file($)
{
    my $config = shift;

    if (-e $config->{lock_file}) {
        LOGDIE("Lock file '$config->{lock_file}' already exists.");
    }
    open LOCK, ">$config->{lock_file}";
    print LOCK "$$";
    close LOCK;
}

sub _unlink_lock_file($)
{
    my $config = shift;

    unlink $config->{lock_file};
}

sub _log_last_copied_file_from_gridfs_to_s3
{
    if ($$ == $_main_process_pid)
    {
        if (defined $_last_copied_filename) {
            open LAST, ">$_config->{file_with_last_filename_copied_from_gridfs_to_s3}";
            print LAST $_last_copied_filename;
            close LAST;
        }
    }
}

sub _log_last_copied_file_from_s3_to_gridfs
{
    if ($$ == $_main_process_pid)
    {
        if (defined $_last_copied_filename) {
            open LAST, ">$_config->{file_with_last_filename_copied_from_s3_to_gridfs}";
            print LAST $_last_copied_filename;
            close LAST;
        }
    }
}

sub _sigint_from_gridfs_to_s3
{
    _log_last_copied_file_from_gridfs_to_s3();
    unlink $_config->{lock_file};
    exit( 1 );
}

sub _sigint_from_s3_to_gridfs
{
    _log_last_copied_file_from_s3_to_gridfs();
    unlink $_config->{lock_file};
    exit( 1 );
}

sub _upload_file_to_s3
{
    my ($job) = @_;

    my $filename = $job->{filename};
    my $config = $job->{config};

    eval {

        # Get storage handlers for current thread (PID)
        my $gridfs = _gridfs_handler_for_pid($$, $config);
        my $amazons3 = _s3_handler_for_pid($$, $config);

        INFO("Copying '$filename'...");
        $amazons3->put($filename, $gridfs->get($filename));

    };

    if ( $@ )
    {
        LOGDIE("Job error occurred while copying '$filename': $@");
    }

    return { filename => $filename };
}

sub _download_file_to_gridfs
{
    my ($job) = @_;

    my $filename = $job->{filename};
    my $config = $job->{config};

    eval {

        # Get storage handlers for current thread (PID)
        my $amazons3 = _s3_handler_for_pid($$, $config);
        my $gridfs = _gridfs_handler_for_pid($$, $config);

        INFO("Copying '$filename'...");
        $gridfs->put($filename, $amazons3->get($filename));

    };

    if ( $@ )
    {
        LOGDIE("Job error occurred while copying '$filename': $@");
    }    

    return { filename => $filename };
}

sub copy_gridfs_to_s3($)
{
	my ($config) = @_;
	$_config = $config;
	$_last_copied_filename = undef;

    $_main_process_pid = $$;

    # Create lock file
    _create_lock_file($_config);

    # Catch SIGINTs to clean up the lock file and cleanly write the last copied file
    $SIG{ 'INT' } = 'GridFSToS3::_sigint_from_gridfs_to_s3';

    # Read last copied filename
    my $offset_filename;
    if (-e $_config->{file_with_last_filename_copied_from_gridfs_to_s3}) {
        open LAST, "<$_config->{file_with_last_filename_copied_from_gridfs_to_s3}";
        $offset_filename = <LAST>;
        chomp $offset_filename;
        close LAST;

        INFO("Will resume from '$offset_filename'.");
    }

    my $worker_threads = $_config->{worker_threads} or LOGDIE("Invalid number of worker threads ('worker_threads').");
    my $job_chunk_size = $_config->{job_chunk_size} or LOGDIE("Invalid number of jobs to enqueue at once ('job_chunk_size').");
    my $gridfs_timeout = int($_config->{mongodb_gridfs}->{timeout}) or LOGDIE("Invalid GridFS timeout (must be positive integer or -1 for no timeout");
    my $s3_timeout = int($_config->{amazon_s3}->{timeout}) or LOGDIE("Invalid S3 timeout (must be positive integer");

    # Initialize worker manager
    my $bw = Parallel::Fork::BossWorkerAsync->new(
        work_handler    => \&_upload_file_to_s3,
        global_timeout  => ($gridfs_timeout == -1 ? 0 : max($gridfs_timeout, $s3_timeout) * 3),
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
    _unlink_lock_file($_config);

    INFO("Done.");

    return 1;
}

sub copy_s3_to_gridfs($)
{
	my ($config) = @_;
	$_config = $config;
	$_last_copied_filename = undef;

    $_main_process_pid = $$;

    # Create lock file
    _create_lock_file($_config);

    # Catch SIGINTs to clean up the lock file and cleanly write the last copied file
    $SIG{ 'INT' } = 'GridFSToS3::_sigint_from_s3_to_gridfs';

    # Read last copied filename
    my $offset_filename;
    if (-e $_config->{file_with_last_filename_copied_from_s3_to_gridfs}) {
        open LAST, "<$_config->{file_with_last_filename_copied_from_s3_to_gridfs}";
        $offset_filename = <LAST>;
        chomp $offset_filename;
        close LAST;

        INFO("Will resume from '$offset_filename'.");
    }

    my $worker_threads = $_config->{worker_threads} or LOGDIE("Invalid number of worker threads ('worker_threads').");
    my $job_chunk_size = $_config->{job_chunk_size} or LOGDIE("Invalid number of jobs to enqueue at once ('job_chunk_size').");
    my $gridfs_timeout = int($_config->{mongodb_gridfs}->{timeout}) or LOGDIE("Invalid GridFS timeout (must be positive integer or -1 for no timeout");
    my $s3_timeout = int($_config->{amazon_s3}->{timeout}) or LOGDIE("Invalid S3 timeout (must be positive integer");

    # Initialize worker manager
    my $bw = Parallel::Fork::BossWorkerAsync->new(
        work_handler    => \&_download_file_to_gridfs,
        global_timeout  => ($gridfs_timeout == -1 ? 0 : max($gridfs_timeout, $s3_timeout) * 3),
        worker_count => $worker_threads,
    );

    # Copy
    my $amazons3 = _s3_handler_for_pid($$, $_config);
    my $list_iterator = $amazons3->list_iterator($offset_filename);
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
                DEBUG("Restored file '$ref->{filename}'");
            }
        }

        # Store the last filename from the chunk as the last copied
        if ($filename) {
            $_last_copied_filename = $filename;
            _log_last_copied_file_from_s3_to_gridfs();
        }
    }

    $bw->shut_down();

    # Remove lock file
    _unlink_lock_file($_config);

    INFO("Done.");

    return 1;
}

1;

=head1 NAME

GridFSToS3 - Copy objects between various key-value stores (MongoDB GridFS, Amazon S3, PostgreSQL BLOB tables)

=head1 SYNOPSIS

  use GridFSToS3;

=head1 DESCRIPTION

Copy objects between various key-value stores (MongoDB GridFS, Amazon S3, PostgreSQL BLOB tables).

=head2 EXPORT

None by default.

=head1 AUTHOR

Linas Valiukas, E<lt>lvaliukas@cyber.law.harvard.eduE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013- Linas Valiukas, 2013- Berkman Center for Internet & Society.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.18.2 or,
at your option, any later version of Perl 5 you may have available.

=cut
