use strict;
use warnings;

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n"});

use Storage::Handler::AmazonS3;
use Storage::Handler::GridFS;

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

1;
