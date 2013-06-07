#!/usr/bin/env perl

use strict;
use warnings;

use Storage::Handler::AmazonS3;
use Storage::Handler::GridFS;

use Parallel::Fork::BossWorkerAsync;

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n"});

use Carp;

use Data::Dumper;

use YAML qw(LoadFile);

use constant WORKER_GLOBAL_TIMEOUT => 2;


# Job queue manager
{
	package JobQueue;

	use strict;
	use warnings;

	use constant CHUNK_SIZE => 10;
	use constant NUMBER_OF_JOBS => 100;

	my $filename = 1;

	sub new
	{
		my $this  = shift;
	    my $class = ref($this) || $this;
	    my $self  = {};
	    bless $self, $class;
	    return $self;
	}

	sub get_chunk_of_jobs
	{
		# Example list of jobs
		my $jobs = [];
		for (my $x = $filename; $x < $filename+CHUNK_SIZE; ++$x) {
			if ($x < NUMBER_OF_JOBS)
			{
				push(@{$jobs}, $x . '');
			}
		}

		$filename += CHUNK_SIZE;

		return $jobs;
	}

	sub reset
	{
		$filename = 1;
	}

	1;
}


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
		croak "Unable to read configuration from '$ARGV[0]': $!";
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

	# Initialize worker manager
	my $bw = Parallel::Fork::BossWorkerAsync->new(
		work_handler    => \&upload_file_to_s3,
		global_timeout  => WORKER_GLOBAL_TIMEOUT,
	);

	my $list = $gridfs->list(10, '1000');
	print Dumper($list);

	die("FIXME");

	my $queue = JobQueue->new();

	DEBUG("Fetching initial chunk");
	my $jobs = $queue->get_chunk_of_jobs();

	while (scalar @{$jobs} != 0)
	{
		# FIXME store last GridFS ObjectId (or filename?) here
		
		# Add a chunk of GridFS jobs to the queue
		for my $filename (@{$jobs}) {
			$bw->add_work( {filename => $filename} );
		}

		# Fetching a chunk of new jobs *after* the workers have something to do
		# so they can finish it in the background
		DEBUG("Fetching a new chunk");
		$jobs = $queue->get_chunk_of_jobs();

		# Wait for the whole chunk to complete
		while ($bw->pending()) {
			my $ref = $bw->get_result();
			if ($ref->{ERROR}) {
				LOGDIE("Job error: $ref->{ERROR}");
			} else {
				DEBUG("Backed up file '$ref->{filename}'");
			}
		}
	}

	$bw->shut_down();
}

sub upload_file_to_s3
{
	my ($job) = @_;
	my $filename = $job->{filename};

	INFO("Backing up file '$filename'...");

	return { filename => $filename };
}


main();
