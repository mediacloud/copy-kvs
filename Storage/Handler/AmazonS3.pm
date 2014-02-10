package Storage::Handler::AmazonS3;

# class for storing / loading files from / to Amazon S3

use strict;
use warnings;

use Moose;
with 'Storage::Handler';

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n"});

# Use old ("legacy") interface because the new one (Net::Amazon::S3::Client::Bucket)
# doesn't seem to support manual markers
use Net::Amazon::S3 0.59;

use POSIX qw(floor);

use Storage::Iterator::AmazonS3;

# S3's number of read / write attempts
# (in case waiting 20 seconds for the read / write to happen doesn't help, the instance should
# retry writing a couple of times)
use constant AMAZON_S3_READ_ATTEMPTS  => 3;
use constant AMAZON_S3_WRITE_ATTEMPTS => 3;


# Configuration
has '_config_access_key_id' => ( is => 'rw', isa => 'Str' );
has '_config_secret_access_key' => ( is => 'rw', isa => 'Str' );
has '_config_bucket_name' => ( is => 'rw', isa => 'Str' );
has '_config_folder_name' => ( is => 'rw', isa => 'Str' );
has '_config_timeout' => ( is => 'rw', isa => 'Int' );
has '_config_use_ssl' => ( is => 'rw', isa => 'Bool' );
has '_config_head_before_putting' => ( is => 'rw', isa => 'Bool' );
has '_config_head_before_getting' => ( is => 'rw', isa => 'Bool' );
has '_config_head_before_deleting' => ( is => 'rw', isa => 'Bool' );
has '_config_overwrite' => ( is => 'rw', isa => 'Bool' );

# Net::Amazon::S3 instance, bucket (lazy-initialized to prevent multiple forks using the same object)
has '_s3' => ( is => 'rw' );
has '_s3_bucket' => ( is => 'rw' );

# Process PID (to prevent forks attempting to clone the Net::Amazon::S3 accessor objects)
has '_pid' => ( is => 'rw' );


# Constructor
sub BUILD {
    my $self = shift;
    my $args = shift;

    if (AMAZON_S3_READ_ATTEMPTS < 1) {
        LOGDIE("AMAZON_S3_READ_ATTEMPTS must be >= 1");
    }
    if (AMAZON_S3_WRITE_ATTEMPTS < 1) {
        LOGDIE("AMAZON_S3_WRITE_ATTEMPTS must be >= 1");
    }

    $self->_config_access_key_id($args->{access_key_id}) or LOGDIE("Access key ID is not defined.");
    $self->_config_secret_access_key($args->{secret_access_key}) or LOGDIE("Secret access key is not defined.");
    $self->_config_bucket_name($args->{bucket_name}) or LOGDIE("Folder name is not defined.");
    $self->_config_folder_name($args->{folder_name} || '');
    $self->_config_timeout($args->{timeout} || 60);
    $self->_config_use_ssl($args->{use_ssl} // 0);
    $self->_config_head_before_putting($args->{head_before_putting} // 0);
    $self->_config_head_before_getting($args->{head_before_getting} // 0);
    $self->_config_head_before_deleting($args->{head_before_deleting} // 0);
    $self->_config_overwrite($args->{overwrite} // 1);

    # Add slash to the end of the folder name (if it doesn't exist yet)
    if ( $self->_config_folder_name and substr( $self->_config_folder_name, -1, 1 ) ne '/' )
    {
        $self->_config_folder_name($self->_config_folder_name . '/');
    }

    $self->_pid($$);
}

sub _initialize_s3_or_die($)
{
    my ( $self ) = @_;

    if ( $self->_pid == $$ and ( $self->_s3 and $self->_s3_bucket ) )
    {

        # Already initialized on the very same process
        return;
    }

    # Timeout should "fit in" at least AMAZON_S3_READ_ATTEMPTS number of retries
    # within the time period
    my $request_timeout = floor(($self->_config_timeout / AMAZON_S3_READ_ATTEMPTS) - 1);
    if ($request_timeout < 10) {
        LOGDIE("Amazon S3 request timeout ($request_timeout s) is too small.");
    }

    eval {

        # Initialize
        $self->_s3(Net::Amazon::S3->new(
            aws_access_key_id     => $self->_config_access_key_id,
            aws_secret_access_key => $self->_config_secret_access_key,
            retry                 => 1,
            secure                => $self->_config_use_ssl,
            timeout               => $request_timeout
        ));
        unless ( $self->_s3 )
        {
            LOGDIE("Unable to initialize Net::Amazon::S3 instance with access key '" . $self->_config_access_key_id . "'.");
        }

        # Get the bucket ($_s3->bucket would not verify that the bucket exists)
        my $response = $self->_s3->buckets;
        foreach my $bucket ( @{ $response->{buckets} } )
        {
            if ( $bucket->bucket eq $self->_config_bucket_name )
            {
                $self->_s3_bucket($bucket);
            }
        }
        unless ( $self->_s3_bucket )
        {
            LOGDIE("Unable to get bucket '" . $self->_config_bucket_name . "'.");
        } else {
            # DEBUG("Bucket was found: " . $self->_s3_bucket);
        }
    };
    if ($@) {
        LOGDIE("Unable to initialize S3 storage handler because: $@");
    }

    # Save PID
    $self->_pid($$);

    my $path = ( $self->_config_folder_name ? $self->_config_bucket_name . '/' . $self->_config_folder_name : $self->_config_bucket_name );
    INFO("Initialized Amazon S3 storage at '$path' with request timeout = $request_timeout s, read attempts = " . AMAZON_S3_READ_ATTEMPTS . ", write attempts = " . AMAZON_S3_WRITE_ATTEMPTS);
}

sub _path_for_filename($$)
{
    my ($self, $filename) = @_;

    unless ($filename) {
        LOGDIE("Filename is empty.");
    }

    if ($self->_config_folder_name ne '' and $self->_config_folder_name ne '/') {
        return $self->_config_folder_name . $filename;
    } else {
        return $filename;
    }
}

sub head($$)
{
    my ( $self, $filename ) = @_;

    $self->_initialize_s3_or_die();

    # S3 sometimes times out when reading, so we'll try to read several times
    my $attempt_to_head_succeeded = 0;
    my $file                      = undef;
    for ( my $retry = 0 ; $retry < AMAZON_S3_READ_ATTEMPTS ; ++$retry )
    {
        if ( $retry > 0 )
        {
            WARN("Retrying ($retry)...");
        }

        eval {

            # HEAD
            $file = $self->_s3_bucket->head_key($self->_path_for_filename($filename));

            $attempt_to_head_succeeded = 1;
        };

        if ( $@ )
        {
            WARN("Attempt to check if file '$filename' exists on S3 didn't succeed because: $@");
        }
        else
        {
            last;
        }
    }

    unless ( $attempt_to_head_succeeded )
    {
        LOGDIE("Unable to HEAD '$filename' on S3 after " . AMAZON_S3_READ_ATTEMPTS . " retries.");
    }

    if ($file) {
        return 1;
    } else {
        return 0;
    }
}

sub delete($$)
{
    my ( $self, $filename ) = @_;

    $self->_initialize_s3_or_die();

    # S3 sometimes times out when deleting, so we'll try to delete several times
    my $attempt_to_delete_succeeded = 0;
    for ( my $retry = 0 ; $retry < AMAZON_S3_WRITE_ATTEMPTS ; ++$retry )
    {
        if ( $retry > 0 )
        {
            WARN("Retrying ($retry)...");
        }

        eval {

            # Delete
            if ($self->_config_head_before_deleting)
            {
                unless ( $self->head( $filename ) )
                {
                    LOGDIE("File '$filename' does not exist.");
                }
            }

            unless ($self->_s3_bucket->delete_key($self->_path_for_filename($filename))) {
                my $s3_errno = $self->_s3_bucket->err // 0;
                my $s3_errstr = $self->_s3_bucket->errstr // 'undefined';
                LOGDIE("Failed to DELETE file, S3 error: $s3_errno: $s3_errstr");
            }

            $attempt_to_delete_succeeded = 1;
        };

        if ( $@ )
        {
            WARN("Attempt to delete file '$filename' from S3 didn't succeed because: $@");
        }
        else
        {
            last;
        }
    }

    unless ( $attempt_to_delete_succeeded )
    {
        LOGDIE("Unable to delete '$filename' from S3 after " . AMAZON_S3_WRITE_ATTEMPTS . " retries.");
    }

    return 1;
}

sub put($$$)
{
    my ( $self, $filename, $contents ) = @_;

    $self->_initialize_s3_or_die();

    my $write_was_successful = 0;

    # S3 sometimes times out when writing, so we'll try to write several times
    for ( my $retry = 0 ; $retry < AMAZON_S3_WRITE_ATTEMPTS ; ++$retry )
    {
        if ( $retry > 0 )
        {
            WARN("Retrying ($retry)...");
        }

        eval {

            # HEAD (if needed)
            my $skip = 0;
            if ( $self->_config_head_before_putting or (! $self->_config_overwrite) )
            {
                if ( $self->head( $filename ) )
                {
                    if ($self->_config_overwrite) {
                        WARN("File '$filename' already exists, " .
                          "will store a new version or overwrite ".
                          "(depending on whether or not versioning is enabled).");
                    } else {
                        INFO("File '$filename' already exists, will skip it.");
                        $skip = 1;
                    }
                }
            }

            # PUT
            unless ($skip) {
                unless ($self->_s3_bucket->add_key($self->_path_for_filename($filename), $contents)) {

                    my $s3_errno = $self->_s3_bucket->err // 0;
                    my $s3_errstr = $self->_s3_bucket->errstr // 'undefined';
                    LOGDIE("Failed to PUT file, S3 error: $s3_errno: $s3_errstr");
                }
            }
            
            $write_was_successful = 1;

        };

        if ( $@ )
        {
            WARN("Attempt to write '$filename' to S3 didn't succeed because: $@");
        }
        else
        {
            last;
        }
    }

    unless ( $write_was_successful )
    {
        LOGDIE("Unable to write '$filename' to Amazon S3 after " . AMAZON_S3_WRITE_ATTEMPTS . " retries.");
    }

    return 1;
}

sub get($$)
{
    my ( $self, $filename ) = @_;

    $self->_initialize_s3_or_die();

    my $value = undef;

    # S3 sometimes times out when reading, so we'll try to read several times
    for ( my $retry = 0 ; $retry < AMAZON_S3_READ_ATTEMPTS ; ++$retry )
    {
        if ( $retry > 0 )
        {
            WARN("Retrying ($retry)...");
        }

        eval {

            # HEAD (if needed)
            if ( $self->_config_head_before_getting )
            {
                unless ( $self->head( $filename ) )
                {
                    LOGDIE("File '$filename' does not exist.");
                }
            }

            # GET
            my $contents = $self->_s3_bucket->get_key($self->_path_for_filename($filename));
            unless (defined($contents)) {
                my $s3_errno = $self->_s3_bucket->err // 0;
                my $s3_errstr = $self->_s3_bucket->errstr // 'undefined';
                LOGDIE("Failed to GET file (content is undefined), S3 error: $s3_errno: $s3_errstr");
            }

            $value = $contents->{value};

        };

        if ( $@ )
        {
            WARN("Attempt to read '$filename' from S3 didn't succeed because: $@");
        }
        else
        {
            last;
        }
    }

    unless ( defined $value )
    {
        LOGDIE("Unable to read '$filename' from Amazon S3 after " . AMAZON_S3_READ_ATTEMPTS . " retries.");
    }

    return $value;
}

sub list_iterator($;$)
{
    my ( $self, $filename_offset ) = @_;

    $self->_initialize_s3_or_die();

    $filename_offset //= '';

    my $iterator;
    eval {
        $iterator = Storage::Iterator::AmazonS3->new(s3 => $self->_s3,
                                                        bucket_name => $self->_config_bucket_name,
                                                        prefix => $self->_config_folder_name,
                                                        offset => $filename_offset,
                                                        read_attempts => AMAZON_S3_READ_ATTEMPTS);
    };
    if ($@ or (! $iterator)) {
        LOGDIE("Unable to create Amazon S3 iterator for filename offset '" . ($filename_offset // 'undef' ) . "'");
        return undef;
    }

    return $iterator;
}

no Moose;    # gets rid of scaffolding

1;
