package Storage::Handler::AmazonS3;

# class for storing / loading files from / to Amazon S3

use strict;
use warnings;

use Moose;
with 'Storage::Handler';

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n"});

# Use old ("legacy") interface because the new one (Net::Amazon::S3::Client::Bucket) doesn't seem to support manual markers
use Net::Amazon::S3;

use Storage::Iterator::AmazonS3;

# Should the Amazon S3 module use secure (SSL-encrypted) connections?
use constant AMAZON_S3_USE_SSL => 0;

# How many seconds should the module wait before bailing on a request to S3 (in seconds)
use constant AMAZON_S3_TIMEOUT => 10;

# Check if content exists before PUTting (good for debugging, slows down the stores)
use constant AMAZON_S3_CHECK_IF_EXISTS_BEFORE_PUTTING => 1;

# Check if content exists before GETting (good for debugging, slows down the fetches)
use constant AMAZON_S3_CHECK_IF_EXISTS_BEFORE_GETTING => 1;

# Check if content exists before DELETing (good for debugging, slows down the fetches)
use constant AMAZON_S3_CHECK_IF_EXISTS_BEFORE_DELETING => 1;


# Configuration
has '_config_access_key_id' => ( is => 'rw' );
has '_config_secret_access_key' => ( is => 'rw' );
has '_config_bucket_name' => ( is => 'rw' );
has '_config_folder_name' => ( is => 'rw' );

# Net::Amazon::S3 instance, bucket (lazy-initialized to prevent multiple forks using the same object)
has '_s3' => ( is => 'rw' );
has '_s3_bucket' => ( is => 'rw' );

# Process PID (to prevent forks attempting to clone the Net::Amazon::S3 accessor objects)
has '_pid' => ( is => 'rw' );


# Constructor
sub BUILD {
    my $self = shift;
    my $args = shift;

    $self->_config_access_key_id($args->{access_key_id}) or LOGDIE("Access key ID is not defined.");
    $self->_config_secret_access_key($args->{secret_access_key}) or LOGDIE("Secret access key is not defined.");
    $self->_config_bucket_name($args->{bucket_name}) or LOGDIE("Folder name is not defined.");
    $self->_config_folder_name($args->{folder_name} || '');

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

    # Initialize
    $self->_s3(Net::Amazon::S3->new(
        aws_access_key_id     => $self->_config_access_key_id,
        aws_secret_access_key => $self->_config_secret_access_key,
        retry                 => 1,
        secure                => AMAZON_S3_USE_SSL,
        timeout               => AMAZON_S3_TIMEOUT
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
    }

    # Save PID
    $self->_pid($$);

    my $path = ( $self->_config_folder_name ? $self->_config_bucket_name . '/' . $self->_config_folder_name : $self->_config_bucket_name );
    INFO("Initialized Amazon S3 download storage at '$path'.");
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

    if ($self->_s3_bucket->head_key($self->_path_for_filename($filename))) {
        return 1;
    } else {
        return 0;
    }
}

sub delete($$)
{
    my ( $self, $filename ) = @_;

    $self->_initialize_s3_or_die();

    if (AMAZON_S3_CHECK_IF_EXISTS_BEFORE_DELETING)
    {
        unless ( $self->head( $filename ) )
        {
            LOGDIE("File '$filename' does not exist.");
        }
    }

    $self->_s3_bucket->delete_key($self->_path_for_filename($filename)) or LOGDIE($self->_s3_bucket->err . ": " . $self->_s3_bucket->errstr);

    return 1;
}

sub put($$$)
{
    my ( $self, $filename, $contents ) = @_;

    $self->_initialize_s3_or_die();

    if ( AMAZON_S3_CHECK_IF_EXISTS_BEFORE_PUTTING )
    {
        if ( $self->head( $filename ) )
        {
            WARN("File '$filename' already exists, " .
              "will store a new version or overwrite ".
              "(depending on whether or not versioning is enabled).");
        }
    }

    $self->_s3_bucket->add_key($self->_path_for_filename($filename), $contents) or LOGDIE($self->_s3_bucket->err . ": " . $self->_s3_bucket->errstr);

    return 1;
}

sub get($$)
{
    my ( $self, $filename ) = @_;

    $self->_initialize_s3_or_die();

    if ( AMAZON_S3_CHECK_IF_EXISTS_BEFORE_GETTING )
    {
        unless ( $self->head( $filename ) )
        {
            LOGDIE("File '$filename' does not exist.");
        }
    }

    my $contents = $self->_s3_bucket->get_key($self->_path_for_filename($filename));
    unless (defined($contents)) {
        LOGDIE($self->_s3_bucket->err . ": " . $self->_s3_bucket->errstr);
    }

    return $contents->{value};
}

sub list_iterator($;$)
{
    my ( $self, $filename_offset ) = @_;

    $self->_initialize_s3_or_die();

    my $iterator = Storage::Iterator::AmazonS3->new(bucket => $self->_s3_bucket,
                                                    prefix => $self->_config_folder_name,
                                                    offset => $filename_offset);
    return $iterator;
}

no Moose;    # gets rid of scaffolding

1;
