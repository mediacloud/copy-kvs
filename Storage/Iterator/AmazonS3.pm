package Storage::Iterator::AmazonS3;

# class for iterating over a list of files in Amazon S3

use strict;
use warnings;

use Moose;
with 'Storage::Iterator';

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n"});

has '_bucket' => ( is => 'rw' );
has '_prefix' => ( is => 'rw' );
has '_offset' => ( is => 'rw' );
has '_read_attempts' => ( is => 'rw' );

has '_end_of_data' => ( is => 'rw' );
has '_filenames' => ( is => 'rw', default => sub { [] } );

# Constructor
sub BUILD {
    my $self = shift;
    my $args = shift;

    $self->_bucket($args->{bucket}) or LOGDIE("Bucket is undefined.");
    $self->_prefix($args->{prefix} || '');   # No prefix (folder)
    $self->_offset($args->{offset} || '');   # No offset (list from beginning)
    $self->_read_attempts($args->{read_attempts}) or LOGDIE("Read attempts count is not defined.");
}

sub _strip_prefix($$)
{
    my ($string, $prefix) = @_;

    $string =~ s/^$prefix//gm;
    return $string;
}

sub next($)
{
    my ($self) = @_;

    if (scalar (@{$self->_filenames}) == 0)
    {
        if ($self->_end_of_data) {
            # Last fetched chunk was the end of the list
            return undef;
        }

        # S3 sometimes times out when reading so we'll try to read several times
        my $attempt_to_read_succeeded = 0;
        my $list;
        for ( my $retry = 0 ; $retry < $self->_read_attempts ; ++$retry )
        {
            if ( $retry > 0 )
            {
                WARN("Retrying ($retry)...");
            }

            eval {

                # Fetch a new chunk
                $list = $self->_bucket->list({
                    prefix => $self->_prefix,
                    marker => $self->_prefix . $self->_offset
                }) or LOGDIE("Unable to fetch the next list of files.");

                $attempt_to_read_succeeded = 1;
            };

            if ( $@ )
            {
                WARN("Attempt to read next the filename didn't succeed because: $@");
            }
            else
            {
                last;
            }
        }

        unless ( $attempt_to_read_succeeded )
        {
            LOGDIE("Unable to read the next filename from S3 after " . $self->_read_attempts . " retries.");
        }

        # Write down the new offset
        $self->_offset(_strip_prefix($list->{next_marker}, $self->_prefix));
        unless ($list->{is_truncated}) {
            $self->_end_of_data(1);
        }

        for my $filename (@{$list->{keys}}) {
            $filename = _strip_prefix($filename->{key}, $self->_prefix) or LOGDIE("Empty filename.");
            push (@{$self->_filenames}, $filename);
        }
    }

    return shift (@{$self->_filenames});
}

no Moose;    # gets rid of scaffolding

1;
