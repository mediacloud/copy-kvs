package Storage::Iterator::AmazonS3;

# class for iterating over a list of files in Amazon S3

use strict;
use warnings;

use Moose;
with 'Storage::Iterator';
use Data::Dumper;

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n"});

my $_bucket = undef;
my $_prefix = undef;
my $_offset = undef;
my $_end_of_data = 0;

my @_filenames;

# Constructor
sub BUILD {
    my $self = shift;
    my $args = shift;

    $_bucket = $args->{bucket} or LOGDIE("Bucket is undefined.");
    $_prefix = $args->{prefix} || '';   # No prefix (folder)
    $_offset = $args->{offset} || '';   # No offset (list from beginning)
}

sub _strip_prefix($$)
{
    my ($string, $prefix) = @_;

    $string =~ s/^$prefix//gm;
    return $string;
}

sub next()
{
    if (scalar (@_filenames) == 0)
    {
        if ($_end_of_data) {
            # Last fetched chunk was the end of the list
            return undef;
        }

        # Fetch a new chunk
        my $list = $_bucket->list({prefix => $_prefix,
                                  marker => $_prefix . $_offset}) or LOGDIE("Unable to fetch the next list of files.");
        $_offset = _strip_prefix($list->{next_marker}, $_prefix);
        unless ($list->{is_truncated}) {
            $_end_of_data = 1;
        }

        for my $filename (@{$list->{keys}}) {
            $filename = _strip_prefix($filename->{key}, $_prefix) or LOGDIE("Empty filename.");
            push (@_filenames, $filename);
        }
    }

    return shift (@_filenames);
}

no Moose;    # gets rid of scaffolding

1;
