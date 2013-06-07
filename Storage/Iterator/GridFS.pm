package Storage::Iterator::GridFS;

# class for iterating over a list of files in MongoDB GridFS

use strict;
use warnings;

use Moose;
with 'Storage::Iterator';

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n"});

# for valid_objectid()
use Storage::Handler::GridFS;

my $_cursor = undef;


# Constructor
sub BUILD {
    my $self = shift;
    my $args = shift;

    $_cursor = $args->{cursor};
    unless ($_cursor) {
        LOGDIE("MongoDB result cursor is undefined.");
    }
}

sub next($)
{
    my ($self) = @_;

    my $object = $_cursor->next;
    unless ($object) {
        # No more files
        return undef;
    }

    my $object_objectid = $object->{_id}->{value};
    my $object_filename = $object->{filename};

    unless (Storage::Handler::GridFS::valid_objectid($object_objectid)) {
        LOGDIE("File's '$object_filename' ObjectId '$object_objectid' is not valid.");
    }

    return $object_filename;
}

no Moose;    # gets rid of scaffolding

1;
