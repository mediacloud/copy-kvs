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

has '_cursor' => ( is => 'rw' );
has '_read_attempts' => ( is => 'rw' );


# Constructor
sub BUILD {
    my $self = shift;
    my $args = shift;

    $self->_cursor($args->{cursor});
    unless ($self->_cursor) {
        LOGDIE("MongoDB result cursor is undefined.");
    }
	$self->_cursor->immortal(1);

    $self->_read_attempts($args->{read_attempts}) or LOGDIE("Read attempts count is not defined.");
}

sub next($)
{
    my ($self) = @_;

    # MongoDB sometimes times out when reading because it's busy creating a new data file,
    # so we'll try to read several times
    my $attempt_to_read_succeeded = 0;
    my $object;
    for ( my $retry = 0 ; $retry < $self->_read_attempts ; ++$retry )
    {
        if ( $retry > 0 )
        {
            WARN("Retrying ($retry)...");
        }

        eval {

            # Read next
            $object = $self->_cursor->next;
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
        LOGDIE("Unable to read the next filename from GridFS after " . $self->_read_attempts . " retries.");
    }

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
