package Storage::Iterator::GridFS;

# class for iterating over a list of files in MongoDB GridFS

use strict;
use warnings;

use Moose;
with 'Storage::Iterator';

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({level => $DEBUG, utf8=>1, layout => "%d{ISO8601} [%P]: %m%n"});

# Number of filenames to cache into $self->_filenames
use constant GRIDFS_CHUNK_SIZE => 1000;

# for valid_objectid()
use Storage::Handler::GridFS;

has '_fs_files_collection' => ( is => 'rw' );
has '_offset' => ( is => 'rw' );
has '_read_attempts' => ( is => 'rw' );

has '_end_of_data' => ( is => 'rw' );
has '_filenames' => ( is => 'rw', default => sub { [] } );

# Constructor
sub BUILD {
    my $self = shift;
    my $args = shift;

    $self->_fs_files_collection($args->{fs_files_collection}) or LOGDIE("MongoDB fs.files collection is undefined.");
    $self->_offset($args->{offset} || '');   # No offset (list from beginning)
    $self->_read_attempts($args->{read_attempts}) or LOGDIE("Read attempts count is not defined.");
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

        # GridFS sometimes times out when reading (if query_timeout != -1) so we'll try to read several times
        my $attempt_to_read_succeeded = 0;
        my @objects;
        for ( my $retry = 0 ; $retry < $self->_read_attempts ; ++$retry )
        {
            if ( $retry > 0 )
            {
                WARN("Retrying ($retry)...");
            }

            eval {

                # Fetch a new chunk
                # (see README.mdown for the explanation of why we don't use MongoDB::Cursor as
                # an iterator itself and instead wrap the creation and usage into a single eval{};)
                my $find_query = { };
                if ($self->_offset ne '') {
                    # Start from the filename offset
                    my $offset_objectid       = $self->_fs_files_collection->find_one({ filename => $self->_offset }, {_id => 1});
                    unless ($offset_objectid) {
                        LOGDIE("Offset file '" . $self->_offset . "' was not found.");
                    }
                    $offset_objectid = $offset_objectid->{_id}->{value};
                    unless (Storage::Handler::GridFS::valid_objectid($offset_objectid)) {
                        LOGDIE("Offset file's '" . $self->_offset . "' ObjectId '$offset_objectid' is not valid.");
                    }

                    $find_query = { _id => { '$gt' => MongoDB::OID->new(value => $offset_objectid) } };
                    DEBUG("Will resume from ObjectId '$offset_objectid'");

                } else {
                    DEBUG("Will resume from the beginning");
                }

                # If the cursor would fail (die) here, it will be recreated in
                # the next attempt to fetch a list of files, so this will likely
                # overcome the "skip through gazillion of files" bug
                my $cursor = $self->_fs_files_collection
                                  ->query( $find_query )
                                  ->sort({ _id => 1})
                                  ->fields({ _id => 1, filename => 1 })
                                  ->limit(GRIDFS_CHUNK_SIZE);
                $cursor->immortal();
                @objects = $cursor->all;

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

        # Store the chunk of filenames locally
        for my $object (@objects) {

            my $object_objectid = $object->{_id}->{value};
            my $object_filename = $object->{filename};

            unless (Storage::Handler::GridFS::valid_objectid($object_objectid)) {
                LOGDIE("File's '$object_filename' ObjectId '$object_objectid' is not valid.");
            }

            push (@{$self->_filenames}, $object_filename);
        }

        # Write down the new offset
        if (scalar @objects) {
            # Use the last filename
            $self->_offset($self->_filenames->[-1]);
        } else {
            # No more objects to be fetched
            $self->_end_of_data(1);
        }
    }

    return shift (@{$self->_filenames});
}

no Moose;    # gets rid of scaffolding

1;
