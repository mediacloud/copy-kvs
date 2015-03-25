package CopyKVS::Handler;

#
# abstract class for storing / loading files
#

use strict;
use warnings;

use Moose::Role;
use CopyKVS::Iterator;

# Fetch file
# returns file contents on success; dies on error
requires 'get';

# Store file
# returns true on success; dies on error
requires 'put';

# Check if file exists
# returns true is file exists, false if file doesn't exist; dies on error
requires 'head';

# Deletes file
# returns true on success; dies on error
requires 'delete';

# Returns file list iterator of CopyKVS::Iterator type
# (from the beginning or from a specified filename offset);
# dies on error
requires 'list_iterator';

no Moose;    # gets rid of scaffolding

1;
