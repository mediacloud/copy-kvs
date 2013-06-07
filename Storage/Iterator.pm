package Storage::Iterator;

#
# abstract class for iterating over files
#

use strict;
use warnings;

use Moose::Role;


# Fetch next filename
# returns next filename if there's something to fetch, undef if there isn't; dies on error
requires 'next';

no Moose;    # gets rid of scaffolding

1;
