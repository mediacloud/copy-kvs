#!/bin/bash
#
# Wrapper around "copy-gridfs-to-s3.pl" script
#
# Writes script's output (both STDERR and STDOUT) to external log.
#
# If the script succeeds with a zero exit status, doesn't print nothing. If the
# script fails, prints out last 40 lines of the log (for Crontab email reports).
#

if [ $# -ne 1 ]; then
    echo "Usage: $0 path_to_log_file.log"
    echo "Also you might want to set the PERL_PATH environment variable."
    exit 1
fi

PWD="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$PWD/copy_or_tail_log.inc.sh"

LOG_FILE="$1"
COPY_SCRIPT="$PWD/../copy-gridfs-to-s3.pl"
CONFIG_FILE="$PWD/../gridfs-to-s3.yml"

copy_or_tail_log "$LOG_FILE" "$COPY_SCRIPT" "$CONFIG_FILE"

exit 0
