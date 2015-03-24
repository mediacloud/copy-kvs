#
# This function runs either the "copy GridFS to S3" or "copy S3 to GridFS"
# script with a defined configuration.
#
# If the script fails, the function tails the script's log and prints it out to
# STDOUT while also exiting with non-zero status.
#
# If the script succeeds, the function doesn't output anything.
#

function copy_or_tail_log {

    local LOG_FILE="$1"
    local COPY_SCRIPT="$2"
    local CONFIG_FILE="$3"
    local FROM_CONNECTOR="$4"
    local TO_CONNECTOR="$5"

    touch "$LOG_FILE" || {
        echo "Log file $LOG_FILE is not writable."
        exit 1
    }

    LOG_TIMESTAMP=`date "+%F-%T" | tr -s ' :' '_'`
    FAIL_LOG_SAMPLE_LINES=40

    if [ ! -f "$COPY_SCRIPT" ]; then
        echo "Copy script $COPY_SCRIPT does not exist."
        exit 1
    fi

    if [ ! -f "$CONFIG_FILE" ]; then
        echo "Configuration file $CONFIG_FILE does not exist."
        exit 1
    fi

    if [ -z "$PERL_PATH" ]; then
        PERL_PATH="/usr/bin/env perl"
    fi

    #
    # ---
    #

    PWD="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

    $PERL_PATH "$COPY_SCRIPT" "$CONFIG_FILE" "$FROM_CONNECTOR" "$TO_CONNECTOR" >> "$LOG_FILE" 2>&1 || {

        echo "'$COPY_SCRIPT' run on ${LOG_TIMESTAMP} has failed."
        echo
        echo "Path to full log file: ${LOG_FILE}"
        echo
        echo "Last ${FAIL_LOG_SAMPLE_LINES} lines of the log:"
        echo
        echo "---"
        tail -n $FAIL_LOG_SAMPLE_LINES "$LOG_FILE"
        echo "---"
        echo

        exit 1
    }

    # Copy script run has succeeded at this point, nothing to be send via Cron

    exit 0
}
