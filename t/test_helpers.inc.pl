
use strict;
use warnings;

use DBI;
use DBIx::Simple;

# Generate random alphanumeric string (password or token) of the specified length
sub random_string($)
{
    my ( $num_bytes ) = @_;
    return join '', map +( 0 .. 9, 'a' .. 'z', 'A' .. 'Z' )[ rand( 10 + 26 * 2 ) ], 1 .. $num_bytes;
}

sub postgres_test_configuration_is_set()
{
    return (  defined $ENV{ COPY_KVS_POSTGRES_HOST }
          and defined $ENV{ COPY_KVS_POSTGRES_USERNAME }
          and defined $ENV{ COPY_KVS_POSTGRES_PASSWORD }
          and defined $ENV{ COPY_KVS_POSTGRES_DATABASE } );
}

sub s3_test_configuration_is_set()
{
    return (  defined $ENV{ COPY_KVS_S3_ACCESS_KEY_ID }
          and defined $ENV{ COPY_KVS_S3_SECRET_ACCESS_KEY }
          and defined $ENV{ COPY_KVS_S3_BUCKET_NAME } );
}

# Get configuration from the environment variable
sub configuration_from_env()
{
    my $test_config = {
        lock_file      => "copy-kvs.lock",
        worker_threads => 32,
        job_chunk_size => 512,
        overwrite      => 1,
        connectors     => {
            "mongodb_gridfs_test" => {
                type             => "GridFS",
                host             => "localhost",
                port             => 27017,
                database         => "copy-kvs-test-" . random_string( 16 ),
                timeout          => -1,
                last_copied_file => "copy-kvs-gridfs-" . random_string( 16 ) . ".last",
            },
            "amazon_s3_test" => {
                type              => "AmazonS3",
                access_key_id     => $ENV{ COPY_KVS_S3_ACCESS_KEY_ID },
                secret_access_key => $ENV{ COPY_KVS_S3_SECRET_ACCESS_KEY },
                bucket_name       => $ENV{ COPY_KVS_S3_BUCKET_NAME },
                directory_name    => 'copy-kvs-test',
                timeout           => 60,
                use_ssl           => 0,
                head_before       => {
                    put    => 0,
                    get    => 0,
                    delete => 0,
                },
                last_copied_file => "copy-kvs-s3-" . random_string( 16 ) . ".last",
            },
            "postgres_blob_test" => {
                type             => "PostgresBLOB",
                host             => $ENV{ COPY_KVS_POSTGRES_HOST },
                port             => 5432,
                username         => $ENV{ COPY_KVS_POSTGRES_USERNAME },
                password         => $ENV{ COPY_KVS_POSTGRES_PASSWORD },
                database         => $ENV{ COPY_KVS_POSTGRES_DATABASE },
                schema           => "public",
                table            => "binary_blobs_test_" . random_string( 16 ),
                id_column        => "object_id",
                data_column      => "data",
                last_copied_file => "copy-kvs-postgres-" . random_string( 16 ) . ".last",
            }
        }
    };

    return $test_config;
}

sub initialize_test_postgresql_table($;$)
{
    my ( $postgres_connector, $skip_creating ) = @_;

    # Connect to database
    my $dsn = sprintf(
        'dbi:Pg:dbname=%s;host=%s;port=%d',    #
        $postgres_connector->{ database },     #
        $postgres_connector->{ host },         #
        $postgres_connector->{ port }          #
    );

    say STDERR "Connecting to DSN '$dsn'...";
    my $db = DBIx::Simple->connect(
        $dsn,                                  #
        $postgres_connector->{ username },     #
        $postgres_connector->{ password }      #
    );

    unless ( $skip_creating )
    {
        # Create test table
        my $schema_name = $postgres_connector->{ schema };
        my $table_name  = $postgres_connector->{ table };
        my $id_column   = $postgres_connector->{ id_column };
        my $data_column = $postgres_connector->{ data_column };

        $db->query(
            <<"EOF"
            CREATE TABLE $schema_name.$table_name (
            ${table_name}_id    SERIAL      PRIMARY KEY,
            $id_column          INTEGER     NOT NULL,
            $data_column        BYTEA       NOT NULL
        )
EOF
        );
        $db->query(
            <<"EOF"
            CREATE UNIQUE INDEX ${table_name}_${id_column}
            ON ${schema_name}.${table_name} ($id_column);
EOF
        );
    }

    return $db;
}

sub drop_test_postgresql_table($)
{
    my ( $postgres_connector ) = @_;

    my $skip_creating = 1;
    my $db = initialize_test_postgresql_table( $postgres_connector, $skip_creating );

    my $schema_name = $postgres_connector->{ schema };
    my $table_name  = $postgres_connector->{ table };

    # Drop test table
    $db->query(
        <<"EOF"
        DROP TABLE $schema_name.$table_name
EOF
    );
}

1;
