use 5.016003;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME          => 'CopyKVS',
    AUTHOR        => 'Linas Valiukas <lvaliukas@cyber.law.harvard.edu>',
    VERSION_FROM  => 'lib/CopyKVS.pm',
    ABSTRACT_FROM => 'lib/CopyKVS.pm',
    ( $ExtUtils::MakeMaker::VERSION >= 6.3002
        ? ( 'LICENSE' => 'agpl' )
        : () ),
    PREREQ_PM => {
        'App::githook_perltidy'           => '0.10.2',
        'DBD::Pg'                         => '3.5.1',
        'DBI'                             => '1.633',
        'DBIx::Simple'                    => '1.35',
        'File::Slurp'                     => 9999.19,
        'FindBin'                         => 0,
        'Log::Log4perl'                   => 1.43,
        'MongoDB'                         => '0.704.1.0',
        'Moose'                           => '2.1005',
        'Net::Amazon::S3'                 => 0.59,
        'Parallel::Fork::BossWorkerAsync' => 0.08,
        'YAML'                            => 0.90,

        # test
        'Test::Deep'          => 0,
        'Test::More'          => 0,
        'Test::Pod'           => 1.45,
        'Test::Pod::Coverage' => 1.08,
    },
);
