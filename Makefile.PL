use 5.016003;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME          => 'GridFSToS3',
    AUTHOR        => 'Linas Valiukas <lvaliukas@cyber.law.harvard.edu>',
    VERSION_FROM  => 'lib/GridFSToS3.pm',
    ABSTRACT_FROM => 'lib/GridFSToS3.pm',
    ( $ExtUtils::MakeMaker::VERSION >= 6.3002
        ? ( 'LICENSE' => 'agpl' )
        : () ),
    PREREQ_PM => {
        'App::githook_perltidy'           => '0.10.2',
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
