<?php

namespace Command;

use Generator\DocumentGenerator;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Stopwatch\Stopwatch;

class InsertCommand extends Command
{
    protected function configure()
    {
        $this
            ->setName('insert')
            ->addOption('server', 's', InputOption::VALUE_OPTIONAL, 'MongoDB server', 'mongodb://localhost:27017')
            ->addOption('db', 'd', InputOption::VALUE_OPTIONAL, 'MongoDB database', 'test')
            ->addOption('collection', 'c', InputOption::VALUE_OPTIONAL, 'MongoDB collection', 'test')
            ->addOption('w', null, InputOption::VALUE_OPTIONAL, 'Write concern', 1)
            ->addOption('wtimeout', null, InputOption::VALUE_OPTIONAL, 'Replication timeout (milliseconds)', 10000)
            ->addOption('timeout', null, InputOption::VALUE_OPTIONAL, 'Insert timeout (milliseconds)', 30000)
            ->addOption('drop', null, InputOption::VALUE_NONE, 'Drop before inserting')
            ->addOption('docs', null, InputOption::VALUE_OPTIONAL, 'Number of documents to insert', 10000)
            ->addOption('size', null, InputOption::VALUE_OPTIONAL, 'Constant field size (bytes)', 4096)
            ->setDescription('Insert documents')
            ->setHelp(<<<'EOF'
Documents will contain an "x" field with a random integer value and a "y" field
with a constant string of a configurable size.
EOF
            )
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $mongoClient = new \MongoClient($input->getOption('server'));
        $collection = $mongoClient->selectCollection($input->getOption('db'), $input->getOption('collection'));

        $docs = (int) $input->getOption('docs');
        $size = (int) $input->getOption('size');

        \MongoCursor::$timeout = (int) $input->getOption('timeout');
        $collection->w = is_numeric($w = $input->getOption('w')) ? (int) $w : $w;
        $collection->wtimeout = (int) $input->getOption('wtimeout');

        if ($input->getOption('drop')) {
            $this->doDrop($collection, $output);
        }

        $generator = new DocumentGenerator($docs, str_repeat('.', $size));

        $this->doInsert($collection, $generator, $output);
    }

    protected function doDrop(\MongoCollection $collection, OutputInterface $output)
    {
        $stopwatch = new Stopwatch();

        $stopwatch->start('drop');
        $collection->drop();
        $event = $stopwatch->stop('drop');
        $output->writeln(sprintf('Dropped %s in %.3f seconds.', $collection, $event->getDuration() / 1000));
    }

    protected function doInsert(\MongoCollection $collection, DocumentGenerator $generator, OutputInterface $output)
    {
        $stopwatch = new Stopwatch();
        $stopwatch->start('insert');

        foreach ($generator as $document) {
            $collection->insert($document);
        }

        $event = $stopwatch->stop('insert');
        $output->writeln(sprintf('Inserted %d documents into %s in %.3f seconds.', count($generator), $collection, $event->getDuration() / 1000));
    }
}
