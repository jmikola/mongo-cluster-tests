<?php

namespace Command;

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
            ->setDefinition(array(
                new InputOption('server', 's', InputOption::VALUE_OPTIONAL, 'MongoDB server', 'mongodb://localhost:27017'),
                new InputOption('db', 'd', InputOption::VALUE_OPTIONAL, 'MongoDB database', 'test'),
                new InputOption('collection', 'c', InputOption::VALUE_OPTIONAL, 'MongoDB collection', 'test'),
                new InputOption('drop', null, InputOption::VALUE_NONE, 'Drop before inserting'),
                new InputOption('docs', null, InputOption::VALUE_OPTIONAL, 'Number of documents to insert', 10000),
                new InputOption('size', null, InputOption::VALUE_OPTIONAL, 'Constant field size (bytes)', 4096),
            ))
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

        $stopwatch = new Stopwatch();

        if ($input->getOption('drop')) {
            $stopwatch->start('drop');
            $collection->drop();
            $event = $stopwatch->stop('drop');
            $output->writeln(sprintf('Dropped %s in %.3f seconds.', $collection, $event->getDuration() / 1000));
        }

        $value = str_repeat('.', $size);

        $stopwatch->start('insert');

        for ($i = 0; $i < $docs; $i++) {
            $collection->insert(array(
                'x' => mt_rand(),
                'y' => $value,
            ));
        }

        $event = $stopwatch->stop('insert');
        $output->writeln(sprintf('Inserted %d documents into %s in %.3f seconds.', $docs, $collection, $event->getDuration() / 1000));
    }
}
