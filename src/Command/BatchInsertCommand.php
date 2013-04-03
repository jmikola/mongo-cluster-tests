<?php

namespace Command;

use Generator\DocumentGenerator;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Stopwatch\Stopwatch;

class BatchInsertCommand extends InsertCommand
{
    protected function configure()
    {
        parent::configure();

        $this
            ->setName('batch-insert')
            ->setDescription('Insert documents in batches')
        ;
    }

    protected function doInsert(\MongoCollection $collection, DocumentGenerator $generator, OutputInterface $output)
    {
        $bsonSize = $generator->getDocumentBsonSize();
        $batchSize = max(1, (int) (4194304 / $bsonSize));
        $batch = array();

        $stopwatch = new Stopwatch();
        $stopwatch->start('insert');

        foreach ($generator as $i => $document) {
            $batch[] = $document;

            if (0 === $i % $batchSize) {
                $collection->batchInsert($batch);
                $batch = array();
            }
        }

        if (!empty($batch)) {
            $collection->batchInsert($batch);
        }

        $event = $stopwatch->stop('insert');
        $output->writeln(sprintf('Inserted %d documents into %s in %.3f seconds.', count($generator), $collection, $event->getDuration() / 1000));
    }
}
