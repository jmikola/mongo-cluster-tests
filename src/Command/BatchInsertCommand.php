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
        $stopwatch->start('batch-insert');

        foreach ($generator as $i => $document) {
            $batch[] = $document;

            if (0 === ($i + 1) % $batchSize) {
                $this->doBatchInsert($collection, $batch, $stopwatch, $output);
                $batch = array();
            }
        }

        if (!empty($batch)) {
            $this->doBatchInsert($collection, $batch, $stopwatch, $output);
        }

        $event = $stopwatch->stop('batch-insert');
        $output->writeln(sprintf('Batch insertion of %d documents completed in %.3f seconds.', count($generator), $event->getDuration() / 1000));
    }

    private function doBatchInsert(\MongoCollection $collection, array $batch, Stopwatch $stopwatch, OutputInterface $output)
    {
        $collection->batchInsert($batch);
        $event = $stopwatch->lap('batch-insert');
        $periods = $event->getPeriods();
        $output->writeln(sprintf('Inserted %d documents into %s in %.3f seconds.', count($batch), $collection, end($periods)->getDuration() / 1000));
    }
}
