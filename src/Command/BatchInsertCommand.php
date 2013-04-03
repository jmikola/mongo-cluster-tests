<?php

namespace Command;

use Generator\DocumentGenerator;
use Symfony\Component\Console\Output\OutputInterface;

class BatchInsertCommand extends InsertCommand
{
    protected function configure()
    {
        parent::configure();

        $this
            ->setName('batch-insert')
            ->setDescription('Insert documents in batches')
        ;

        $help = <<<'EOF'
This commands extends <info>insert</info> by inserting documents in batches. The batch
size is calculated automatically based on the size of each generated document.
EOF;

        $this->setHelp($help . "\n\n" . $this->getHelp());
    }

    protected function doInsert(DocumentGenerator $generator, OutputInterface $output)
    {
        $bsonSize = $generator->getDocumentBsonSize();
        $batchSize = max(1, (int) (4194304 / $bsonSize));
        $batch = array();

        $this->stopwatch->start('batch-insert');

        foreach ($generator as $i => $document) {
            $batch[] = $document;

            if (0 === ($i + 1) % $batchSize) {
                $this->doBatchInsert($batch, $output);
                $batch = array();
            }
        }

        if (!empty($batch)) {
            $this->doBatchInsert($batch, $output);
        }

        $event = $this->stopwatch->stop('batch-insert');
        $output->writeln(sprintf('Batch insertion of %d documents completed in %.3f seconds.', count($generator), $event->getDuration() / 1000));
    }

    private function doBatchInsert(array $batch, OutputInterface $output)
    {
        $this->collection->batchInsert($batch);
        $event = $this->stopwatch->lap('batch-insert');
        $periods = $event->getPeriods();
        $output->writeln(sprintf('Inserted %d documents into %s in %.3f seconds.', count($batch), $this->collection, end($periods)->getDuration() / 1000));
    }
}
