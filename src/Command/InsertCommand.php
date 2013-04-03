<?php

namespace Command;

use Generator\DocumentGenerator;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class InsertCommand extends AbstractCommand
{
    protected function configure()
    {
        parent::configure();

        $this
            ->setName('insert')
            ->addOption('drop', null, InputOption::VALUE_NONE, 'Drop before inserting (removes indexes and sharding)')
            ->addOption('remove', null, InputOption::VALUE_NONE, 'Remove before inserting (preserves indexes)')
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
        if ($input->getOption('drop')) {
            $this->doDrop($output);
        }

        if ($input->getOption('remove')) {
            $this->doRemove($output);
        }

        $docs = (int) $input->getOption('docs');
        $size = (int) $input->getOption('size');
        $generator = new DocumentGenerator($docs, str_repeat('.', $size));

        $this->doInsert($generator, $output);
    }

    protected function doDrop(OutputInterface $output)
    {
        $this->stopwatch->start('drop');

        $this->collection->drop();

        $event = $this->stopwatch->stop('drop');
        $output->writeln(sprintf('Dropped %s in %.3f seconds.', $this->collection, $event->getDuration() / 1000));
    }

    protected function doRemove(OutputInterface $output)
    {
        $this->stopwatch->start('remove');

        $this->collection->remove();

        $event = $this->stopwatch->stop('remove');
        $output->writeln(sprintf('Removed all documents in %s in %.3f seconds.', $this->collection, $event->getDuration() / 1000));
    }

    protected function doInsert(DocumentGenerator $generator, OutputInterface $output)
    {
        $this->stopwatch->start('insert');

        foreach ($generator as $document) {
            $this->collection->insert($document);
        }

        $event = $this->stopwatch->stop('insert');
        $output->writeln(sprintf('Inserted %d documents into %s in %.3f seconds.', count($generator), $collection, $event->getDuration() / 1000));
    }
}
