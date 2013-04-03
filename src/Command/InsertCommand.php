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
            ->setDescription('Insert documents')
            ->addOption('drop', null, InputOption::VALUE_NONE, 'Drop before inserting')
            ->addOption('remove', null, InputOption::VALUE_NONE, 'Remove before inserting')
            ->addOption('docs', null, InputOption::VALUE_OPTIONAL, 'Number of documents to insert', 10000)
            ->addOption('size', null, InputOption::VALUE_OPTIONAL, 'Constant field size (bytes)', 4096)
        ;

        $help = <<<'EOF'
Documents inserted to the database will have the following structure:

    {
        "_id": <info><ObjectId></info>,
        "x": <info><random-integer></info>,
        "y": <info><fixed-string></info>
    }

The number of documents to be inserted is configurable, as is the size of the
fixed string.

The <info>drop</info> and <info>remove</info> options may be used to clear the collection of existing
documents before insertion. When inserting into a sharded cluster, dropping the
collection will remove it from the shard configuration and likewise delete the
shard key's index. In that case, <info>remove</info> may be preferable.
EOF;

        $this->setHelp($help . "\n\n" . $this->getHelp());
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
