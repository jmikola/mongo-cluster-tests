<?php

namespace Command;

use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Stopwatch\Stopwatch;

class CountCommand extends FindCommand
{
    protected function configure()
    {
        parent::configure();

        $this
            ->setName('count')
            ->setDescription('Count documents')
            ->setHelp(<<<'EOF'
Count documents matching the given criteria. The command will be executed using
all possible read preferences.

The query argument must be valid JSON. Object properties and strings must be
enclosed in double quotes.
EOF
            )
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $mongoClient = new \MongoClient($input->getOption('server'));
        $collection = $mongoClient->selectCollection($input->getOption('db'), $input->getOption('collection'));
        $cmd = $mongoClient->selectCollection($input->getOption('db'), '$cmd');

        \MongoCursor::$timeout = (int) $input->getOption('timeout');
        $query = $this->decodeQuery($input->getArgument('query'));

        $output->writeln(sprintf('Counting documents in %s matching: %s', $collection, json_encode($query)));

        $stopwatch = new Stopwatch();

        foreach ($this->readPreferences as $readPreference) {
            $stopwatch->start('count:' . $readPreference);
            $collection->setReadPreference($readPreference);
            try {
                $count = $collection->count($query);
                $event = $stopwatch->stop('count:' . $readPreference);
                $output->writeln(sprintf('Counted %d documents with %s read preference in %.3f seconds.', $count, $readPreference, $event->getDuration() / 1000));
            } catch (\MongoCursorTimeoutException $e) {
                $event = $stopwatch->stop('count:' . $readPreference);
                $output->writeln(sprintf('Counting documents with %s read preference timed out after %.3f seconds.', $readPreference, $event->getDuration() / 1000));
            }

            $cmd->setReadPreference($readPreference);
            $cursor = $cmd->find(array('count' => $collection, 'query' => $query));
            $this->printExplain($cursor, $output);
        }
    }
}
