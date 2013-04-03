<?php

namespace Command;

use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class CountCommand extends AbstractCommand
{
    protected function configure()
    {
        parent::configure();

        $this
            ->setName('count')
            ->addOption('query', null, InputOption::VALUE_OPTIONAL, 'Query criteria (JSON)', '{}')
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
        $query = $this->decodeJson($input->getOption('query'));
        $cmd = $this->db->selectCollection('$cmd');

        $output->writeln(sprintf('Counting documents in %s matching: %s', $this->collection, json_encode($query)));

        foreach ($this->readPreferences as $readPreference) {
            $this->collection->setReadPreference($readPreference);
            $eventName = 'count:' . $readPreference;
            $this->stopwatch->start($eventName);

            try {
                $count = $this->collection->count($query);
                $event = $this->stopwatch->stop($eventName);
                $output->writeln(sprintf('Counted %d documents with %s read preference in %.3f seconds.', $count, $readPreference, $event->getDuration() / 1000));
            } catch (\MongoCursorTimeoutException $e) {
                $event = $this->stopwatch->stop($eventName);
                $output->writeln(sprintf('Counting documents with %s read preference timed out after %.3f seconds.', $readPreference, $event->getDuration() / 1000));
            }
        }

        $output->writeln('');
        $output->writeln(sprintf('Counting documents via findOne() on %s', $cmd));

        foreach ($this->readPreferences as $readPreference) {
            $cmd->setReadPreference($readPreference);
            $eventName = '$cmd:' . $readPreference;
            $this->stopwatch->start($eventName);

            try {
                $result = $cmd->findOne(array('count' => $this->collection->getName(), 'query' => $query));
                $event = $this->stopwatch->stop($eventName);
                $output->writeln(sprintf('Counted %d documents with %s read preference in %.3f seconds.', $result['n'], $readPreference, $event->getDuration() / 1000));
                $output->writeln(sprintf('  $cmd result: %s', json_encode($result)));
            } catch (\MongoCursorTimeoutException $e) {
                $event = $this->stopwatch->stop($eventName);
                $output->writeln(sprintf('Counting documents with %s read preference timed out after %.3f seconds.', $readPreference, $event->getDuration() / 1000));
            }
        }
    }
}
