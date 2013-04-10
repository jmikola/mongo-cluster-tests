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
            ->setDescription('Count documents')
            ->addOption('query', null, InputOption::VALUE_OPTIONAL, 'Query criteria (JSON)', '{}')
        ;

        $help = <<<'EOF'
Count documents matching the given criteria.

If a read preferenec has not been specified, the query will be executed once for
each possible read preference. If read preference tags have been specified, they
will be re-used for each query.

The query argument must be valid JSON. Object properties and strings must be
enclosed in double quotes. Additionally, it may be necessary to wrap the query
with single quotes to disable evaluation of query operators (prefixed by <info>$</info>)
as shell variables.
EOF;

        $this->setHelp($help . "\n\n" . $this->getHelp());
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $query = $this->decodeJson($input->getOption('query'));
        $cmd = $this->db->selectCollection('$cmd');

        $readPreferenceTags = $this->getReadPreferenceTags();
        $readPreferences = null !== $input->getOption('readPreference')
            ? array($input->getOption('readPreference'))
            : $this->readPreferences;

        $output->writeln(sprintf('Counting documents in %s matching: %s', $this->collection, json_encode($query)));
        $output->writeln(sprintf('Using read preference tags: %s', json_encode($readPreferenceTags)));

        foreach ($readPreferences as $readPreference) {
            // Work-around for https://jira.mongodb.org/browse/PHP-735
            if (-1 === version_compare(phpversion('mongo'), '1.4.0')) {
                $this->mongo->setReadPreference($readPreference, $readPreference === \MongoClient::RP_PRIMARY ? array() : $readPreferenceTags);
                $this->collection = $this->mongo->selectCollection($this->db, $this->collection->getName());
            }

            $this->collection->setReadPreference($readPreference, $readPreference === \MongoClient::RP_PRIMARY ? array() : $readPreferenceTags);
            $eventName = 'count:' . $readPreference;
            $this->stopwatch->start($eventName);

            try {
                $count = $this->collection->count($query);
                $event = $this->stopwatch->stop($eventName);
                $output->writeln(sprintf('Counted %d documents with %s read preference in %.3f seconds.', $count, $readPreference, $event->getDuration() / 1000));
            } catch (\MongoCursorException $e) {
                $event = $this->stopwatch->stop($eventName);
                $output->writeln(sprintf('Error counting documents with %s read preference after %.3f seconds', $readPreference, $event->getDuration() / 1000));
                $output->writeln(sprintf('  %s: %s', get_class($e), $e->getMessage()));
            }
        }

        $output->writeln('');
        $output->writeln(sprintf('Counting documents via findOne() on %s', $cmd));

        foreach ($readPreferences as $readPreference) {
            $cmd->setReadPreference($readPreference, $readPreference === \MongoClient::RP_PRIMARY ? array() : $readPreferenceTags);
            $eventName = '$cmd:' . $readPreference;
            $this->stopwatch->start($eventName);

            try {
                $result = $cmd->findOne(array('count' => $this->collection->getName(), 'query' => $query));
                $event = $this->stopwatch->stop($eventName);
                $output->writeln(sprintf('Counted %d documents with %s read preference in %.3f seconds.', $result['n'], $readPreference, $event->getDuration() / 1000));
                $output->writeln(sprintf('  $cmd result: %s', json_encode($result)));
            } catch (\MongoCursorException $e) {
                $event = $this->stopwatch->stop($eventName);
                $output->writeln(sprintf('Error counting documents with %s read preference after %.3f seconds', $readPreference, $event->getDuration() / 1000));
                $output->writeln(sprintf('  %s: %s', get_class($e), $e->getMessage()));
            }
        }
    }
}
