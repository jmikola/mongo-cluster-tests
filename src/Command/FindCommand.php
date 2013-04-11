<?php

namespace Command;

use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class FindCommand extends AbstractCommand
{
    protected function configure()
    {
        parent::configure();

        $this
            ->setName('find')
            ->setDescription('Find documents')
            ->addOption('query', null, InputOption::VALUE_OPTIONAL, 'Query criteria (JSON)', '{}')
        ;

        $help = <<<'EOF'
Find documents matching the given criteria.

If a read preferenec has not been specified, the query will be executed once for
each possible read preference. If read preference tags have been specified, they
will be re-used for each query.

The query argument must be valid JSON. Object properties and strings must be
enclosed in double quotes. Additionally, it may be necessary to wrap the query
with single quotes to disable evaluation of query operators (prefixed by <info>$</info>)
as shell variables.
EOF;

        $this->setHelp($help . "\n\n" . $this->getHelp());
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $query = $this->decodeJson($input->getOption('query'));

        $readPreferenceTags = $this->getReadPreferenceTags();
        $readPreferences = null !== $input->getOption('readPreference')
            ? array($input->getOption('readPreference'))
            : $this->readPreferences;

        $output->writeln(sprintf('Finding documents in %s matching: %s', $this->collection, json_encode($query)));
        $output->writeln(sprintf('Using read preference tags: %s', json_encode($readPreferenceTags)));

        foreach ($readPreferences as $readPreference) {
            // Work-around for https://jira.mongodb.org/browse/PHP-735
            if (-1 === version_compare(phpversion('mongo'), '1.4.0')) {
                $this->mongo->setReadPreference($readPreference, $readPreference === \MongoClient::RP_PRIMARY ? array() : $readPreferenceTags);
                $this->collection = $this->mongo->selectCollection($this->db, $this->collection->getName());
            }

            $this->collection->setReadPreference($readPreference, $readPreference === \MongoClient::RP_PRIMARY ? array() : $readPreferenceTags);

            $eventName = 'find:' . $readPreference;
            $this->stopwatch->start($eventName);

            try {
                $cursor = $this->collection->find($query);

                /* Exhaust the cursor by iterating. Avoid iterator_to_array() in
                 * order to conserve memory usage.
                 */
                for ($i = 0; $cursor->hasNext(); $i++, $cursor->next());

                $event = $this->stopwatch->stop($eventName);
                $output->writeln(sprintf('Found %d documents with %s read preference in %.3f seconds.', $i, $readPreference, $event->getDuration() / 1000));
            } catch (\MongoCursorTimeoutException $e) {
                $event = $this->stopwatch->stop($eventName);
                $output->writeln(sprintf('Found %d documents with %s read preference before timing out after %.3f seconds.', $i, $readPreference, $event->getDuration() / 1000));
                $output->writeln(sprintf('  %s: %s', get_class($e), $e->getMessage()));
            } catch (\MongoCursorException $e) {
                $event = $this->stopwatch->stop($eventName);
                $output->writeln(sprintf('Error finding documents with %s read preference: %s', $readPreference, $e->getMessage()));
                $output->writeln(sprintf('  %s: %s', get_class($e), $e->getMessage()));

                // Skip explain after a non-timeout MongoCursorException
                continue;
            }

            $this->doExplain($cursor, $output);
        }
    }

    protected function doExplain(\MongoCursor $cursor, OutputInterface $output)
    {
        $explain = $cursor->explain();

        if (isset($explain['server'])) {
            $output->writeln(sprintf('  explain.server = %s', $explain['server']));
        }

        if (isset($explain['shards'])) {
            foreach($explain['shards'] as $shard => $shardExplains) {
                $shard = strtok($shard, '/');
                foreach ($shardExplains as $i => $shardExplain) {
                    $output->writeln(sprintf('  explain.shards["%s"][%d].server = %s', $shard, $i, $shardExplain['server']));
                    $output->writeln(sprintf('  explain.shards["%s"][%d].millis = %d', $shard, $i, $shardExplain['millis']));
                }
            }
            $output->writeln(sprintf('  explain.millisShardTotal = %d', $explain['millisShardTotal']));
            $output->writeln(sprintf('  explain.millisShardAvg = %d', $explain['millisShardAvg']));
        }

        $output->writeln(sprintf('  explain.millis = %d', $explain['millis']));
    }
}
