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
            ->addOption('query', null, InputOption::VALUE_OPTIONAL, 'Query criteria (JSON)', '{}')
            ->setDescription('Find documents')
            ->setHelp(<<<'EOF'
Find documents matching the given criteria. The query will be executed using
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

        $output->writeln(sprintf('Finding documents in %s matching: %s', $this->collection, json_encode($query)));

        foreach ($this->readPreferences as $readPreference) {
            $this->collection->setReadPreference($readPreference);

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
