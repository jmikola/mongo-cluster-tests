<?php

namespace Command;

use Exception\JsonDecodeException;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Stopwatch\Stopwatch;

class FindCommand extends Command
{
    protected $readPreferences = array(
        \MongoClient::RP_PRIMARY,
        \MongoClient::RP_PRIMARY_PREFERRED,
        \MongoClient::RP_SECONDARY,
        \MongoClient::RP_SECONDARY_PREFERRED,
        \MongoClient::RP_NEAREST,
    );

    protected function configure()
    {
        $this
            ->setName('find')
            ->setDefinition(array(
                new InputArgument('query', InputArgument::OPTIONAL, 'Query criteria (JSON)', '{}'),
                new InputOption('server', 's', InputOption::VALUE_OPTIONAL, 'MongoDB server', 'mongodb://localhost:27017'),
                new InputOption('db', 'd', InputOption::VALUE_OPTIONAL, 'MongoDB database', 'test'),
                new InputOption('collection', 'c', InputOption::VALUE_OPTIONAL, 'MongoDB collection', 'test'),
                new InputOption('timeout', null, InputOption::VALUE_OPTIONAL, 'Query timeout (milliseconds)', 5000),
            ))
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
        $mongoClient = new \MongoClient($input->getOption('server'));
        $collection = $mongoClient->selectCollection($input->getOption('db'), $input->getOption('collection'));

        \MongoCursor::$timeout = (int) $input->getOption('timeout');
        $query = $this->decodeQuery($input->getArgument('query'));

        $output->writeln(sprintf('Finding documents in %s matching: %s', $collection, json_encode($query)));

        $stopwatch = new Stopwatch();

        foreach ($this->readPreferences as $readPreference) {
            $stopwatch->start('find:' . $readPreference);
            $collection->setReadPreference($readPreference);
            try {
                $cursor = $collection->find($query);

                /* Exhaust the cursor by iterating. Avoid iterator_to_array() in
                 * order to conserve memory usage.
                 */
                for ($i = 0; $cursor->hasNext(); $i++, $cursor->next());

                $event = $stopwatch->stop('find:' . $readPreference);
                $output->writeln(sprintf('Found %d documents with %s read preference in %.3f seconds.', $i, $readPreference, $event->getDuration() / 1000));
            } catch (\MongoCursorTimeoutException $e) {
                $event = $stopwatch->stop('find:' . $readPreference);
                $output->writeln(sprintf('Found %d documents with %s read preference before timing out after %.3f seconds.', $i, $readPreference, $event->getDuration() / 1000));
            }

            $this->printExplain($cursor, $output);
        }
    }

    protected function decodeQuery($queryJson)
    {
        $errors = array(
            \JSON_ERROR_DEPTH => 'Maximum stack depth exceeded',
            \JSON_ERROR_STATE_MISMATCH => 'Invalid or malformed JSON',
            \JSON_ERROR_CTRL_CHAR => 'Control character or encoding error',
            \JSON_ERROR_SYNTAX => 'Syntax error (missing double quotes?)',
            \JSON_ERROR_UTF8 => 'UTF-8 encoding error',
        );

        $query = json_decode($queryJson);
        $error = json_last_error();

        if ($error === \JSON_ERROR_NONE) {
            return $query;
        }

        throw new JsonDecodeException(isset($errors[$error]) ? $errors[$error] : 'Unknown error');
    }

    protected function printExplain(\MongoCursor $cursor, OutputInterface $output)
    {
        $explain = $cursor->explain();

        $output->writeln(sprintf('  explain.server = %s', $explain['server']));

        if (isset($explain['shards'])) {
            foreach($explain['shards'] as $k => $shard) {
                $output->writeln(sprintf('  explain.shards[%d].server = %s', $k, $shard['server']));
            }
        }
    }
}
