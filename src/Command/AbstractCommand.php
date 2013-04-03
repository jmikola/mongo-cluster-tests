<?php

namespace Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Stopwatch\Stopwatch;

abstract class AbstractCommand extends Command
{
    protected $collection;
    protected $db;
    protected $mongo;
    protected $stopwatch;

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
            ->addOption('server', 's', InputOption::VALUE_OPTIONAL, 'MongoDB server', 'mongodb://localhost:27017')
            ->addOption('db', 'd', InputOption::VALUE_OPTIONAL, 'MongoDB database', 'test')
            ->addOption('collection', 'c', InputOption::VALUE_OPTIONAL, 'MongoDB collection', 'test')
            ->addOption('authDb', null, InputOption::VALUE_OPTIONAL, 'MongoClient auth database')
            ->addOption('username', null, InputOption::VALUE_OPTIONAL, 'MongoClient auth username')
            ->addOption('password', null, InputOption::VALUE_OPTIONAL, 'MongoClient auth password')
            ->addOption('readPreference', null, InputOption::VALUE_OPTIONAL, 'MongoClient read preference')
            ->addOption('readPreferenceTags', null, InputOption::VALUE_OPTIONAL | InputOption::VALUE_IS_ARRAY, 'MongoClient read preference tags')
            ->addOption('replicaSet', null, InputOption::VALUE_OPTIONAL, 'MongoClient replica set')
            ->addOption('connectTimeoutMS', null, InputOption::VALUE_OPTIONAL, 'MongoClient connection timeout (milliseconds)')
            ->addOption('socketTimeoutMS', null, InputOption::VALUE_OPTIONAL, 'MongoClient socket timeout (milliseconds)', 30000)
            ->addOption('w', null, InputOption::VALUE_OPTIONAL, 'MongoClient write concern', 1)
            ->addOption('wTimeout', null, InputOption::VALUE_OPTIONAL, 'MongoClient write concern timeout (milliseconds)', 10000)
            ->addOption('timeout', null, InputOption::VALUE_OPTIONAL, 'MongoCursor timeout (milliseconds)', 30000)
            ->setHelp(<<<'EOF'
<info>Common Options</info>

Write concern may be specified as a number or string (e.g. "majority").

Read preferences must correspond to a valid read preference string:

   * primary
   * primaryPreferred
   * secondary
   * secondaryPreferred
   * nearest "primary",

Read preference tags follow the format "key:value,key:value" for each tag set.
Multiple read preference tags may be specified like so:

  --readPreferenceTags dc:east,use:reporting --readPreferenceTags dc:west

Please see the MongoClient::__construct() documentation for more information.
EOF
            )
        ;
    }

    protected function initialize(InputInterface $input, OutputInterface $output)
    {
        $server = $input->getOption('server');
        $options = array();

        foreach (array('username', 'password', 'readPreference') as $option) {
            if (null !== ($value = $input->getOption($option))) {
                $options[$option] = $value;
            }
        }

        foreach (array('connectTimeoutMS', 'socketTimeoutMS', 'timeout', 'wTimeout') as $option) {
            if (null !== ($value = $input->getOption($option))) {
                $options[$option] = (int) $value;
            }
        }

        if (null !== ($value = $input->getOption('authDb'))) {
            $options['db'] = $value;
        }

        if (array() !== ($value = $input->getOption('readPreferenceTags'))) {
            $options['readPreferenceTags'] = $value;
        }

        if (null !== ($value = $input->getOption('w'))) {
            $options['w'] = is_numeric($value) ? (int) $value : $value;
        }

        $this->mongo = new \MongoClient($server, $options);
        $this->db = $this->mongo->selectDB($input->getOption('db'));
        $this->collection = $this->db->selectCollection($input->getOption('collection'));
        $this->stopwatch = new Stopwatch();
    }

    /**
     * Decodes a JSON string.
     *
     * @param string $json
     * @param boolean $assoc
     * @return mixed
     * @throws JsonDecodeException if JSON is invalid
     */
    protected function decodeJson($json, $assoc = false)
    {
        $errors = array(
            \JSON_ERROR_DEPTH => 'Maximum stack depth exceeded',
            \JSON_ERROR_STATE_MISMATCH => 'Invalid or malformed JSON',
            \JSON_ERROR_CTRL_CHAR => 'Control character or encoding error',
            \JSON_ERROR_SYNTAX => 'Syntax error (missing double quotes?)',
            \JSON_ERROR_UTF8 => 'UTF-8 encoding error',
        );

        $value = json_decode($json, $assoc);
        $error = json_last_error();

        if ($error === \JSON_ERROR_NONE) {
            return $value;
        }

        throw new JsonDecodeException(isset($errors[$error]) ? $errors[$error] : 'Unknown error');
    }

    /**
     * Returns the read preference tags from MongoClient.
     *
     * @return array
     */
    protected function getReadPreferenceTags()
    {
        $readPreference = $this->mongo->getReadPreference();

        return $readPreference['tagsets'];
    }
}
