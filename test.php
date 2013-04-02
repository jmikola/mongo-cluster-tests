#!/usr/bin/env php
<?php

use Symfony\Component\Console\Application;

require_once __DIR__.'/vendor/autoload.php';

$app = new Application();

$app->addCommands(array(
    new Command\InsertCommand(),
));

$app->run();
