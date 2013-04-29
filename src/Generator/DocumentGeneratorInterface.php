<?php

namespace Generator;

interface DocumentGeneratorInterface extends \Countable, \Iterator
{
    function getDocumentBsonSize();
}
