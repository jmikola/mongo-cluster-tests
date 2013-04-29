<?php

namespace Generator;

class RandomDocumentGenerator extends AbstractDocumentGenerator
{
    protected function generate()
    {
        return array(
            '_id' => new \MongoId(),
            'x' => mt_rand(),
            'y' => $this->fixedString,
        );
    }
}
