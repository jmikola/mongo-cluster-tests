<?php

namespace Generator;

class SequentialDocumentGenerator extends AbstractDocumentGenerator
{
    protected function generate()
    {
        return array(
            '_id' => new \MongoId(),
            'x' => $this->position,
            'y' => $this->fixedString,
        );
    }
}
