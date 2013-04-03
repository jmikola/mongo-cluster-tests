<?php

namespace Generator;

class DocumentGenerator implements \Countable, \Iterator
{
    private $size;
    private $constantValue;
    private $position;
    private $current;
    private $documentBsonSize;

    public function __construct($size, $constantValue)
    {
        $this->size = $size;
        $this->constantValue = $constantValue;
        $this->position = 0;
    }

    public function getDocumentBsonSize()
    {
        if (!isset($this->documentBsonSize)) {
            $this->documentBsonSize = strlen(bson_encode($this->generate()));
        }

        return $this->documentBsonSize;
    }

    public function count()
    {
        return $this->size;
    }

    public function current()
    {
        if (!isset($this->current)) {
            $this->current = $this->generate();
        }

        return $this->current;
    }

    public function key()
    {
        return $this->position;
    }

    public function next()
    {
        $this->current = null;
        $this->position += 1;
    }

    public function rewind()
    {
        $this->position = 0;
    }

    public function valid()
    {
        return $this->position < $this->size;
    }

    private function generate()
    {
        return array(
            '_id' => new \MongoId(),
            'x' => mt_rand(),
            'y' => $this->constantValue,
        );
    }
}
