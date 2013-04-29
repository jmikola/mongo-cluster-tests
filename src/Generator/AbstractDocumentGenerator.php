<?php

namespace Generator;

abstract class AbstractDocumentGenerator implements DocumentGeneratorInterface
{
    protected $size;
    protected $fixedString;
    protected $position;
    protected $current;
    protected $documentBsonSize;

    public function __construct($size, $fixedString)
    {
        $this->size = $size;
        $this->fixedString = $fixedString;
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

    abstract protected function generate();
}
