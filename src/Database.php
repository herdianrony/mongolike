<?php

namespace MongoLike;

class Database
{
    protected $path;
    protected $collections = [];

    public function __construct(string $path)
    {
        if (!file_exists($path)) mkdir($path, 0777, true);
        $this->path = $path;
    }

    public function vacuum(): void
    {
        foreach ($this->listCollections() as $collection) {
            $collection->drop();
        }
        if (is_dir($this->path)) rmdir($this->path);
    }

    public function drop(): void
    {
        $this->vacuum();
    }

    public function createCollection(string $name): Collection
    {
        $collection = new Collection($name, $this);
        $collection->createTable();
        return $collection;
    }

    public function dropCollection(string $name): void
    {
        $collection = $this->selectCollection($name);
        $collection->drop();
    }

    public function getCollectionNames(): array
    {
        return $this->listCollections();
    }

    public function listCollections(): array
    {
        $collections = [];
        if (!is_dir($this->path)) return $collections;

        foreach (new \DirectoryIterator($this->path) as $fileInfo) {
            if ($fileInfo->isDot() || !$fileInfo->isFile()) continue;
            if ($fileInfo->getExtension() === 'sqlite') {
                $collections[] = $fileInfo->getBasename('.sqlite');
            }
        }
        return $collections;
    }

    public function selectCollection(string $name): Collection
    {
        $sanitized = preg_replace('/[^a-zA-Z0-9_]/', '_', $name);
        if (!isset($this->collections[$sanitized])) {
            $this->collections[$sanitized] = new Collection($sanitized, $this);
        }
        return $this->collections[$sanitized];
    }

    public function getPath(): string
    {
        return $this->path;
    }
}
