<?php

namespace MongoLike;

class Client
{
    public $path;
    protected $databases = [];

    public function __construct(string $path)
    {
        $this->path = rtrim(str_replace("\\", "/", $path), '/') . '/';
    }

    public function listDBs(): array
    {
        $dbs = [];
        if (!is_dir($this->path)) return $dbs;

        foreach (new \DirectoryIterator($this->path) as $fileInfo) {
            if ($fileInfo->isDot() || !$fileInfo->isDir()) continue;
            $dbs[] = $fileInfo->getFilename();
        }
        return $dbs;
    }

    public function selectDB(string $name): Database
    {
        if (!isset($this->databases[$name])) {
            $this->databases[$name] = new Database($this->path . $name . '/');
        }
        return $this->databases[$name];
    }

    public function selectCollection(string $dbname, string $collectionname): Collection
    {
        return $this->selectDB($dbname)->selectCollection($collectionname);
    }
}
