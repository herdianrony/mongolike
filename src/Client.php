<?php

namespace MongoLike;

class Client
{
    protected $path;
    protected $databases = [];

    public function __construct($path)
    {
        $this->path = rtrim(str_replace("\\", "/", $path), '/') . '/';
    }

    public function selectDB($name)
    {
        if (!isset($this->databases[$name])) {
            $dbfile = $this->path . $name . '.sqlite';
            $this->databases[$name] = new Database($dbfile);
        }
        return $this->databases[$name];
    }

    public function listDBs()
    {
        $dbs = [];
        if (!is_dir($this->path)) return $dbs;

        foreach (scandir($this->path) as $file) {
            if ($file === '.' || $file === '..') continue;
            if (pathinfo($file, PATHINFO_EXTENSION) === 'sqlite') {
                $dbs[] = pathinfo($file, PATHINFO_FILENAME);
            }
        }
        return $dbs;
    }
}
