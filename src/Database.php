<?php

namespace MongoLike;

class Database
{
    protected $dbfile;
    protected $connection;
    protected $collections = [];

    public function __construct($dbfile)
    {
        $this->dbfile = $dbfile;
        $this->connect();
    }

    protected function connect()
    {
        // Buat direktori jika belum ada
        $dir = dirname($this->dbfile);
        if (!is_dir($dir)) mkdir($dir, 0777, true);

        $this->connection = new \PDO('sqlite:' . $this->dbfile);
        $this->connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    }

    public function selectCollection($name)
    {
        $sanitized = $this->sanitizeCollectionName($name);

        if (!isset($this->collections[$sanitized])) {
            $this->collections[$sanitized] = new Collection($sanitized, $this->connection);
        }
        return $this->collections[$sanitized];
    }

    public function getConnection()
    {
        return $this->connection;
    }

    public function listCollections()
    {
        $stmt = $this->connection->query("SELECT name FROM sqlite_master WHERE type='table'");
        return $stmt->fetchAll(\PDO::FETCH_COLUMN);
    }

    public function drop()
    {
        if (file_exists($this->dbfile)) {
            unlink($this->dbfile);
        }
    }

    private function sanitizeCollectionName($name)
    {
        return preg_replace('/[^a-zA-Z0-9_]/', '_', $name);
    }
}
