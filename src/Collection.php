<?php

namespace MongoLike;

class Collection
{
    const DATA_FIELD = 'document';

    public string $name;
    protected Database $database;
    protected \PDO $connection;
    protected IndexManager $indexManager;

    public function __construct(string $name, Database $database)
    {
        $this->name = $name;
        $this->database = $database;
        $dbFile = $database->getPath() . $name . '.sqlite';
        $this->connection = new \PDO('sqlite:' . $dbFile);
        $this->connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $this->indexManager = new IndexManager();
    }

    public function getDatabase(): Database
    {
        return $this->database;
    }

    public function createTable(): void
    {
        $this->connection->exec(sprintf(
            'CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY AUTOINCREMENT, %s TEXT)',
            self::DATA_FIELD
        ));
        $this->connection->exec('CREATE TABLE IF NOT EXISTS _indexes (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            field TEXT,
            type TEXT,
            options TEXT
        )');
    }

    public function drop(): void
    {
        $dbFile = $this->database->getPath() . $this->name . '.sqlite';
        if (file_exists($dbFile)) unlink($dbFile);
    }

    public function renameCollection(string $newname): void
    {
        $oldFile = $this->database->getPath() . $this->name . '.sqlite';
        $newFile = $this->database->getPath() . $newname . '.sqlite';

        if (file_exists($newFile)) {
            throw new \Exception("Collection '$newname' already exists");
        }

        rename($oldFile, $newFile);
        $this->name = $newname;
    }

    public function insert(array $document): string
    {
        if (!isset($document['_id'])) {
            $document['_id'] = uniqid();
        }

        $stmt = $this->connection->prepare(sprintf(
            'INSERT INTO data (%s) VALUES (:data)',
            self::DATA_FIELD
        ));

        $stmt->execute([':data' => json_encode($document)]);
        return $document['_id'];
    }

    public function save(array &$document): string
    {
        if (isset($document['_id'])) {
            $this->update(['_id' => $document['_id']], $document);
            return $document['_id'];
        }
        return $this->insert($document);
    }

    public function update(array $criteria, array $data): int
    {
        $rows = $this->find($criteria)->toArray();
        $count = 0;

        foreach ($rows as $row) {
            $merged = array_merge($row, $data);
            $stmt = $this->connection->prepare(
                'UPDATE data SET document = :document WHERE id = :id'
            );
            $stmt->execute([
                ':document' => json_encode($merged),
                ':id' => $row['_id']
            ]);
            $count++;
        }
        return $count;
    }

    public function remove(array $criteria = []): int
    {
        $rows = $this->find($criteria)->toArray();
        $count = 0;

        foreach ($rows as $row) {
            $stmt = $this->connection->prepare('DELETE FROM data WHERE id = :id');
            $stmt->execute([':id' => $row['_id']]);
            $count++;
        }
        return $count;
    }

    public function count(): int
    {
        $stmt = $this->connection->query('SELECT COUNT(*) FROM data');
        return (int) $stmt->fetchColumn();
    }

    public function find(array|callable $criteria = []): Cursor
    {
        return new Cursor($this, $criteria);
    }

    public function findOne(array|callable $criteria = []): ?array
    {
        return $this->find($criteria)->limit(1)->getNext();
    }

    public function getConnection(): \PDO
    {
        return $this->connection;
    }

    public function createIndex(string $field, array $options = []): bool
    {
        return $this->indexManager->createIndex(
            $this->database->getPath(),
            $this->name,
            $field,
            $options
        );
    }

    public function dropIndex(string $field): bool
    {
        return $this->indexManager->dropIndex(
            $this->database->getPath(),
            $this->name,
            $field
        );
    }

    public function listIndexes(): array
    {
        return $this->indexManager->listIndexes(
            $this->database->getPath(),
            $this->name
        );
    }

    public function aggregate(array $pipeline): array
    {
        $aggregator = new Aggregation($this);
        return $aggregator->execute($pipeline);
    }

    public function relate(string $collectionName, string $foreignField, string $localField = '_id'): Relation
    {
        $relatedCollection = $this->database->selectCollection($collectionName);
        return new Relation($this, $relatedCollection, $localField, $foreignField);
    }
}
