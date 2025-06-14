<?php

namespace MongoLike;

class IndexManager
{
    public function createIndex(string $dbPath, string $collection, string $field, array $options = []): bool
    {
        $dbFile = $dbPath . $collection . '.sqlite';

        try {
            $pdo = new \PDO('sqlite:' . $dbFile);
            $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

            $indexName = 'idx_' . $field . '_' . uniqid();
            $indexType = $options['unique'] ?? false ? 'UNIQUE INDEX' : 'INDEX';

            $pdo->exec("CREATE $indexType IF NOT EXISTS $indexName 
                        ON data (json_extract(document, '\$.$field'))");

            // Save index metadata
            $stmt = $pdo->prepare("INSERT INTO _indexes (name, field, type, options) 
                                  VALUES (?, ?, ?, ?)");
            $stmt->execute([
                $indexName,
                $field,
                $indexType,
                json_encode($options)
            ]);

            return true;
        } catch (\Exception $e) {
            error_log("Index creation failed: " . $e->getMessage());
            return false;
        }
    }

    public function dropIndex(string $dbPath, string $collection, string $field): bool
    {
        $dbFile = $dbPath . $collection . '.sqlite';

        try {
            $pdo = new \PDO('sqlite:' . $dbFile);
            $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

            // Get index name
            $stmt = $pdo->prepare("SELECT name FROM _indexes WHERE field = ?");
            $stmt->execute([$field]);
            $indexName = $stmt->fetchColumn();

            if ($indexName) {
                $pdo->exec("DROP INDEX IF EXISTS $indexName");
                $pdo->exec("DELETE FROM _indexes WHERE name = '$indexName'");
                return true;
            }

            return false;
        } catch (\Exception $e) {
            error_log("Index drop failed: " . $e->getMessage());
            return false;
        }
    }

    public function listIndexes(string $dbPath, string $collection): array
    {
        $dbFile = $dbPath . $collection . '.sqlite';
        $indexes = [];

        if (!file_exists($dbFile)) return $indexes;

        try {
            $pdo = new \PDO('sqlite:' . $dbFile);
            $stmt = $pdo->query("SELECT name, field, type, options FROM _indexes");

            while ($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                $indexes[] = [
                    'name' => $row['name'],
                    'field' => $row['field'],
                    'type' => $row['type'],
                    'options' => json_decode($row['options'], true)
                ];
            }

            return $indexes;
        } catch (\Exception $e) {
            return [];
        }
    }
}
