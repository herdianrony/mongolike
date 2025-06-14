<?php

namespace MongoLike;

class Cursor implements \Iterator
{
    protected Collection $collection;
    protected $criteria;
    protected int $position = 0;
    protected array $data = [];
    protected ?int $limit = null;
    protected int $skip = 0;
    protected array $sort = [];

    public function __construct(Collection $collection, $criteria)
    {
        $this->collection = $collection;
        $this->criteria = $criteria;
        $this->loadData();
    }

    public function count(): int
    {
        $stmt = $this->buildQuery(true);
        return (int) $stmt->fetchColumn();
    }

    public function limit(int $num): self
    {
        $this->limit = $num;
        $this->loadData();
        return $this;
    }

    public function skip(int $num): self
    {
        $this->skip = $num;
        $this->loadData();
        return $this;
    }

    public function sort(array $fields): self
    {
        $this->sort = $fields;
        $this->loadData();
        return $this;
    }

    public function each(callable $callable): int
    {
        $count = 0;
        foreach ($this->data as $doc) {
            $callable($doc);
            $count++;
        }
        return $count;
    }

    public function toArray(): array
    {
        return $this->data;
    }

    public function getNext(): ?array
    {
        $this->position++;
        return $this->data[$this->position] ?? null;
    }

    public function rewind(): void
    {
        $this->position = 0;
    }

    public function current(): mixed
    {
        return $this->data[$this->position] ?? null;
    }

    public function key(): mixed
    {
        return $this->position;
    }

    public function next(): void
    {
        $this->position++;
    }

    public function valid(): bool
    {
        return isset($this->data[$this->position]);
    }

    protected function loadData(): void
    {
        $stmt = $this->buildQuery();
        $results = [];

        while ($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            $document = json_decode($row['document'], true);
            $document['_id'] = $row['id'];
            $results[] = $document;
        }

        // Apply sorting
        if ($this->sort) {
            usort($results, function ($a, $b) {
                foreach ($this->sort as $field => $direction) {
                    $valA = $a[$field] ?? null;
                    $valB = $b[$field] ?? null;

                    if ($valA === $valB) continue;

                    $result = $valA <=> $valB;
                    return $direction > 0 ? $result : -$result;
                }
                return 0;
            });
        }

        // Apply skip and limit
        if ($this->skip) $results = array_slice($results, $this->skip);
        if ($this->limit) $results = array_slice($results, 0, $this->limit);

        $this->data = $results;
    }

    protected function buildQuery(bool $count = false): \PDOStatement
    {
        $sql = $count ? 'SELECT COUNT(*) FROM data' : 'SELECT id, document FROM data';
        $params = [];

        if ($this->criteria && is_array($this->criteria)) {
            $where = [];
            foreach ($this->criteria as $field => $value) {
                if (is_array($value)) {
                    foreach ($value as $operator => $opValue) {
                        $param = ":{$field}_" . uniqid();
                        switch ($operator) {
                            case '$gt':
                                $where[] = "json_extract(document, '\$.$field') > $param";
                                break;
                            case '$lt':
                                $where[] = "json_extract(document, '\$.$field') < $param";
                                break;
                            case '$gte':
                                $where[] = "json_extract(document, '\$.$field') >= $param";
                                break;
                            case '$lte':
                                $where[] = "json_extract(document, '\$.$field') <= $param";
                                break;
                            case '$ne':
                                $where[] = "json_extract(document, '\$.$field') != $param";
                                break;
                            case '$eq':
                                $where[] = "json_extract(document, '\$.$field') = $param";
                                break;
                            case '$in':
                                $placeholders = [];
                                foreach ($opValue as $i => $val) {
                                    $p = ":{$field}_in_$i";
                                    $placeholders[] = $p;
                                    $params[$p] = $val;
                                }
                                $where[] = "json_extract(document, '\$.$field') IN (" . implode(',', $placeholders) . ")";
                                break;
                        }
                        if ($operator !== '$in') $params[$param] = $opValue;
                    }
                } else {
                    $param = ":{$field}";
                    $where[] = "json_extract(document, '\$.$field') = $param";
                    $params[$param] = $value;
                }
            }

            if ($where) $sql .= ' WHERE ' . implode(' AND ', $where);
        }

        $stmt = $this->collection->getConnection()->prepare($sql);
        $stmt->execute($params);
        return $stmt;
    }
}
