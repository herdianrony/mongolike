<?php

namespace MongoLike;

class Aggregation
{
    protected Collection $collection;

    public function __construct(Collection $collection)
    {
        $this->collection = $collection;
    }

    public function execute(array $pipeline): array
    {
        $results = $this->collection->find()->toArray();

        foreach ($pipeline as $stage) {
            $operator = key($stage);
            $arguments = current($stage);

            switch ($operator) {
                case '$match':
                    $results = $this->match($results, $arguments);
                    break;
                case '$group':
                    $results = $this->group($results, $arguments);
                    break;
                case '$sort':
                    $results = $this->sort($results, $arguments);
                    break;
                case '$limit':
                    $results = array_slice($results, 0, $arguments);
                    break;
                case '$skip':
                    $results = array_slice($results, $arguments);
                    break;
                case '$lookup':
                    $results = $this->lookup($results, $arguments);
                    break;
                case '$project':
                    $results = $this->project($results, $arguments);
                    break;
                case '$unwind':
                    $results = $this->unwind($results, $arguments);
                    break;
            }
        }

        return $results;
    }

    protected function match(array $data, array $criteria): array
    {
        return array_filter($data, function ($doc) use ($criteria) {
            foreach ($criteria as $field => $condition) {
                $value = $doc[$field] ?? null;

                if (is_array($condition)) {
                    foreach ($condition as $operator => $opValue) {
                        switch ($operator) {
                            case '$gt':
                                if (!($value > $opValue)) return false;
                                break;
                            case '$lt':
                                if (!($value < $opValue)) return false;
                                break;
                            case '$gte':
                                if (!($value >= $opValue)) return false;
                                break;
                            case '$lte':
                                if (!($value <= $opValue)) return false;
                                break;
                            case '$ne':
                                if (!($value != $opValue)) return false;
                                break;
                            case '$eq':
                                if (!($value == $opValue)) return false;
                                break;
                            case '$in':
                                if (!in_array($value, $opValue)) return false;
                                break;
                            case '$nin':
                                if (in_array($value, $opValue)) return false;
                                break;
                            case '$exists':
                                $exists = array_key_exists($field, $doc);
                                if (($opValue && !$exists) || (!$opValue && $exists)) return false;
                                break;
                        }
                    }
                } else {
                    if ($value != $condition) return false;
                }
            }
            return true;
        });
    }

    protected function group(array $data, array $arguments): array
    {
        $grouped = [];
        $idField = $arguments['_id'] ?? null;

        foreach ($data as $doc) {
            $id = $idField ? (is_array($idField) ? $this->extractId($doc, $idField) : ($doc[$idField] ?? null)) : null;

            if (!isset($grouped[$id])) {
                $grouped[$id] = ['_id' => $id];
                foreach ($arguments as $key => $value) {
                    if ($key === '_id') continue;
                    $grouped[$id][$key] = $this->initAccumulator($value);
                }
            }

            foreach ($arguments as $key => $value) {
                if ($key === '_id') continue;
                $this->accumulate($grouped[$id][$key], $value, $doc);
            }
        }

        // Finalize accumulators
        foreach ($grouped as &$group) {
            foreach ($arguments as $key => $value) {
                if ($key === '_id') continue;
                $this->finalize($group[$key], $value);
            }
        }

        return array_values($grouped);
    }

    protected function extractId(array $doc, array $idFields): array
    {
        $id = [];
        foreach ($idFields as $field => $alias) {
            $id[$alias] = $doc[$field] ?? null;
        }
        return $id;
    }

    protected function initAccumulator($value)
    {
        $operator = key($value);
        switch ($operator) {
            case '$sum':
                return 0;
            case '$avg':
                return ['sum' => 0, 'count' => 0];
            case '$min':
                return PHP_INT_MAX;
            case '$max':
                return PHP_INT_MIN;
            case '$push':
                return [];
            case '$addToSet':
                return [];
            default:
                return null;
        }
    }

    protected function accumulate(&$accumulator, $value, array $doc)
    {
        $operator = key($value);
        $field = substr(current($value), 1); // Remove '$' prefix

        switch ($operator) {
            case '$sum':
                $accumulator += $doc[$field] ?? 0;
                break;
            case '$avg':
                if (isset($doc[$field])) {
                    $accumulator['sum'] += $doc[$field];
                    $accumulator['count']++;
                }
                break;
            case '$min':
                if (isset($doc[$field]) && $doc[$field] < $accumulator) {
                    $accumulator = $doc[$field];
                }
                break;
            case '$max':
                if (isset($doc[$field]) && $doc[$field] > $accumulator) {
                    $accumulator = $doc[$field];
                }
                break;
            case '$push':
                $accumulator[] = $doc[$field] ?? null;
                break;
            case '$addToSet':
                $val = $doc[$field] ?? null;
                if (!in_array($val, $accumulator)) {
                    $accumulator[] = $val;
                }
                break;
        }
    }

    protected function finalize(&$accumulator, $value)
    {
        $operator = key($value);

        switch ($operator) {
            case '$avg':
                $accumulator = $accumulator['count'] > 0
                    ? $accumulator['sum'] / $accumulator['count']
                    : 0;
                break;
        }
    }

    protected function sort(array $data, array $criteria): array
    {
        usort($data, function ($a, $b) use ($criteria) {
            foreach ($criteria as $field => $direction) {
                $valA = $a[$field] ?? null;
                $valB = $b[$field] ?? null;

                if ($valA === $valB) continue;

                $result = $valA <=> $valB;
                return $direction > 0 ? $result : -$result;
            }
            return 0;
        });
        return $data;
    }

    protected function lookup(array $data, array $arguments): array
    {
        $database = $this->collection->getDatabase();
        $fromCollection = $database->selectCollection($arguments['from']);

        $localField = $arguments['localField'];
        $foreignField = $arguments['foreignField'];
        $as = $arguments['as'];

        foreach ($data as &$doc) {
            $localValue = $doc[$localField] ?? null;
            $related = $fromCollection->find([$foreignField => $localValue])->toArray();
            $doc[$as] = $related;
        }

        return $data;
    }


    protected function project(array $data, array $fields): array
    {
        return array_map(function ($doc) use ($fields) {
            $projected = [];
            foreach ($fields as $field => $include) {
                if ($include === 1) {
                    $projected[$field] = $doc[$field] ?? null;
                } elseif (is_array($include)) {
                    // Handle nested projections
                    $subDoc = $doc[$field] ?? [];
                    if (is_array($subDoc)) {
                        $projected[$field] = $this->project([$subDoc], $include)[0] ?? [];
                    }
                }
            }
            return $projected;
        }, $data);
    }

    protected function unwind(array $data, string $path): array
    {
        $unwound = [];
        $path = trim($path, '$');

        foreach ($data as $doc) {
            $array = $doc[$path] ?? [];
            if (!is_array($array)) continue;

            foreach ($array as $item) {
                $newDoc = $doc;
                $newDoc[$path] = $item;
                $unwound[] = $newDoc;
            }
        }

        return $unwound;
    }
}
