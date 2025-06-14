<?php

namespace MongoLike;

class Relation
{
    protected Collection $localCollection;
    protected Collection $foreignCollection;
    protected string $localField;
    protected string $foreignField;

    public function __construct(
        Collection $localCollection,
        Collection $foreignCollection,
        string $localField,
        string $foreignField
    ) {
        $this->localCollection = $localCollection;
        $this->foreignCollection = $foreignCollection;
        $this->localField = $localField;
        $this->foreignField = $foreignField;
    }

    public function getFor(array $document): array
    {
        $localValue = $document[$this->localField] ?? null;
        return $this->foreignCollection->find([$this->foreignField => $localValue])->toArray();
    }

    public function attach(array $localDocument, array $foreignDocument): bool
    {
        $localValue = $localDocument[$this->localField] ?? null;
        $foreignDocument[$this->foreignField] = $localValue;
        return (bool) $this->foreignCollection->save($foreignDocument);
    }

    public function detach(array $localDocument, array $foreignDocument): bool
    {
        $localValue = $localDocument[$this->localField] ?? null;
        $foreignId = $foreignDocument['_id'] ?? null;

        if ($foreignId && ($this->foreignCollection->findOne(['_id' => $foreignId]))) {
            return $this->foreignCollection->remove(['_id' => $foreignId]) > 0;
        }
        return false;
    }

    public function sync(array $localDocument, array $foreignDocuments): bool
    {
        $localValue = $localDocument[$this->localField] ?? null;

        // Remove existing relations
        $this->foreignCollection->remove([$this->foreignField => $localValue]);

        // Add new relations
        foreach ($foreignDocuments as $doc) {
            $doc[$this->foreignField] = $localValue;
            $this->foreignCollection->insert($doc);
        }

        return true;
    }

    public function countFor(array $document): int
    {
        $localValue = $document[$this->localField] ?? null;
        return $this->foreignCollection->count([$this->foreignField => $localValue]);
    }
}
