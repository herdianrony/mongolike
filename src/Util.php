<?php

namespace MongoLike;

class Util
{
    public static function isAssocArray(array $array): bool
    {
        if ([] === $array) return false;
        return array_keys($array) !== range(0, count($array) - 1);
    }

    public static function arrayGet(array $array, string $key, $default = null)
    {
        $keys = explode('.', $key);
        $current = $array;

        foreach ($keys as $k) {
            if (!is_array($current)) return $default;
            if (!array_key_exists($k, $current)) return $default;
            $current = $current[$k];
        }

        return $current;
    }
}
