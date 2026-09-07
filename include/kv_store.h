#pragma once
#include <string>
#include <unordered_map>

// KVStore - simple key/value state machine
// Parses commands: "SET key val", "DEL key", "GET key"
class KVStore {
public:
    KVStore() = default;
    ~KVStore() = default;
};
