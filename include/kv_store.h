#pragma once
#include <string>
#include <unordered_map>

// KVStore - simple key/value state machine
// Commands: "SET key val", "DEL key", "GET key"
class KVStore {
public:
    KVStore() = default;
    ~KVStore() = default;

    std::string apply(const std::string& command);
    std::string get(const std::string& key) const;

private:
    std::unordered_map<std::string, std::string> data_;
};
