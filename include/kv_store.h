#pragma once
#include <string>
#include <unordered_map>

// KVStore - simple key/value state machine
// Commands: "SET key val", "DEL key", "GET key"
// GET is read-only and doesn't go through Raft; SET/DEL are replicated
class KVStore {
public:
    KVStore() = default;
    ~KVStore() = default;

    // apply a replicated command, returns result string
    std::string apply(const std::string& command);

    // direct read (no consensus needed)
    std::string get(const std::string& key) const;

    // for snapshotting
    const std::unordered_map<std::string, std::string>& data() const { return data_; }
    void restore(const std::unordered_map<std::string, std::string>& data) { data_ = data; }

private:
    std::unordered_map<std::string, std::string> data_;
};
