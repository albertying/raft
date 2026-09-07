#include "kv_store.h"
#include <sstream>

std::string KVStore::apply(const std::string& command) {
    std::istringstream ss(command);
    std::string op;
    ss >> op;

    if (op == "SET") {
        std::string key, val;
        ss >> key >> val;
        data_[key] = val;
        return "OK";
    } else if (op == "DEL") {
        std::string key;
        ss >> key;
        data_.erase(key);
        return "OK";
    } else if (op == "GET") {
        std::string key;
        ss >> key;
        auto it = data_.find(key);
        if (it != data_.end()) return it->second;
        return "";
    }
    return "ERR unknown command";
}

std::string KVStore::get(const std::string& key) const {
    auto it = data_.find(key);
    if (it != data_.end()) return it->second;
    return "";
}
