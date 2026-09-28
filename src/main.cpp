#include "raft.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <string>
#include <filesystem>

static int waitForLeader(const std::vector<std::unique_ptr<RaftNode>>& nodes, int timeoutMs = 3000) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        for (size_t i = 0; i < nodes.size(); i++) {
            if (nodes[i]->getState() == NodeState::LEADER) return (int)i;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return -1;
}

static bool waitForCommit(RaftNode* node, int targetIndex, int timeoutMs = 3000) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (node->getLastApplied() >= targetIndex) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

int main() {
    std::cout << "=== Raft KV Store Demo ===" << std::endl;

    // Set up WAL directory
    const std::string walDir = "/tmp/raft_demo_wal";
    std::filesystem::create_directories(walDir);

    // Build 3-node cluster
    constexpr int N = 3;
    std::vector<std::unique_ptr<RaftNode>> nodes;
    for (int i = 0; i < N; i++) {
        auto node = std::make_unique<RaftNode>(i);
        node->setWalDir(walDir);
        nodes.push_back(std::move(node));
    }

    for (int i = 0; i < N; i++) {
        std::vector<RaftNode*> peers;
        for (int j = 0; j < N; j++) if (j != i) peers.push_back(nodes[j].get());
        nodes[i]->setPeers(peers);
    }

    for (int i = 0; i < N; i++) nodes[i]->start();

    std::cout << "Waiting for leader election..." << std::endl;
    int leader = waitForLeader(nodes);
    if (leader < 0) {
        std::cerr << "ERROR: no leader elected!" << std::endl;
        return 1;
    }
    std::cout << "Leader: node " << leader << std::endl;

    // Submit some commands
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"name", "raft"},
        {"version", "1.0"},
        {"author", "albert"},
        {"status", "running"},
    };

    int lastIdx = 0;
    for (auto& [k, v] : kvs) {
        int idx, term;
        std::string cmd = "SET " + k + " " + v;
        if (nodes[leader]->submit(cmd, idx, term)) {
            std::cout << "Submitted: " << cmd << " (index=" << idx << ")" << std::endl;
            lastIdx = idx;
        }
    }

    // Wait for all nodes to commit
    for (auto& n : nodes) {
        if (!waitForCommit(n.get(), lastIdx, 3000)) {
            std::cerr << "WARNING: node " << n->getId() << " didn't commit in time" << std::endl;
        }
    }

    // Read back values
    std::cout << "\nReading values from all nodes:" << std::endl;
    for (auto& [k, v] : kvs) {
        std::cout << "  key=" << k << ": ";
        for (auto& n : nodes) {
            std::cout << "[node" << n->getId() << "=" << n->getValue(k) << "] ";
        }
        std::cout << std::endl;
    }

    // Demo: kill leader, new leader elected, write more
    std::cout << "\nKilling leader node " << leader << "..." << std::endl;
    nodes[leader]->kill();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    int newLeader = waitForLeader(nodes);
    if (newLeader >= 0) {
        std::cout << "New leader: node " << newLeader << std::endl;
        int idx, term;
        nodes[newLeader]->submit("SET new_leader yes", idx, term);
        for (int i = 0; i < N; i++) {
            if (i != leader) waitForCommit(nodes[i].get(), idx, 2000);
        }
        std::cout << "Wrote 'new_leader=yes' via new leader" << std::endl;
    }

    // Cleanup
    for (auto& n : nodes) {
        if (n->getState() != NodeState::FOLLOWER || true) n->kill();
    }
    std::cout << "\nDemo complete." << std::endl;
    return 0;
}
