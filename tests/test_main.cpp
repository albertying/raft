#include <iostream>
#include <string>
#include <functional>
#include <vector>

// Simple test runner - no external dependencies
struct Test {
    std::string name;
    std::function<void()> fn;
};

static std::vector<Test>& getTests() {
    static std::vector<Test> tests;
    return tests;
}

void registerTest(const std::string& name, std::function<void()> fn) {
    getTests().push_back({name, fn});
}

int main() {
    int passed = 0, failed = 0;
    for (auto& t : getTests()) {
        try {
            t.fn();
            std::cout << "[PASS] " << t.name << std::endl;
            passed++;
        } catch (const std::exception& e) {
            std::cout << "[FAIL] " << t.name << ": " << e.what() << std::endl;
            failed++;
        } catch (...) {
            std::cout << "[FAIL] " << t.name << ": unknown exception" << std::endl;
            failed++;
        }
    }
    std::cout << "\n" << passed << " passed, " << failed << " failed" << std::endl;
    return failed > 0 ? 1 : 0;
}
