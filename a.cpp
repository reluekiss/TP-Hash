#include <iostream>
#include <cstdlib>
#include <ctime>
#include <string>
#include <vector>
#include <chrono>
#include <boost/unordered_map.hpp>

#define NOPS 10000000

std::string random_string(size_t length) {
    const std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::string result(length, '\0');
    for (size_t i = 0; i < length; i++)
        result[i] = charset[rand() % charset.size()];
    return result;
}

struct my_type_t {
    int num;
    std::string str;
};

int main() {
    srand(static_cast<unsigned>(time(nullptr)));
    using clock_t = std::chrono::high_resolution_clock;
    using ns = std::chrono::nanoseconds;

    std::vector<std::string> inserted_keys;
    inserted_keys.reserve(NOPS);

    boost::unordered_map<std::string, my_type_t> map;

    ns total_insert(0), total_lookup(0), total_delete(0);

    // Insertion profiling
    for (size_t op = 0; op < NOPS; op++) {
        std::string key = random_string(10);
        my_type_t value { rand(), random_string(15) };
        inserted_keys.push_back(key);

        auto start = clock_t::now();
        map.emplace(key, value);
        auto end = clock_t::now();
        total_insert += std::chrono::duration_cast<ns>(end - start);
    }

    // Lookup profiling
    for (size_t op = 0; op < NOPS; op++) {
        const std::string &key = inserted_keys[rand() % inserted_keys.size()];

        auto start = clock_t::now();
        auto it = map.find(key);
        auto end = clock_t::now();
        total_lookup += std::chrono::duration_cast<ns>(end - start);

        if (it == map.end())
            std::cerr << "lookup failed for key: " << key << "\n";
    }

    // Deletion profiling
    for (size_t op = 0; op < NOPS; op++) {
        const std::string &key = inserted_keys[rand() % inserted_keys.size()];

        auto start = clock_t::now();
        map.erase(key);
        auto end = clock_t::now();
        total_delete += std::chrono::duration_cast<ns>(end - start);
    }

    std::cout << "Boost Unordered Map Profiling Completed:\n";
    std::cout << "  Avg insert: " << total_insert.count() / double(NOPS) << " ns\n";
    std::cout << "  Avg lookup: " << total_lookup.count() / double(NOPS) << " ns\n";
    std::cout << "  Avg delete: " << total_delete.count() / double(NOPS) << " ns\n";

    return 0;
}
