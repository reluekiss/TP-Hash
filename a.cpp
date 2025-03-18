#include <iostream>
#include <cstdlib>
#include <ctime>
#include <string>
#include <vector>
#include <chrono>
#include "flat_hash_map.hpp"  // include the library header

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

    std::vector<std::string> inserted_keys;
    inserted_keys.reserve(NOPS);

    ska::flat_hash_map<std::string, my_type_t> map;

    // Insertion profiling
    auto start = clock_t::now();
    for (size_t op = 0; op < NOPS; op++) {
        std::string key = random_string(10);
        my_type_t value { rand(), random_string(15) };
        map.emplace(key, value);
        inserted_keys.push_back(key);
    }
    auto end = clock_t::now();
    double avg_insert = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() / double(NOPS);

    // Lookup profiling
    start = clock_t::now();
    for (size_t op = 0; op < NOPS; op++) {
        const std::string &key = inserted_keys[rand() % inserted_keys.size()];
        if (map.find(key) == map.end())
            std::cerr << "lookup failed for key: " << key << "\n";
    }
    end = clock_t::now();
    double avg_lookup = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() / double(NOPS);

    // Deletion profiling
    start = clock_t::now();
    for (size_t op = 0; op < NOPS; op++) {
        const std::string &key = inserted_keys[rand() % inserted_keys.size()];
        map.erase(key);
    }
    end = clock_t::now();
    double avg_delete = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() / double(NOPS);

    std::cout << "Flat Hash Map Profiling Completed:\n";
    std::cout << "  Avg insert: " << avg_insert << " ns\n";
    std::cout << "  Avg lookup: " << avg_lookup << " ns\n";
    std::cout << "  Avg delete: " << avg_delete << " ns\n";

    return 0;
}

