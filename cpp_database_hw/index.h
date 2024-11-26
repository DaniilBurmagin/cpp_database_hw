#pragma once
#include <unordered_map>
#include <vector>
#include <any>
#include <string>

class Index {
private:
    std::unordered_map<std::string, std::vector<size_t>> string_index_data;
    std::unordered_map<int, std::vector<size_t>> int_index_data;

public:
    void add_entry(const std::any& key, size_t row_index);

    std::vector<size_t> find(const std::any& key) const;

    void remove_entry(const std::any& key, size_t row_index);
};
