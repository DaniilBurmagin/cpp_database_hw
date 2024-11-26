#include "index.h"
#include <stdexcept>
#include <algorithm>
#include <typeinfo>

void Index::add_entry(const std::any& key, size_t row_index) {
    if (key.type() == typeid(int)) {
        int value = std::any_cast<int>(key);
        int_index_data[value].push_back(row_index);
    }
    else if (key.type() == typeid(std::string)) {
        std::string value = std::any_cast<std::string>(key);
        string_index_data[value].push_back(row_index);
    }
    else {
        throw std::invalid_argument("Unsupported key type for indexing.");
    }
}

std::vector<size_t> Index::find(const std::any& key) const {
    if (key.type() == typeid(int)) {
        int value = std::any_cast<int>(key);
        if (int_index_data.find(value) != int_index_data.end()) {
            return int_index_data.at(value);
        }
    }
    else if (key.type() == typeid(std::string)) {
        std::string value = std::any_cast<std::string>(key);
        if (string_index_data.find(value) != string_index_data.end()) {
            return string_index_data.at(value);
        }
    }
    return {};
}

void Index::remove_entry(const std::any& key, size_t row_index) {
    if (key.type() == typeid(int)) {
        int value = std::any_cast<int>(key);
        auto it = int_index_data.find(value);
        if (it != int_index_data.end()) {
            auto& vec = it->second;
            vec.erase(std::remove(vec.begin(), vec.end(), row_index), vec.end());
            if (vec.empty()) {
                int_index_data.erase(it);
            }
        }
    }
    else if (key.type() == typeid(std::string)) {
        std::string value = std::any_cast<std::string>(key);
        auto it = string_index_data.find(value);
        if (it != string_index_data.end()) {
            auto& vec = it->second;
            vec.erase(std::remove(vec.begin(), vec.end(), row_index), vec.end());
            if (vec.empty()) {
                string_index_data.erase(it);
            }
        }
    }
}
