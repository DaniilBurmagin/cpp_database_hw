#include "database.h"
#include "query_processor.h"
#include <fstream>
#include <stdexcept>
#include <sstream>
#include <iostream>

void Database::create_table(const std::string& name, const std::map<std::string, std::string>& schema) {
    if (tables.find(name) != tables.end()) {
        throw std::runtime_error("Table already exists: " + name);
    }
    tables[name] = std::make_shared<Table>(schema);
}

Table* Database::get_table(const std::string& name) {
    if (tables.find(name) == tables.end()) {
        return nullptr;
    }
    return tables[name].get();
}

std::string Database::execute(const std::string& query) {
    QueryProcessor processor;
    return processor.parse_and_execute(*this, query);
}

void Database::save_to_file(const std::string& filename) const {
    std::ofstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file for saving: " + filename);
    }

    file << tables.size() << "\n";
    for (const auto& [name, table] : tables) {
        file << name << "\n";
        table->save(file);
    }
}

void Database::load_from_file(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file for loading: " + filename);
    }

    size_t table_count;
    file >> table_count;
    file.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

    tables.clear();
    for (size_t i = 0; i < table_count; ++i) {
        std::string name;
        std::getline(file, name);
        auto table = std::make_shared<Table>();
        table->load(file);
        tables[name] = table;
    }
}

void Database::begin_transaction() {
    transaction_stack.push_back(tables);
    std::cout << "Transaction started.\n";
}

void Database::rollback_transaction() {
    if (transaction_stack.empty()) {
        throw std::runtime_error("No active transaction to rollback.");
    }
    tables = transaction_stack.back();
    transaction_stack.pop_back();
    std::cout << "Transaction rolled back.\n";
}

void Database::commit_transaction() {
    if (transaction_stack.empty()) {
        throw std::runtime_error("No active transaction to commit.");
    }
    transaction_stack.pop_back();
    std::cout << "Transaction committed.\n";
}
