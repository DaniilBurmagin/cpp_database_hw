#include "database.h"
#include "query_processor.h"
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <iostream>

void Database::create_table(const std::string& name, const std::map<std::string, std::string>& schema) {
    tables[name] = std::make_shared<Table>(schema);
}

std::string Database::execute(const std::string& query) {
    return QueryProcessor::parse_and_execute(*this, query);
}

void Database::save_to_file(const std::string& filename) const {
    std::ofstream file(filename, std::ios::binary);
    if (!file) throw std::runtime_error("Cannot open file for saving.");

    for (const auto& [name, table] : tables) {
        file << name << "\n";
        table->save(file);
    }
}

void Database::load_from_file(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file) throw std::runtime_error("Cannot open file for loading.");

    std::string name;
    while (std::getline(file, name)) {
        auto table = std::make_unique<Table>();
        table->load(file);
        tables[name] = std::move(table);
    }
}

Table* Database::get_table(const std::string& name) const {
    auto it = tables.find(name);
    if (it == tables.end()) return nullptr;
    return it->second.get();
}

void Database::begin_transaction() {
    std::map<std::string, std::shared_ptr<Table>> snapshot = tables;
    transaction_stack.push_back(snapshot);
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

