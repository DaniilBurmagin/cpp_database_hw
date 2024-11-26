#pragma once
#include <map>
#include <string>
#include <memory>
#include <vector>
#include "table.h"

class Database {
private:
    std::map<std::string, std::shared_ptr<Table>> tables;
    std::vector<std::map<std::string, std::shared_ptr<Table>>> transaction_stack;

public:
    void create_table(const std::string& name, const std::map<std::string, std::string>& schema);
    std::string execute(const std::string& query);
    void save_to_file(const std::string& filename) const;
    void load_from_file(const std::string& filename);

    void begin_transaction();
    void commit_transaction();
    void rollback_transaction();

    Table* get_table(const std::string& name) const;
};
