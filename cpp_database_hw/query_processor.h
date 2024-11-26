#pragma once
#include <string>

class Database; // Предварительное объявление

class QueryProcessor {
public:
    static std::string parse_and_execute(Database& db, const std::string& query);
};
