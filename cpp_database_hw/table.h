#pragma once
#include <vector>
#include <string>
#include <map>
#include <any>
#include <functional>
#include <ostream>
#include <istream>
#include "index.h"

class Table {
    std::vector<std::string> columns;
    std::map<std::string, std::string> column_types;
    std::vector<std::vector<std::any>> rows;
    std::map<std::string, Index> indices;
    std::map<std::string, std::string> constraints;

public:
    Table() = default;
    Table(const std::map<std::string, std::string>& schema);

    std::shared_ptr<Table> clone() const;
    void auto_index(const std::string& column);
    void insert(const std::map<std::string, std::any>& values);
    std::vector<std::map<std::string, std::any>> select(const std::string& condition) const;
    void update(const std::string& condition, const std::map<std::string, std::any>& updates);
    void remove(const std::string& condition);
    void create_index(const std::string& column);

    void save(std::ostream& os) const;
    void load(std::istream& is);

private:
    std::function<bool(const std::map<std::string, std::any>&)> parse_condition(const std::string& condition) const;
    std::function<bool(const std::map<std::string, std::any>&)> parse_simple_condition(const std::string& condition) const;
};
