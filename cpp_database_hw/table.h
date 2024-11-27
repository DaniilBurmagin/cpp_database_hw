#ifndef TABLE_H
#define TABLE_H

#include <map>
#include <vector>
#include <string>
#include <any>
#include <functional>
#include <memory>
#include <iostream>

class Index {
public:
    void add_entry(const std::any& value, size_t row_index);
};

class Table {
public:
    Table(const std::map<std::string, std::string>& schema);
    Table() = default;

    void insert(const std::map<std::string, std::any>& values);
    void remove(const std::string& condition);
    void update(const std::string& condition, const std::map<std::string, std::any>& updates);
    std::vector<std::map<std::string, std::any>> select(const std::string& condition) const;
    bool is_unique(const std::string& column_name, const std::any& value) const;

    void create_index(const std::string& column);
    void auto_index(const std::string& column);

    void save(std::ostream& os) const;
    void load(std::istream& is);
    std::shared_ptr<Table> clone() const;

private:
    std::vector<std::string> columns;
    std::map<std::string, std::string> column_types;
    std::vector<std::vector<std::any>> rows;
    std::map<std::string, Index> indices;
    std::map<std::string, std::string> constraints;

    std::function<bool(const std::map<std::string, std::any>&)> parse_condition(const std::string& condition) const;
    std::function<bool(const std::map<std::string, std::any>&)> parse_simple_condition(const std::string& condition) const;
};

#endif // TABLE_H
