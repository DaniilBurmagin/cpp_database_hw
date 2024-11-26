#include "table.h"
#include <stdexcept>
#include <sstream>
#include <functional>
#include <iostream>

Table::Table(const std::map<std::string, std::string>& schema) {
    for (const auto& [col_name, col_type] : schema) {
        columns.push_back(col_name);
        column_types[col_name] = col_type;
    }
}

void Table::save(std::ostream& os) const {
    os << columns.size() << "\n"; // Количество столбцов
    for (const auto& col : columns) {
        os << col << " " << column_types.at(col) << "\n"; // Имена и типы столбцов
    }

    os << rows.size() << "\n"; // Количество строк
    for (size_t i = 0; i < rows.size(); ++i) {
        for (size_t j = 0; j < rows[i].size(); ++j) {
            const auto& cell = rows[i][j];
            if (cell.has_value()) {
                if (cell.type() == typeid(int)) {
                    os << "int " << std::any_cast<int>(cell);
                }
                else if (cell.type() == typeid(std::string)) {
                    os << "string " << std::any_cast<std::string>(cell);
                }
                else if (cell.type() == typeid(bool)) {
                    os << "bool " << (std::any_cast<bool>(cell) ? "true" : "false");
                }
                else {
                    os << "null";
                }
            }
            else {
                os << "null";
            }
            if (j < rows[i].size() - 1) {
                os << " "; // Разделитель между столбцами
            }
        }
        if (i < rows.size() - 1) {
            os << "\n"; // Перевод строки между строками данных
        }
    }
}


void Table::load(std::istream& is) {
    size_t col_count;
    std::string line;


    while (std::getline(is, line) && line.empty()) {

    }
    if (line.empty()) {
        throw std::runtime_error("Failed to read column count line.");
    }
    line.erase(0, line.find_first_not_of(" \t"));
    line.erase(line.find_last_not_of(" \t") + 1);
    std::cout << "Raw column count line: [" << line << "]" << std::endl;

    try {
        col_count = std::stoul(line);
    }
    catch (const std::exception& e) {
        throw std::runtime_error("Invalid column count: " + std::string(e.what()));
    }

    if (col_count > 1000) {
        throw std::runtime_error("Invalid column count or corrupted data.");
    }
    std::cout << "Columns to load: " << col_count << std::endl;

    // Загружаем столбцы
    columns.resize(col_count);
    for (size_t i = 0; i < col_count; ++i) {
        std::string col_name, col_type;
        if (!(is >> col_name >> col_type)) {
            throw std::runtime_error("Failed to read column name or type.");
        }
        columns[i] = col_name;
        column_types[col_name] = col_type;
        std::cout << "Column loaded: " << col_name << ", Type: " << col_type << std::endl;
    }
    is.ignore(std::numeric_limits<std::streamsize>::max(), '\n');


    while (std::getline(is, line) && line.empty()) {

    }
    if (line.empty()) {
        throw std::runtime_error("Failed to read row count line.");
    }
    std::cout << "Raw row count line: " << line << std::endl;

    size_t row_count;
    try {
        row_count = std::stoul(line);
    }
    catch (const std::exception& e) {
        throw std::runtime_error("Invalid row count: " + std::string(e.what()));
    }

    if (row_count > 100000) {
        throw std::runtime_error("Row count exceeds reasonable limit. Data might be corrupted.");
    }
    std::cout << "Rows to load: " << row_count << std::endl;

    rows.resize(row_count);
    for (size_t i = 0; i < row_count; ++i) {
        rows[i].resize(columns.size());
        for (size_t j = 0; j < columns.size(); ++j) {
            std::string type, value;
            if (!(is >> type >> value)) {
                throw std::runtime_error("Failed to read cell data at row " + std::to_string(i) + ", column " + std::to_string(j));
            }

            try {
                if (type == "null" || type == "NULL") {
                    rows[i][j] = std::any();
                }
                else if (type == "int") {
                    if (value.empty()) {
                        throw std::runtime_error("Invalid integer value at row " + std::to_string(i) + ", column " + std::to_string(j));
                    }
                    rows[i][j] = std::stoi(value);
                }
                else if (type == "string") {
                    rows[i][j] = value;
                }
                else if (type == "bool") {
                    rows[i][j] = (value == "true");
                }
                else {
                    throw std::runtime_error("Unknown data type: " + type);
                }
                std::cout << "Cell loaded: (" << i << ", " << j << ") = " << type << " " << value << std::endl;
            }
            catch (const std::exception& e) {
                throw std::runtime_error("Error parsing cell at row " + std::to_string(i) + ", column " + std::to_string(j) +
                    ": " + std::string(e.what()));
            }
        }
        is.ignore(std::numeric_limits<std::streamsize>::max(), '\n'); // Удаляем остаток строки
    }
}

std::vector<std::map<std::string, std::any>> Table::select(const std::string& condition) const {
    std::vector<std::map<std::string, std::any>> result;
    auto condition_fn = parse_condition(condition);
    for (const auto& row : rows) {
        std::map<std::string, std::any> mapped_row;
        for (size_t i = 0; i < columns.size(); ++i) {
            mapped_row[columns[i]] = row[i];
        }
        if (condition_fn(mapped_row)) {
            result.push_back(mapped_row);
        }
    }
    return result;
}

void Table::update(const std::string& condition, const std::map<std::string, std::any>& updates) {
    auto condition_fn = parse_condition(condition);
    for (auto& row : rows) {
        std::map<std::string, std::any> mapped_row;
        for (size_t i = 0; i < columns.size(); ++i) {
            mapped_row[columns[i]] = row[i];
        }
        if (condition_fn(mapped_row)) {
            for (const auto& [col_name, new_value] : updates) {
                auto it = std::find(columns.begin(), columns.end(), col_name);
                if (it != columns.end()) {
                    size_t col_index = std::distance(columns.begin(), it);
                    row[col_index] = new_value;
                }
            }
        }
    }
}

void Table::remove(const std::string& condition) {
    auto condition_fn = parse_condition(condition);
    rows.erase(std::remove_if(rows.begin(), rows.end(), [&](const std::vector<std::any>& row) {
        std::map<std::string, std::any> mapped_row;
        for (size_t i = 0; i < columns.size(); ++i) {
            mapped_row[columns[i]] = row[i];
        }
        return condition_fn(mapped_row);
        }), rows.end());
}


void Table::create_index(const std::string& column) {
    if (std::find(columns.begin(), columns.end(), column) == columns.end()) {
        throw std::runtime_error("Column " + column + " not found.");
    }
    indices[column] = Index();
    for (size_t i = 0; i < rows.size(); ++i) {
        size_t col_index = std::distance(columns.begin(), std::find(columns.begin(), columns.end(), column));
        indices[column].add_entry(rows[i][col_index], i);
    }
}

void Table::auto_index(const std::string& column) {
    if (indices.find(column) == indices.end()) {
        create_index(column);
        std::cout << "Auto index created for column: " << column << std::endl;
    }
}

void Table::insert(const std::map<std::string, std::any>& values) {
    std::vector<std::any> row(columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
        const auto& col_name = columns[i];
        if (values.find(col_name) != values.end() && values.at(col_name).has_value()) {
            std::cout << "Inserting value for column: " << col_name << ", Value type: "
                << values.at(col_name).type().name() << std::endl;

            if (constraints[col_name] == "NOT NULL" && !values.at(col_name).has_value()) {
                throw std::runtime_error("Column '" + col_name + "' cannot be NULL.");
            }
            row[i] = values.at(col_name);
        }
        else {
            std::cout << "Inserting default (NULL) value for column: " << col_name << std::endl;
            if (constraints[col_name] == "NOT NULL") {
                throw std::runtime_error("Column '" + col_name + "' cannot be NULL.");
            }
            row[i] = std::any();
        }
    }
    rows.push_back(row);
}


std::shared_ptr<Table> Table::clone() const {
    auto new_table = std::make_shared<Table>();
    new_table->columns = this->columns;
    new_table->column_types = this->column_types;
    new_table->rows = this->rows;
    new_table->indices = this->indices;
    new_table->constraints = this->constraints;
    return new_table;
}

std::function<bool(const std::map<std::string, std::any>&)> Table::parse_simple_condition(const std::string& condition) const {
    auto equals_pos = condition.find('=');
    if (equals_pos == std::string::npos) {
        throw std::runtime_error("Syntax error in condition: " + condition);
    }

    std::string col_name = condition.substr(0, equals_pos);
    std::string col_value = condition.substr(equals_pos + 1);

    col_name.erase(0, col_name.find_first_not_of(" \t"));
    col_name.erase(col_name.find_last_not_of(" \t") + 1);
    col_value.erase(0, col_value.find_first_not_of(" \t"));
    col_value.erase(col_value.find_last_not_of(" \t") + 1);

    if (col_value == "NULL" || col_value == "null") {
        return [col_name](const std::map<std::string, std::any>& row) {
            return !row.at(col_name).has_value();
            };
    }
    else if (col_value[0] == '\'') {
        col_value = col_value.substr(1, col_value.size() - 2);
        return [col_name, col_value](const std::map<std::string, std::any>& row) {
            return row.at(col_name).type() == typeid(std::string) && std::any_cast<std::string>(row.at(col_name)) == col_value;
            };
    }
    else if (col_value == "true" || col_value == "false") {
        bool bool_value = (col_value == "true");
        return [col_name, bool_value](const std::map<std::string, std::any>& row) {
            return row.at(col_name).type() == typeid(bool) && std::any_cast<bool>(row.at(col_name)) == bool_value;
            };
    }
    else {
        int int_value = std::stoi(col_value);
        return [col_name, int_value](const std::map<std::string, std::any>& row) {
            return row.at(col_name).type() == typeid(int) && std::any_cast<int>(row.at(col_name)) == int_value;
            };
    }
}

std::function<bool(const std::map<std::string, std::any>&)> Table::parse_condition(const std::string& condition) const {
    if (condition.empty() || condition == "true") {
        return [](const std::map<std::string, std::any>&) { return true; };
    }

    auto parse_logical = [&](const std::string& cond) -> std::function<bool(const std::map<std::string, std::any>&)> {
        size_t and_pos = cond.find(" AND ");
        size_t or_pos = cond.find(" OR ");
        size_t not_pos = cond.find("NOT ");

        if (and_pos != std::string::npos) {
            std::string left = cond.substr(0, and_pos);
            std::string right = cond.substr(and_pos + 5);
            return [this, left, right](const std::map<std::string, std::any>& row) {
                return parse_condition(left)(row) && parse_condition(right)(row);
                };
        }
        else if (or_pos != std::string::npos) {
            std::string left = cond.substr(0, or_pos);
            std::string right = cond.substr(or_pos + 4);
            return [this, left, right](const std::map<std::string, std::any>& row) {
                return parse_condition(left)(row) || parse_condition(right)(row);
                };
        }
        else if (not_pos == 0) {
            std::string inner = cond.substr(4);
            return [this, inner](const std::map<std::string, std::any>& row) {
                return !parse_condition(inner)(row);
                };
        }

        return parse_simple_condition(cond);
        };

    return parse_logical(condition);
}



