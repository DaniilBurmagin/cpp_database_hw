#include "table.h"
#include <stdexcept>
#include <sstream>
#include <algorithm>
#include <functional>
#include <iostream>
#include "utils.h"


// Конструктор таблицы
Table::Table(const std::map<std::string, std::string>& schema) {
    for (const auto& [col_name, col_type] : schema) {
        std::string clean_col_name = trim(col_name);
        std::string clean_col_type = trim(col_type);
        if (clean_col_name.empty() || clean_col_type.empty()) {
            throw std::runtime_error("Schema contains empty column name or type.");
        }
        columns.push_back(clean_col_name);
        column_types[clean_col_name] = clean_col_type;
    }
}

// Реализация метода is_unique
bool Table::is_unique(const std::string& column_name, const std::any& value) const {
    auto it = std::find(columns.begin(), columns.end(), column_name);
    if (it == columns.end()) {
        throw std::runtime_error("Column '" + column_name + "' not found.");
    }

    size_t column_index = std::distance(columns.begin(), it);
    for (const auto& row : rows) {
        if (row[column_index].has_value()) {
            if (row[column_index].type() == value.type()) {
                if (row[column_index].type() == typeid(int)) {
                    if (std::any_cast<int>(row[column_index]) == std::any_cast<int>(value)) {
                        return false; // Значение не уникально
                    }
                }
                else if (row[column_index].type() == typeid(std::string)) {
                    if (std::any_cast<std::string>(row[column_index]) == std::any_cast<std::string>(value)) {
                        return false; // Значение не уникально
                    }
                }
                else if (row[column_index].type() == typeid(bool)) {
                    if (std::any_cast<bool>(row[column_index]) == std::any_cast<bool>(value)) {
                        return false; // Значение не уникально
                    }
                }
                else {
                    throw std::runtime_error("Unsupported type for uniqueness check.");
                }
            }
        }
    }

    return true; // Значение уникально
}


// Сохранение таблицы
void Table::save(std::ostream& os) const {
    if (columns.empty()) {
        throw std::runtime_error("Cannot save: no columns defined.");
    }

    os << columns.size() << "\n";
    for (const auto& col : columns) {
        os << col << " " << column_types.at(col) << "\n";
    }

    os << rows.size() << "\n";
    for (const auto& row : rows) {
        for (size_t j = 0; j < row.size(); ++j) {
            const auto& cell = row[j];
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
            if (j < row.size() - 1) os << " ";
        }
        os << "\n";
    }
}


// Загрузка таблицы
void Table::load(std::istream& is) {
    std::string line;

    // Чтение количества столбцов
    while (std::getline(is, line) && line.empty()) {}
    line = trim(line);
    if (line.empty()) throw std::runtime_error("Column count line is empty.");

    size_t col_count = 0;
    try {
        col_count = std::stoul(line);
    }
    catch (...) {
        throw std::runtime_error("Invalid column count: " + line);
    }
    if (col_count == 0 || col_count > 1000) {
        throw std::runtime_error("Column count out of valid range.");
    }

    // Чтение схемы столбцов
    columns.clear();
    column_types.clear();
    for (size_t i = 0; i < col_count; ++i) {
        std::string col_name, col_type;
        if (!(is >> col_name >> col_type)) {
            throw std::runtime_error("Failed to read column schema.");
        }
        col_name = trim(col_name);
        col_type = trim(col_type);
        if (col_name.empty() || col_type.empty()) {
            throw std::runtime_error("Column name or type is empty.");
        }
        columns.push_back(col_name);
        column_types[col_name] = col_type;
    }
    is.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

    // Чтение количества строк
    while (std::getline(is, line) && line.empty()) {}
    line = trim(line);
    if (line.empty()) throw std::runtime_error("Row count line is empty.");

    size_t row_count = 0;
    try {
        row_count = std::stoul(line);
    }
    catch (...) {
        throw std::runtime_error("Invalid row count: " + line);
    }
    if (row_count > 100000) {
        throw std::runtime_error("Row count exceeds reasonable limit.");
    }

    // Чтение строк данных
    rows.clear();
    rows.resize(row_count);
    for (size_t i = 0; i < row_count; ++i) {
        rows[i].resize(columns.size());
        for (size_t j = 0; j < columns.size(); ++j) {
            std::string type, value;
            if (!(is >> type >> value)) {
                throw std::runtime_error("Failed to read cell data at row " + std::to_string(i) + ", column " + std::to_string(j));
            }
            type = trim(type);
            value = trim(value);
            try {
                if (type == "null") {
                    rows[i][j] = std::any();
                }
                else if (type == "int") {
                    if (!is_numeric(value)) {
                        throw std::runtime_error("Invalid integer value: " + value);
                    }
                    rows[i][j] = std::stoi(value);
                }
                else if (type == "string") {
                    rows[i][j] = value;
                }
                else if (type == "bool") {
                    if (value != "true" && value != "false") {
                        throw std::runtime_error("Invalid boolean value: " + value);
                    }
                    rows[i][j] = (value == "true");
                }
                else {
                    throw std::runtime_error("Unknown type: " + type);
                }
            }
            catch (const std::exception& e) {
                throw std::runtime_error("Error parsing cell at row " + std::to_string(i) + ", column " + std::to_string(j) + ": " + e.what());
            }
        }
        is.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
}

std::vector<std::map<std::string, std::any>> Table::select(const std::string& condition) const {
    std::vector<std::map<std::string, std::any>> result;
    auto condition_fn = parse_condition(condition);

    for (const auto& row : rows) {
        std::map<std::string, std::any> mapped_row;
        for (size_t i = 0; i < columns.size(); ++i) {
            try {
                if (row[i].type() == typeid(int)) {
                    mapped_row[columns[i]] = std::any_cast<int>(row[i]);
                }
                else if (row[i].type() == typeid(std::string)) {
                    mapped_row[columns[i]] = std::any_cast<std::string>(row[i]);
                }
                else if (row[i].type() == typeid(bool)) {
                    mapped_row[columns[i]] = std::any_cast<bool>(row[i]);
                }
                else {
                    mapped_row[columns[i]] = std::any(); // NULL value
                }
            }
            catch (const std::exception& e) {
                throw std::runtime_error("Error processing row value for column '" + columns[i] + "': " + e.what());
            }
        }
        if (condition_fn(mapped_row)) {
            result.push_back(mapped_row);
        }
    }
    return result;
}



void Table::update(const std::string& condition, const std::map<std::string, std::any>& updates) {
    std::cout << "Updating rows with condition: " << condition << "\n";
    auto condition_fn = parse_condition(condition);

    for (auto& row : rows) {
        std::map<std::string, std::any> mapped_row;
        for (size_t i = 0; i < columns.size(); ++i) {
            mapped_row[columns[i]] = row[i];
        }

        // Проверяем, подходит ли строка под условие
        if (condition_fn(mapped_row)) {
            std::cout << "Row matches condition. Updating...\n";
            for (const auto& [col_name, new_value] : updates) {
                auto it = std::find(columns.begin(), columns.end(), col_name);
                if (it == columns.end()) {
                    throw std::runtime_error("Column '" + col_name + "' not found for update.");
                }

                size_t col_index = std::distance(columns.begin(), it);
                const std::string& col_type = column_types[col_name];
                std::cout << "Updating column '" << col_name << "' of type '" << col_type << "'\n";

                try {
                    if (!new_value.has_value()) {
                        row[col_index] = std::any();
                        std::cout << "Set column '" << col_name << "' to NULL.\n";
                    }
                    else if (col_type == "int32") {
                        if (new_value.type() != typeid(int)) {
                            throw std::runtime_error("Type mismatch: expected int32.");
                        }
                        row[col_index] = std::any_cast<int>(new_value);
                        std::cout << "Updated column '" << col_name << "' to value: " << std::any_cast<int>(new_value) << "\n";
                    }
                    else if (col_type == "string") {
                        if (new_value.type() != typeid(std::string)) {
                            throw std::runtime_error("Type mismatch: expected string.");
                        }
                        row[col_index] = std::any_cast<std::string>(new_value);
                        std::cout << "Updated column '" << col_name << "' to value: " << std::any_cast<std::string>(new_value) << "\n";
                    }
                    else if (col_type == "bool") {
                        if (new_value.type() != typeid(bool)) {
                            throw std::runtime_error("Type mismatch: expected bool.");
                        }
                        row[col_index] = std::any_cast<bool>(new_value);
                        std::cout << "Updated column '" << col_name << "' to value: " << (std::any_cast<bool>(new_value) ? "true" : "false") << "\n";
                    }
                    else {
                        throw std::runtime_error("Unsupported column type: " + col_type);
                    }
                }
                catch (const std::exception& e) {
                    throw std::runtime_error("Error updating column '" + col_name + "': " + e.what());
                }
            }
        }
    }
}


void Table::remove(const std::string& condition) {
    // Парсим условие
    auto condition_fn = parse_condition(condition);

    // Лямбда-функция для проверки условия
    auto match_condition = [&](const std::vector<std::any>& row) {
        std::map<std::string, std::any> mapped_row;
        for (size_t i = 0; i < columns.size(); ++i) {
            mapped_row[columns[i]] = row[i];
        }
        try {
            return condition_fn(mapped_row);
        }
        catch (const std::exception& e) {
            throw std::runtime_error("Error evaluating condition: " + std::string(e.what()));
        }
        };

    // Подсчитываем строки до удаления
    size_t initial_size = rows.size();

    // Удаление строк, которые удовлетворяют условию
    auto new_end = std::remove_if(rows.begin(), rows.end(), match_condition);
    size_t removed_count = std::distance(new_end, rows.end());
    rows.erase(new_end, rows.end());

    // Логируем результат
    if (removed_count > 0) {
        std::cout << "Removed " << removed_count << " row(s) matching condition: " << condition << "\n";
    }
    else {
        std::cout << "No rows matched the condition: " << condition << "\n";
    }

    // Проверка после удаления
    if (rows.size() >= initial_size) {
        std::cerr << "Warning: No rows were removed, check the condition syntax.\n";
    }
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




std::function<bool(const std::map<std::string, std::any>&)> Table::parse_condition(const std::string& condition) const {
    std::cout << "Parsing condition: " << condition << "\n";
    std::string trimmed_condition = trim(condition);

    // Условие "true" или "false" означает, что оно всегда истинно/ложно
    if (trimmed_condition == "true") {
        return [](const std::map<std::string, std::any>&) { return true; };
    }
    if (trimmed_condition == "false") {
        return [](const std::map<std::string, std::any>&) { return false; };
    }

    // Обработка логических операторов AND и OR
    if (trimmed_condition.find(" AND ") != std::string::npos) {
        size_t pos = trimmed_condition.find(" AND ");
        std::string left = trimmed_condition.substr(0, pos);
        std::string right = trimmed_condition.substr(pos + 5);
        return [this, left, right](const std::map<std::string, std::any>& row) {
            return parse_condition(left)(row) && parse_condition(right)(row);
            };
    }
    if (trimmed_condition.find(" OR ") != std::string::npos) {
        size_t pos = trimmed_condition.find(" OR ");
        std::string left = trimmed_condition.substr(0, pos);
        std::string right = trimmed_condition.substr(pos + 4);
        return [this, left, right](const std::map<std::string, std::any>& row) {
            return parse_condition(left)(row) || parse_condition(right)(row);
            };
    }

    // Обработка NOT
    if (trimmed_condition.find("NOT ") == 0) {
        std::string inner = trimmed_condition.substr(4);
        return [this, inner](const std::map<std::string, std::any>& row) {
            return !parse_condition(inner)(row);
            };
    }

    // Простое условие (например, "name='Alice'")
    return parse_simple_condition(trimmed_condition);
}




std::function<bool(const std::map<std::string, std::any>&)> Table::parse_condition(const std::string& condition) const {
    std::cout << "Parsing condition: " << condition << "\n";
    std::string trimmed_condition = trim(condition);

    if (trimmed_condition == "true") {
        return [](const std::map<std::string, std::any>&) { return true; };
    }
    if (trimmed_condition == "false") {
        return [](const std::map<std::string, std::any>&) { return false; };
    }

    if (trimmed_condition.find(" AND ") != std::string::npos) {
        size_t pos = trimmed_condition.find(" AND ");
        std::string left = trimmed_condition.substr(0, pos);
        std::string right = trimmed_condition.substr(pos + 5);
        return [this, left, right](const std::map<std::string, std::any>& row) {
            return parse_condition(left)(row) && parse_condition(right)(row);
            };
    }

    if (trimmed_condition.find(" OR ") != std::string::npos) {
        size_t pos = trimmed_condition.find(" OR ");
        std::string left = trimmed_condition.substr(0, pos);
        std::string right = trimmed_condition.substr(pos + 4);
        return [this, left, right](const std::map<std::string, std::any>& row) {
            return parse_condition(left)(row) || parse_condition(right)(row);
            };
    }

    size_t equals_pos = trimmed_condition.find('=');
    if (equals_pos == std::string::npos) {
        throw std::runtime_error("Syntax error in condition: " + condition);
    }

    std::string col_name = trim(trimmed_condition.substr(0, equals_pos));
    std::string col_value = trim(trimmed_condition.substr(equals_pos + 1));

    if (col_value.empty()) {
        throw std::runtime_error("Empty value in condition: " + condition);
    }
    col_value.erase(col_value.find_last_not_of(" \t") + 1);
    if (col_value == "true" || col_value == "false") {
        bool bool_value = (col_value == "true");
        return [col_name, bool_value](const std::map<std::string, std::any>& row) {
            auto it = row.find(col_name);
            if (it == row.end()) {
                throw std::runtime_error("Column '" + col_name + "' not found.");
            }
            if (it->second.type() != typeid(bool)) {
                throw std::runtime_error("Type mismatch for column '" + col_name + "', expected bool.");
            }
            return std::any_cast<bool>(it->second) == bool_value;
            return !row.at(col_name).has_value();
            return !row.at(col_name).has_value();

    if (col_value[0] == '\'' && col_value.back() == '\'') {
        std::string str_value = col_value.substr(1, col_value.size() - 2);
        return [col_name, str_value](const std::map<std::string, std::any>& row) {
            auto it = row.find(col_name);
            if (it == row.end()) {
                throw std::runtime_error("Column '" + col_name + "' not found.");
            }
            if (it->second.type() != typeid(std::string)) {
                throw std::runtime_error("Type mismatch for column '" + col_name + "', expected string.");
            }
            return std::any_cast<std::string>(it->second) == str_value;
            };
    }

    if (is_numeric(col_value)) {
        int int_value = std::stoi(col_value);
        return [col_name, int_value](const std::map<std::string, std::any>& row) {
            auto it = row.find(col_name);
            if (it == row.end()) {
                throw std::runtime_error("Column '" + col_name + "' not found.");
            }
            if (it->second.type() != typeid(int)) {
                throw std::runtime_error("Type mismatch for column '" + col_name + "', expected int.");
            }
            return std::any_cast<int>(it->second) == int_value;
            return row.at(col_name).type() == typeid(int) && std::any_cast<int>(row.at(col_name)) == int_value;
            return row.at(col_name).type() == typeid(int) && std::any_cast<int>(row.at(col_name)) == int_value;

    throw std::runtime_error("Unsupported value format in condition: " + condition);
}
}

std::function<bool(const std::map<std::string, std::any>&)> Table::parse_simple_condition(const std::string& condition) const {
    auto equals_pos = condition.find('=');
    if (equals_pos == std::string::npos) {
        throw std::runtime_error("Syntax error in condition: " + condition);
    }
    }
    std::string col_name = trim(condition.substr(0, equals_pos));
    std::string col_value = trim(condition.substr(equals_pos + 1));

    if (col_name.empty()) {
        throw std::runtime_error("Column name is empty in condition: " + condition);
    }

    // Обработка значения NULL
    if (col_value.empty() || col_value == "NULL" || col_value == "null") {
        return [col_name](const std::map<std::string, std::any>& row) {
            auto it = row.find(col_name);
            if (it == row.end()) {
                throw std::runtime_error("Column '" + col_name + "' not found.");
            }
            return !it->second.has_value();
            };
    }

    // Обработка строковых значений
    if (!col_value.empty() && col_value[0] == '\'' && col_value.back() == '\'') {
        col_value = col_value.substr(1, col_value.size() - 2); // Удаление кавычек
        return [col_name, col_value](const std::map<std::string, std::any>& row) {
            auto it = row.find(col_name);
            if (it == row.end() || it->second.type() != typeid(std::string)) {
                return false;
            }
            return std::any_cast<std::string>(it->second) == col_value;
            };
    }

    // Обработка булевых значений
    if (col_value == "true" || col_value == "false") {
        bool bool_value = (col_value == "true");
        return [col_name, bool_value](const std::map<std::string, std::any>& row) {
            auto it = row.find(col_name);
            if (it == row.end() || it->second.type() != typeid(bool)) {
                return false;
            }
            return std::any_cast<bool>(it->second) == bool_value;
            };
    }
        }
    // Обработка целочисленных значений
    try {
        if (!is_numeric(col_value)) {
            throw std::runtime_error("Invalid integer format in condition: " + col_value);
        }

        int int_value = std::stoi(col_value);
        return [col_name, int_value](const std::map<std::string, std::any>& row) {
            auto it = row.find(col_name);
            if (it == row.end() || it->second.type() != typeid(int)) {
                return false;
            }
            return std::any_cast<int>(it->second) == int_value;
            };
    }
    catch (const std::exception& e) {
        throw std::runtime_error("Failed to parse integer value in condition: " + col_value + ", error: " + e.what());
    }
}


        };
        };
        };
        };
        };




