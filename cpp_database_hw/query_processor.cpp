#include "query_processor.h"
#include "database.h"
#include <sstream>
#include <stdexcept>
#include <iostream> 

std::string QueryProcessor::parse_and_execute(Database& db, const std::string& query) {
    std::istringstream stream(query);
    std::string command;
    stream >> command;

    if (command == "CREATE") {
        std::string temp;
        stream >> temp;
        if (temp == "TABLE") {
            std::string table_name;
            stream >> table_name;

            std::string schema_def;
            std::getline(stream, schema_def, '(');
            std::getline(stream, schema_def, ')');

            std::istringstream schema_stream(schema_def);
            std::map<std::string, std::string> schema;
            std::string column;
            while (std::getline(schema_stream, column, ',')) {
                auto colon_pos = column.find(':');
                if (colon_pos == std::string::npos)
                    throw std::runtime_error("Syntax error in schema definition.");
                std::string col_name = column.substr(0, colon_pos);
                std::string col_type = column.substr(colon_pos + 1);
                schema[col_name] = col_type;
            }

            db.create_table(table_name, schema);
            std::cout << "Table created: " << table_name << std::endl;
            return "Table " + table_name + " created.";
        }
    }
    else if (command == "INSERT") {
        std::string temp;
        stream >> temp; // TO
        if (temp != "TO") throw std::runtime_error("Syntax error: Expected 'TO' after INSERT.");

        std::string table_name;
        stream >> table_name;

        std::string values_def;
        std::getline(stream, values_def, '(');
        std::getline(stream, values_def, ')');

        std::istringstream values_stream(values_def);
        std::map<std::string, std::any> values;
        std::string value;
        while (std::getline(values_stream, value, ',')) {
            auto equals_pos = value.find('=');
            if (equals_pos != std::string::npos) {
                std::string col_name = value.substr(0, equals_pos);
                std::string col_value = value.substr(equals_pos + 1);

                if (col_value[0] == '\'') {
                    col_value = col_value.substr(1, col_value.size() - 2);
                    values[col_name] = col_value;
                }
                else if (col_value == "true" || col_value == "false") {
                    values[col_name] = (col_value == "true");
                }
                else {
                    values[col_name] = std::stoi(col_value);
                }
            }
        }

        Table* table = db.get_table(table_name);
        if (!table) throw std::runtime_error("Table not found: " + table_name);

        table->insert(values);
        std::cout << "Row inserted into table: " << table_name << std::endl;
        return "Row inserted into " + table_name + ".";
    }
    else if (command == "DELETE") {
        std::string temp, table_name, condition;
        stream >> temp >> table_name >> temp;
        if (temp != "WHERE") throw std::runtime_error("Syntax error: Expected 'WHERE'.");

        std::getline(stream, condition);

        Table* table = db.get_table(table_name);
        if (!table) throw std::runtime_error("Table not found: " + table_name);

        table->remove(condition);
        return "Rows deleted from " + table_name + ".";
    }
    else if (command == "UPDATE") {
        std::string table_name, temp, condition;
        stream >> table_name >> temp;
        if (temp != "SET") throw std::runtime_error("Syntax error: Expected 'SET'.");

        std::string updates_str;
        std::getline(stream, updates_str, 'W');
        std::getline(stream, condition);

        std::map<std::string, std::any> updates;
        std::istringstream updates_stream(updates_str);
        std::string update;
        while (std::getline(updates_stream, update, ',')) {
            auto equals_pos = update.find('=');
            if (equals_pos == std::string::npos) throw std::runtime_error("Syntax error in UPDATE.");
            std::string col_name = update.substr(0, equals_pos);
            std::string col_value = update.substr(equals_pos + 1);

            if (col_value[0] == '\'') {
                col_value = col_value.substr(1, col_value.size() - 2);
                updates[col_name] = col_value;
            }
            else if (col_value == "true" || col_value == "false") {
                updates[col_name] = (col_value == "true");
            }
            else {
                updates[col_name] = std::stoi(col_value);
            }
        }

        Table* table = db.get_table(table_name);
        if (!table) throw std::runtime_error("Table not found: " + table_name);

        table->update(condition, updates);
        return "Rows updated in " + table_name + ".";
    }
    else if (command == "SELECT") {
        std::string columns, temp, table_name, condition;
        stream >> columns >> temp >> table_name >> temp;
        std::getline(stream, condition);

        Table* table = db.get_table(table_name);
        if (!table) throw std::runtime_error("Table not found: " + table_name);

        auto rows = table->select(condition);
        std::ostringstream result;
        for (const auto& row : rows) {
            for (const auto& [col_name, value] : row) {
                if (value.type() == typeid(std::string)) {
                    result << col_name << ": " << std::any_cast<std::string>(value) << ", ";
                }
                else if (value.type() == typeid(int)) {
                    result << col_name << ": " << std::any_cast<int>(value) << ", ";
                }
                else if (value.type() == typeid(bool)) {
                    result << col_name << ": " << (std::any_cast<bool>(value) ? "true" : "false") << ", ";
                }
            }
            result << "\n";
        }
        return result.str();
    }

    return "Unknown command.";
}
