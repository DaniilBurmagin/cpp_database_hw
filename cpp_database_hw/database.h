#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <map>
#include <memory>
#include <vector>
#include "table.h"

class Database {
public:
    // Создаёт таблицу с указанным именем и схемой.
    void create_table(const std::string& name, const std::map<std::string, std::string>& schema);

    // Получает указатель на таблицу по её имени.
    Table* get_table(const std::string& name);

    // Выполняет SQL-запрос и возвращает результат в виде строки.
    std::string execute(const std::string& query);

    // Сохраняет базу данных в бинарный файл.
    void save_to_file(const std::string& filename) const;

    // Загружает базу данных из бинарного файла.
    void load_from_file(const std::string& filename);

    // Начало транзакции.
    void begin_transaction();

    // Откат транзакции.
    void rollback_transaction();

    // Подтверждение транзакции.
    void commit_transaction();

private:
    std::map<std::string, std::shared_ptr<Table>> tables; // Хранилище таблиц
    std::vector<std::map<std::string, std::shared_ptr<Table>>> transaction_stack; // Стек для транзакций
};

#endif // DATABASE_H
