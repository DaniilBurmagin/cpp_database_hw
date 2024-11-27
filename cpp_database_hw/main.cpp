#include "database.h"
#include <iostream>
#include <fstream>
#include <string>

int main() {
    try {
        Database db;

        // Создание таблицы
        db.execute("CREATE TABLE users (id:int32,name:string,is_admin:bool)");
        db.execute("INSERT TO users (id=1,name='Alice',is_admin=false)");
        db.execute("INSERT TO users (id=2,name='Bob',is_admin=true)");
        db.execute("INSERT TO users (id=3,name='Charlie',is_admin=false)");

        // Создание индекса
        db.execute("CREATE INDEX ON users (id)");
        std::cout << "Index created on column 'id'.\n";

        // Сохранение данных
        std::cout << "Saved rows:\n" << db.execute("SELECT * FROM users WHERE true") << std::endl;
        db.save_to_file("db.bin");
        std::cout << "Data saved to file.\n";

        // Чтение содержимого файла
        std::ifstream file("db.bin");
        if (file.is_open()) {
            std::cout << "File content (db.bin):" << std::endl;
            std::string line;
            while (std::getline(file, line)) {
                std::cout << line << std::endl;
            }
            file.close();
        }
        else {
            std::cerr << "Failed to open file for reading.\n";
        }

        // Загрузка данных из файла
        db.load_from_file("db.bin");
        std::cout << "Data loaded from file.\n";

        // Печать данных
        std::cout << "Loaded rows:\n" << db.execute("SELECT * FROM users WHERE true") << std::endl;

        // Выборка с условиями
        std::cout << "Rows with is_admin=true:\n"
            << db.execute("SELECT * FROM users WHERE is_admin=true") << std::endl;

        std::cout << "Rows where name='Alice' AND is_admin=false:\n"
            << db.execute("SELECT * FROM users WHERE name='Alice' AND is_admin=false") << std::endl;

        // Обновление данных
        db.execute("UPDATE users SET name='UpdatedName', is_admin=true WHERE id=1");
        std::cout << "Rows after update:\n" << db.execute("SELECT * FROM users WHERE true") << std::endl;

        // Удаление данных
        db.execute("DELETE FROM users WHERE id=3");
        std::cout << "Rows after delete:\n" << db.execute("SELECT * FROM users WHERE true") << std::endl;

        // Ошибка при вставке
        try {
            db.execute("INSERT TO users (id=4,name=NULL,is_admin=true)");
        }
        catch (const std::exception& e) {
            std::cerr << "Constraint error: " << e.what() << std::endl;
        }

        // Транзакции
        db.begin_transaction();
        db.execute("INSERT TO users (id=4,name='David',is_admin=true)");
        db.execute("UPDATE users SET is_admin=false WHERE id=2");
        db.rollback_transaction();
        std::cout << "Rows after rollback:\n" << db.execute("SELECT * FROM users WHERE true") << std::endl;

        db.begin_transaction();
        db.execute("INSERT TO users (id=4,name='Eve',is_admin=true)");
        db.execute("UPDATE users SET is_admin=false WHERE id=2");
        db.commit_transaction();
        std::cout << "Rows after commit:\n" << db.execute("SELECT * FROM users WHERE true") << std::endl;

    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    return 0;
}
