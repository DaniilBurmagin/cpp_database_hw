#ifndef UTILS_H
#define UTILS_H

#include <string>

// Удаляет пробелы в начале и в конце строки.
std::string trim(const std::string& str);

// Проверяет, является ли строка числом.
bool is_numeric(const std::string& str);

#endif // UTILS_H
