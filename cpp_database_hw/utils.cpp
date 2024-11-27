#include "utils.h"
#include <algorithm>
#include <cctype>

// Удаляет пробелы в начале и в конце строки.
std::string trim(const std::string& str) {
    if (str.empty()) return str;

    auto start = std::find_if_not(str.begin(), str.end(), [](unsigned char c) { return std::isspace(c); });
    auto end = std::find_if_not(str.rbegin(), str.rend(), [](unsigned char c) { return std::isspace(c); }).base();

    return (start < end) ? std::string(start, end) : std::string();
}

// Проверяет, является ли строка числом (целое или с плавающей точкой).
bool is_numeric(const std::string& str) {
    if (str.empty()) return false;

    size_t start = 0;
    if (str[0] == '+' || str[0] == '-') {
        start = 1; // Пропускаем знак
    }

    bool has_point = false;
    for (size_t i = start; i < str.size(); ++i) {
        if (str[i] == '.') {
            if (has_point) return false; // Больше одной точки
            has_point = true;
        }
        else if (!std::isdigit(static_cast<unsigned char>(str[i]))) {
            return false; // Не цифра
        }
    }

    return true;
}
