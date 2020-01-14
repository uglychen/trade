#ifndef MYSQL_UTILS_H_
#define MYSQL_UTILS_H_

#include <mysql.h>

void show_mysql_error(MYSQL *mysql);
void show_stmt_error(MYSQL_STMT *stmt);

#endif  // MYSQL_UTILS_H_