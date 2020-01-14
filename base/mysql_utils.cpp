#include "mysql_utils.h"

#include "utils.h"
#include "logging.h"

void show_mysql_error(MYSQL *mysql) {
    die("(%d) [%s] \"%s\"", mysql_errno(mysql), mysql_sqlstate(mysql), mysql_error(mysql));
}

void show_stmt_error(MYSQL_STMT *stmt) {
    LOG(INFO) << "(" << mysql_stmt_errno(stmt) << ") [" << mysql_stmt_sqlstate(stmt) << "] \"" << mysql_stmt_error(stmt) << "\"";
}
