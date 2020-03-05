#include "store.h"

#include <stdlib.h>
#include <algorithm>

#include "utils.h"
#include "mysql_utils.h"
#include "config.h"
#include "logging.h"

#define SUMMARY_EVERY_US 1000000

struct ORDER_DATA {
    char order_id[32];
    char order_id_ind;
    long long user_id;
    char user_id_ind;
    char base_asset[48];
    char base_asset_ind;
    char quote_asset[48];
    char quote_asset_ind;
    char type;
    char type_ind;
    char side;
    char side_ind;
    char price[30];
    char price_ind;
    char origin_amount[30];
    char origin_amount_ind;
    char executed_price[30];
    char executed_price_ind;
    char executed_amount[30];
    char executed_amount_ind;
    char executed_quote_amount[30];
    char executed_quote_amount_ind;
    char status;
    char status_ind;
    char source;
    char source_ind;
    char ip[128];
    char ip_ind;

    MYSQL_TIME created_at;
    char created_at_ind;

    char is_subscribe;
    char is_subscribe_ind;

    char unfreeze_status;
    char unfreeze_status_ind;

    char first_unfreeze_at[30];
    char first_unfreeze_at_ind;

    char second_unfreeze_at[30];
    char second_unfreeze_at_ind;

};

struct TRADE_DATA {
    char trade_id[32];
    char trade_id_ind;
    char order_id[32];
    char order_id_ind;
    long long user_id;
    char user_id_ind;
    char base_asset[48];
    char base_asset_ind;
    char quote_asset[48];
    char quote_asset_ind;
    char side;
    char side_ind;
    char price[30];
    char price_ind;
    char amount[30];
    char amount_ind;
    char quote_amount[30];
    char quote_amount_ind;
    char fee[30];
    char fee_ind;
    char fee_asset[48];
    char fee_asset_ind;
    char is_maker;
    char is_maker_ind;
    MYSQL_TIME created_at;
    char created_at_ind;
};

struct ACCOUNT_DATA {
    char tx_id[32];
    char tx_id_ind;
    long long user_id;
    char user_id_ind;
    char asset_name[48];
    char asset_name_ind;
    char available_amount[30];
    char available_amount_ind;
    char frozen_amount[30];
    char frozen_amount_ind;
    char available_balance[30];
    char available_balance_ind;
    char frozen_balance[30];
    char frozen_balance_ind;
    char available_hash[256];
    char available_hash_ind;
    char frozen_hash[256];
    char frozen_hash_ind;
    char balance[30];
    char balance_ind;
    char type;
    char type_ind;
    char description[128];
    char description_ind;
    MYSQL_TIME created_at;
    char created_at_ind;
};

struct WITHDRAW_DATA {
    char withdraw_id[32];
    char withdraw_id_ind;
    char amt[32];
    char amt_ind;
};

struct JNL_DATA {
    char jnl_id[32];
    char jnl_id_ind;
};

struct LOCK_DATA {
    int lock_id;
    char lock_id_ind;
};

Store::Store() 
    : previous_ping_time_(0) {
}

Store::~Store() {
}

void Store::Init() {
    InitMysql();
    InitMongo();

    Config config;
    maker_user_set.clear();
    Json::Value maker_user_list = config["maker_user_list"];
    for (int i = 0; i < (int)maker_user_list.size(); i++){
        maker_user_set.insert(maker_user_list[i].asInt());
    }
}

bool Store::DoWork(Json::Value& values) {
    if (!values.isArray()) return false;

    Json::Value orders = Json::Value(Json::arrayValue);
    Json::Value trades = Json::Value(Json::arrayValue);
    Json::Value account_map = Json::Value(Json::objectValue);
    Json::Value accounts = Json::Value(Json::arrayValue);
    Json::Value journals = Json::Value(Json::arrayValue);
    Json::Value withdraws = Json::Value(Json::arrayValue);
    Json::Value jnls = Json::Value(Json::arrayValue);
    Json::Value locks = Json::Value(Json::arrayValue);

    for (unsigned i = 0; i < values.size(); i++) {
        std::string type = values[i]["type"].asString();
        if (type == "order") {
            char executed_amount[30] = {0};
            char executed_price[30] = {0};
            long double remain_amount = strtold(values[i]["amount"].asCString(), NULL);
            long double origin_amount = strtold(values[i]["origin_amount"].asCString(), NULL);
            long double executed_quote_amount = strtold(values[i]["executed_quote_amount"].asCString(), NULL);
            sprintf(executed_amount, "%.8Lf", origin_amount - remain_amount);
            values[i]["executed_amount"] = executed_amount;
            if (origin_amount - remain_amount > 1e-10) {
                sprintf(executed_price, "%.8Lf", executed_quote_amount / (origin_amount - remain_amount));
            } else {
                sprintf(executed_price, "%0.8Lf", 0.0L);
            }
            values[i]["executed_price"] = executed_price;
            orders.append(values[i]);
        } else if (type == "trade") {
            trades.append(values[i]);
        } else if (type == "account") {
            journals.append(values[i]);
            account_map[values[i]["user_id"].asString() + values[i]["asset"].asString()] = values[i];
        } else if (type == "apply_withdraw") {
            withdraws.append(values[i]);
        } else if (type == "human") {
            int jnl_type = values[i]["jnl_type"].asInt();
            if (jnl_type == 19) {
                locks.append(values[i]);
            } else {
                jnls.append(values[i]);
            }
        } else {
            LOG(ERROR) << "Unknown Type: " << type;
        }
    }
    
    if (mysql_autocommit(mysql_, 0))
        show_mysql_error(mysql_);

    if (orders.size()) {
        if (!BulkInsertOrder(orders)) {
            goto failure;
        }
    }
    if (trades.size()) {
        if (!BulkInsertTrade(trades)) {
            goto failure;
        }
    }
    if (account_map.size()) {
        for (Json::Value::iterator it = account_map.begin(); it != account_map.end(); it++) {
            accounts.append(*it);
        }
        for (unsigned i = 0; i < accounts.size(); i++) {
            char balance[30] = {0};
            long double available = strtold(accounts[i]["new_available"].asCString(), NULL);
            long double frozen = strtold(accounts[i]["new_frozen"].asCString(), NULL);
            sprintf(balance, "%.8Lf", available + frozen);
            accounts[i]["balance"] = balance;
        }
        if (!BulkInsertAccount(accounts)) {
            goto failure;
        }
    }
    if (withdraws.size()) {
        if (!BulkUpdateWithdraw(withdraws)) {
            goto failure;
        }
    }
    if (jnls.size()) {
        if (!BulkUpdateJnl(jnls)) {
            goto failure;
        }
    }
    if (locks.size()) {
        if (!BulkUpdateLock(locks)) {
            goto failure;
        }
    }
    if (mysql_commit(mysql_))
        show_mysql_error(mysql_);

    if (journals.size()) {
        BulkInsertJournal(journals);
    }

    return true;

failure:
    if (mysql_rollback(mysql_))
        show_mysql_error(mysql_);

    return false;
}

bool Store::DoIdleWork() {
    uint64_t now = now_microseconds();

    if (now - previous_ping_time_ > 10000000) {
        mysql_autocommit(mysql_, 1);
        if (mysql_query(mysql_, "SELECT 1")) {
            show_mysql_error(mysql_);
        } else {
            MYSQL_RES* result = mysql_store_result(mysql_);
            if (!result) {
                show_mysql_error(mysql_);
            } else {
                mysql_free_result(result);
            }
        }
        previous_ping_time_ = now;
    }
    
    return true;
}

void Store::CleanUp() {
    mysql_close(mysql_);
    mongoc_client_destroy(mongo_);
}

void Store::InitMysql() {
    Config config;

    std::string host;
    int port;
    std::string username;
    std::string password;
    std::string database;

    const Json::Value mysql = config["mysql"];
    if (mysql.empty()) {
        die("missing mysql config");
    }

    host = mysql.get("host", "").asString();
    username = mysql.get("username", "").asString();
    password = mysql.get("password", "").asString();
    database = mysql.get("database", "").asString();
    port = mysql.get("port", 0).asInt();

	LOG(ERROR) << "InitMysql host: " << host;
	LOG(ERROR) << "InitMysql username: " << username;
	LOG(ERROR) << "InitMysql password: " << password;
	LOG(ERROR) << "InitMysql database: " << database;
	LOG(ERROR) << "InitMysql port: " << port;
	
    mysql_ = mysql_init(NULL);
    std::string real_passwd = password;
    if (!mysql_real_connect(mysql_, host.c_str(), username.c_str(), real_passwd.c_str(), database.c_str(), port, NULL, 0))
        show_mysql_error(mysql_);

    if (mysql_set_character_set(mysql_, "utf8"))
        show_mysql_error(mysql_);

    int timeout = 10;
    int read_timeout = 60;
    mysql_options(mysql_, MYSQL_OPT_CONNECT_TIMEOUT, (unsigned int *)&timeout);
    mysql_options(mysql_, MYSQL_OPT_READ_TIMEOUT, (unsigned int *)&read_timeout);
    mysql_options(mysql_, MYSQL_OPT_WRITE_TIMEOUT, (unsigned int *)&read_timeout);
}

void Store::InitMongo() {
    Config config;

    std::string host;
    int port;
    std::string username;
    std::string password;
    std::string database;
    std::string collection;

    const Json::Value mongo = config["mongo"];
    if (mongo.empty()) {
        die("missing mongo config");
    }

    host = mongo.get("host", "").asString();
    port = mongo.get("port", 0).asInt();
    username = mongo.get("username", "").asString();
    password = mongo.get("password", "").asString();

	LOG(ERROR) << "InitMongo host: " << host;
	LOG(ERROR) << "InitMongo port: " << port;
	LOG(ERROR) << "InitMongo username: " << username;
	LOG(ERROR) << "InitMongo password: " << password;

    char uri[128];
    if (!username.length() || !password.length()) {
        sprintf(uri, "mongodb://%s:%d", host.c_str(), port);
    } else {
        std::string real_passwd = password;
        sprintf(uri, "mongodb://%s:%s@%s:%d/?authSource=admin", username.c_str(), real_passwd.c_str(), host.c_str(), port);
    }
    LOG(INFO) << "================================uri:  " << uri;
    mongo_ = mongoc_client_new(uri);
    if (!mongo_)
        die("Failed to parse URI");
}

bool Store::BulkInsertOrder(Json::Value& value) {
    Json::Value values = Json::Value(Json::arrayValue);
    if (value.isArray()) {
        values = value;
    } else {
        values.append(value);
    }

    ORDER_DATA* data = (ORDER_DATA*)malloc(sizeof(ORDER_DATA) * values.size());
    memset(data, 0, sizeof(ORDER_DATA) * values.size());
    unsigned int array_size = values.size();
    size_t row_size = sizeof(ORDER_DATA);

    for (unsigned i = 0; i < values.size(); i++) {
        strcpy(data[i].order_id, values[i]["id"].asCString());
        data[i].order_id_ind = STMT_INDICATOR_NTS;
        data[i].user_id = values[i].get("user_id", 0).asInt64();
        data[i].user_id_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].base_asset, values[i].get("base_asset", "").asCString());
        data[i].base_asset_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].quote_asset, values[i].get("quote_asset", "").asCString());
        data[i].quote_asset_ind = STMT_INDICATOR_NTS;
        data[i].type = values[i].get("order_type", 0).asInt();
        data[i].type_ind = STMT_INDICATOR_NONE;
        data[i].side = values[i].get("order_op", 0).asInt();
        data[i].side_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].price, values[i].get("price", "0.0").asCString());
        data[i].price_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].origin_amount, values[i].get("origin_amount", "0.0").asCString());
        data[i].origin_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].executed_price, values[i]["executed_price"].asCString());
        data[i].executed_price_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].executed_amount, values[i]["executed_amount"].asCString());
        data[i].executed_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].executed_quote_amount, values[i]["executed_quote_amount"].asCString());
        data[i].executed_quote_amount_ind = STMT_INDICATOR_NTS;
        data[i].status = values[i]["status"].asInt();
        data[i].status_ind = STMT_INDICATOR_NONE;
        data[i].source = values[i].get("source", 0).asInt();
        data[i].source_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].ip, values[i].get("ip", "").asCString());
        data[i].ip_ind = STMT_INDICATOR_NTS;

        time_t t = values[i].get("create_at", 0).asInt64();
        if (t == 0) {
            t = values[i]["update_at"].asInt64();
        }


        data[i].is_subscribe = values[i]["is_subscribe"].asInt();
        data[i].is_subscribe_ind = STMT_INDICATOR_NONE;
        data[i].unfreeze_status = values[i].get("unfreeze_status", 0).asInt();
        data[i].unfreeze_status_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].first_unfreeze_at, values[i].get("first_unfreeze_at", "2020-03-02 00:00:00").asCString());
        data[i].first_unfreeze_at_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].second_unfreeze_at, values[i].get("second_unfreeze_at", "2020-03-02 00:00:00").asCString());
        data[i].second_unfreeze_at_ind = STMT_INDICATOR_NTS;

        //LOG(INFO) << "data[i].source: " << data[i].source;
        //LOG(INFO) << "is_subscribe: " << data[i].is_subscribe;
        //LOG(INFO) << "is_subscribe: " << values[i].get("is_subscribe", 0).asInt();
        //LOG(INFO) << "unfreeze_status: " << data[i].unfreeze_status;
        //LOG(INFO) << "first_unfreeze_at: " << data[i].first_unfreeze_at;
        //LOG(INFO) << "second_unfreeze_at: " << data[i].second_unfreeze_at;

        struct tm local_time = {0};
        localtime_r(&t, &local_time);
        data[i].created_at.year = local_time.tm_year + 1900;
        data[i].created_at.month = local_time.tm_mon + 1;
        data[i].created_at.day = local_time.tm_mday;
        data[i].created_at.hour = local_time.tm_hour;
        data[i].created_at.minute = local_time.tm_min;
        data[i].created_at.second = local_time.tm_sec;
        data[i].created_at_ind = STMT_INDICATOR_NONE;     
    }

    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * 19);
    memset(bind, 0, sizeof(MYSQL_BIND) * 19);

    // string
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = (void*)&data[0].order_id;
    bind[0].u.indicator = &data[0].order_id_ind;
    // bigint
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = (void*)&data[0].user_id;
    bind[1].u.indicator = &data[0].user_id_ind;
    // string
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = (void*)&data[0].base_asset;
    bind[2].u.indicator = &data[0].base_asset_ind;
    // string
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = (void*)&data[0].quote_asset;
    bind[3].u.indicator = &data[0].quote_asset_ind;
    // tinyint
    bind[4].buffer_type = MYSQL_TYPE_TINY;
    bind[4].buffer = (void*)&data[0].type;
    bind[4].u.indicator = &data[0].type_ind;
    // tinyint
    bind[5].buffer_type = MYSQL_TYPE_TINY;
    bind[5].buffer = (void*)&data[0].side;
    bind[5].u.indicator = &data[0].side_ind;
    // decimal
    bind[6].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[6].buffer = (void*)&data[0].price;
    bind[6].u.indicator = &data[0].price_ind;
    // decimal
    bind[7].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[7].buffer = &data[0].origin_amount;
    bind[7].u.indicator = &data[0].origin_amount_ind;
    // decimal
    bind[8].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[8].buffer = (void*)&data[0].executed_price;
    bind[8].u.indicator = &data[0].executed_price_ind;
    // decimal
    bind[9].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[9].buffer = (void*)&data[0].executed_amount;
    bind[9].u.indicator = &data[0].executed_amount_ind;
    // decimal
    bind[10].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[10].buffer = (void*)&data[0].executed_quote_amount;
    bind[10].u.indicator = &data[0].executed_quote_amount_ind;
    // tinyint
    bind[11].buffer_type = MYSQL_TYPE_TINY;
    bind[11].buffer = (void*)&data[0].status;
    bind[11].u.indicator = &data[0].status_ind;
    // tinyint
    bind[12].buffer_type = MYSQL_TYPE_TINY;
    bind[12].buffer = (void*)&data[0].source;
    bind[12].u.indicator = &data[0].source_ind;
    // string
    bind[13].buffer_type = MYSQL_TYPE_STRING;
    bind[13].buffer = (void*)&data[0].ip;
    bind[13].u.indicator = &data[0].ip_ind;

    // datetime
    bind[14].buffer_type = MYSQL_TYPE_DATETIME;
    bind[14].buffer = (void*)&data[0].created_at;
    bind[14].u.indicator = &data[0].created_at_ind;

    //tinyint
    bind[15].buffer_type = MYSQL_TYPE_TINY;
    bind[15].buffer = (void*)&data[0].is_subscribe;
    bind[15].u.indicator = &data[0].is_subscribe_ind;
    
    // tinyint
    bind[16].buffer_type = MYSQL_TYPE_TINY;
    bind[16].buffer = (void*)&data[0].unfreeze_status;
    bind[16].u.indicator = &data[0].unfreeze_status_ind;

    // string
    bind[17].buffer_type = MYSQL_TYPE_STRING;
    bind[17].buffer = (void*)&data[0].first_unfreeze_at;
    bind[17].u.indicator = &data[0].first_unfreeze_at_ind;

    // string
    bind[18].buffer_type = MYSQL_TYPE_STRING;
    bind[18].buffer = (void*)&data[0].second_unfreeze_at;
    bind[18].u.indicator = &data[0].second_unfreeze_at_ind;

    std::string sql = "INSERT INTO t_order (order_id, user_id, base_asset, quote_asset, type, side, price, origin_amount, "
                      "executed_price, executed_amount, executed_quote_amount, status, source, ip, created_at, "
                      "is_subscribe, unfreeze_status, first_unfreeze_at, second_unfreeze_at)"
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                      "ON DUPLICATE KEY UPDATE executed_price=VALUES(executed_price), executed_amount=VALUES(executed_amount), "
                      "executed_quote_amount=VALUES(executed_quote_amount), status=VALUES(status), updated_at=VALUES(created_at)";


    /*std::string sql = "INSERT INTO t_order (order_id, user_id, base_asset, quote_asset, type, side, price, origin_amount, "
                  "executed_price, executed_amount, executed_quote_amount, status, source, ip, created_at, "
                  "is_subscribe)"
                  "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?) "
                  "ON DUPLICATE KEY UPDATE executed_price=VALUES(executed_price), executed_amount=VALUES(executed_amount), "
                  "executed_quote_amount=VALUES(executed_quote_amount), status=VALUES(status), updated_at=VALUES(created_at)";
    */

    MYSQL_STMT* stmt = mysql_stmt_init(mysql_);
    if (!stmt) {
        goto failure;
    }

    LOG(INFO) << "sql.c_str()=====>: " << sql.c_str();

    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length())) {
        goto stmt_failure;
    }

    mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
    mysql_stmt_attr_set(stmt, STMT_ATTR_ROW_SIZE, &row_size);

    if (mysql_stmt_bind_param(stmt, bind)) {
        goto stmt_failure;
    }

    if (mysql_stmt_execute(stmt)) {
        goto stmt_failure;
    }

    //assert(mysql_stmt_affected_rows(stmt) == values.size());BulkInsertOrder

    mysql_stmt_close(stmt);

    free(data);
    free(bind);

    LOG(INFO) << "=================== BulkInsertOrder INTO t_order: ok!!!!! ===============";
    return true;

stmt_failure:
    show_stmt_error(stmt);
    mysql_stmt_close(stmt);

failure:
    free(data);
    free(bind);
    return false;
}

bool Store::BulkInsertTrade(Json::Value& value) {
    Json::Value values = Json::Value(Json::arrayValue);
    if (value.isArray()) {
        values = value;
    } else {
        values.append(value);
    }

    TRADE_DATA* data = (TRADE_DATA*)malloc(sizeof(TRADE_DATA) * values.size());
    memset(data, 0, sizeof(TRADE_DATA) * values.size());
    unsigned int array_size = values.size();
    size_t row_size = sizeof(TRADE_DATA);

    for (unsigned i = 0; i < values.size(); i++) {
        strcpy(data[i].trade_id, values[i]["id"].asCString());
        data[i].trade_id_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].order_id, values[i]["order_id"].asCString());
        data[i].order_id_ind = STMT_INDICATOR_NTS;
        data[i].user_id = values[i]["user_id"].asInt64();
        data[i].user_id_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].base_asset, values[i]["base_asset"].asCString());
        data[i].base_asset_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].quote_asset, values[i]["quote_asset"].asCString());
        data[i].quote_asset_ind = STMT_INDICATOR_NTS;
        data[i].side = values[i]["is_buy"].asInt() ? 1 : 2;
        data[i].side_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].price, values[i]["price"].asCString());
        data[i].price_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].amount, values[i]["amount"].asCString());
        data[i].amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].quote_amount, values[i]["quote_amount"].asCString());
        data[i].quote_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].fee, values[i]["fee_amount"].asCString());
        data[i].fee_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].fee_asset, values[i]["fee_asset"].asCString());
        data[i].fee_asset_ind = STMT_INDICATOR_NTS;
        data[i].is_maker = values[i]["is_maker"].asInt();
        data[i].is_maker_ind = STMT_INDICATOR_NONE;

        time_t t = values[i]["create_at"].asInt64();
        struct tm local_time = {0};
        localtime_r(&t, &local_time);
        data[i].created_at.year = local_time.tm_year + 1900;
        data[i].created_at.month = local_time.tm_mon + 1;
        data[i].created_at.day = local_time.tm_mday;
        data[i].created_at.hour = local_time.tm_hour;
        data[i].created_at.minute = local_time.tm_min;
        data[i].created_at.second = local_time.tm_sec;
        data[i].created_at_ind = STMT_INDICATOR_NONE;
    }

    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * 13);
    memset(bind, 0, sizeof(MYSQL_BIND) * 13);

    // string
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = (void*)&data[0].trade_id;
    bind[0].u.indicator = &data[0].trade_id_ind;
    // string
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = (void*)&data[0].order_id;
    bind[1].u.indicator = &data[0].order_id_ind;
    // bigint
    bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[2].buffer = (void*)&data[0].user_id;
    bind[2].u.indicator = &data[0].user_id_ind;
    // string
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = (void*)&data[0].base_asset;
    bind[3].u.indicator = &data[0].base_asset_ind;;
    // string
    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = (void*)&data[0].quote_asset;
    bind[4].u.indicator = &data[0].quote_asset_ind;
    // tinyint
    bind[5].buffer_type = MYSQL_TYPE_TINY;
    bind[5].buffer = (void*)&data[0].side;
    bind[5].u.indicator = &data[0].side_ind;
    // decimal
    bind[6].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[6].buffer = (void*)&data[0].price;
    bind[6].u.indicator = &data[0].price_ind;
    // decimal
    bind[7].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[7].buffer = (void*)&data[0].amount;
    bind[7].u.indicator = &data[0].amount_ind;
    // decimal
    bind[8].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[8].buffer = (void*)&data[0].quote_amount;
    bind[8].u.indicator = &data[0].quote_amount_ind;
    // decimal
    bind[9].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[9].buffer = (void*)&data[0].fee;
    bind[9].u.indicator = &data[0].fee_ind;
    // decimal
    bind[10].buffer_type = MYSQL_TYPE_STRING;
    bind[10].buffer = (void*)&data[0].fee_asset;
    bind[10].u.indicator = &data[0].fee_asset_ind;
    // tinyint
    bind[11].buffer_type = MYSQL_TYPE_TINY;
    bind[11].buffer = (void*)&data[0].is_maker;
    bind[11].u.indicator = &data[0].is_maker_ind;
    // datetime
    bind[12].buffer_type = MYSQL_TYPE_DATETIME;
    bind[12].buffer = (void*)&data[0].created_at;
    bind[12].u.indicator = &data[0].created_at_ind;

    std::string sql = "INSERT INTO t_trade (trade_id, order_id, user_id, base_asset, quote_asset, side, "
                      "price, amount, quote_amount, fee, fee_asset, is_maker, created_at) VALUES "
                      "(?,?,?,?,?,?,?,?,?,?,?,?,?)";

    MYSQL_STMT* stmt = mysql_stmt_init(mysql_);
    if (!stmt) {
        goto failure;
    }

    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length())) {
        goto stmt_failure;
    }

    mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
    mysql_stmt_attr_set(stmt, STMT_ATTR_ROW_SIZE, &row_size);

    if (mysql_stmt_bind_param(stmt, bind)) {
        goto stmt_failure;
    }

    if (mysql_stmt_execute(stmt)) {
        goto stmt_failure;
    }

    assert(mysql_stmt_affected_rows(stmt) == values.size());

    mysql_stmt_close(stmt);

    free(data);
    free(bind);

    LOG(INFO) << "=================== BulkInsertTrade INTO t_trade: ok!!!!! ===============";

    return true;

stmt_failure:
    show_stmt_error(stmt);
    mysql_stmt_close(stmt);

failure:
    free(data);
    free(bind);
    return false;
}

bool Store::BulkInsertAccount(Json::Value& value) {
    Json::Value values = Json::Value(Json::arrayValue);
    if (value.isArray()) {
        values = value;
    } else {
        values.append(value);
    }

    ACCOUNT_DATA* data = (ACCOUNT_DATA*)malloc(sizeof(ACCOUNT_DATA) * values.size());
    memset(data, 0, sizeof(ACCOUNT_DATA) * values.size());
    unsigned int array_size = values.size();
    size_t row_size = sizeof(ACCOUNT_DATA);

    for (unsigned i = 0; i < values.size(); i++) {
        data[i].user_id = values[i]["user_id"].asInt64();
        data[i].user_id_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].asset_name, values[i]["asset"].asCString());
        data[i].asset_name_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].available_balance, values[i]["new_available"].asCString());
        data[i].available_balance_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].frozen_balance, values[i]["new_frozen"].asCString());
        data[i].frozen_balance_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].available_hash, values[i].get("new_encode_available", "").asCString());
        data[i].available_hash_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].frozen_hash, values[i].get("new_encode_frozen", "").asCString());
        data[i].frozen_hash_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].balance, values[i]["balance"].asCString());
        data[i].balance_ind = STMT_INDICATOR_NTS;

        time_t t = values[i]["update_at"].asInt64();
        struct tm local_time = {0};
        localtime_r(&t, &local_time);
        data[i].created_at.year = local_time.tm_year + 1900;
        data[i].created_at.month = local_time.tm_mon + 1;
        data[i].created_at.day = local_time.tm_mday;
        data[i].created_at.hour = local_time.tm_hour;
        data[i].created_at.minute = local_time.tm_min;
        data[i].created_at.second = local_time.tm_sec;
        data[i].created_at_ind = STMT_INDICATOR_NONE;
    }

    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * 8);
    memset(bind, 0, sizeof(MYSQL_BIND) * 8);

    // bigint
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = (void*)&data[0].user_id;
    bind[0].u.indicator = &data[0].user_id_ind;
    // string
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = (void*)&data[0].asset_name;
    bind[1].u.indicator = &data[0].asset_name_ind;
    // decimal
    bind[2].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[2].buffer = (void*)&data[0].available_balance;
    bind[2].u.indicator = &data[0].available_balance_ind;
    // decimal
    bind[3].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[3].buffer = (void*)&data[0].frozen_balance;
    bind[3].u.indicator = &data[0].frozen_balance_ind;
    // string
    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = (void*)&data[0].available_hash;
    bind[4].u.indicator = &data[0].available_hash_ind;
    // string
    bind[5].buffer_type = MYSQL_TYPE_STRING;
    bind[5].buffer = (void*)&data[0].frozen_hash;
    bind[5].u.indicator = &data[0].frozen_hash_ind;
    // decimal
    bind[6].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[6].buffer = (void*)&data[0].balance;
    bind[6].u.indicator = &data[0].balance_ind;
    // datetime
    bind[7].buffer_type = MYSQL_TYPE_DATETIME;
    bind[7].buffer = (void*)&data[0].created_at;
    bind[7].u.indicator = &data[0].created_at_ind;

    std::string sql = "INSERT INTO t_account (user_id, asset_symbol, available_bal, frozen_bal, available_bal_encryption, frozen_bal_encryption, bal, created_at) "
                      "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " 
                      "ON DUPLICATE KEY UPDATE available_bal=VALUES(available_bal), frozen_bal=VALUES(frozen_bal), "
                      "available_bal_encryption=VALUES(available_bal_encryption), frozen_bal_encryption=VALUES(frozen_bal_encryption), "
                      "bal=VALUES(bal), updated_at=VALUES(created_at)";

    MYSQL_STMT* stmt = mysql_stmt_init(mysql_);
    if (!stmt) {
        goto failure;
    }

    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length())) {
        goto stmt_failure;
    }

    mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
    mysql_stmt_attr_set(stmt, STMT_ATTR_ROW_SIZE, &row_size);

    if (mysql_stmt_bind_param(stmt, bind)) {
        goto stmt_failure;
    }

    if (mysql_stmt_execute(stmt)) {
        goto stmt_failure;
    }

    //assert(mysql_stmt_affected_rows(stmt) == values.size());

    mysql_stmt_close(stmt);

    free(data);
    free(bind);
    return true;

stmt_failure:
    show_stmt_error(stmt);
    mysql_stmt_close(stmt);

failure:
    free(data);
    free(bind);
    return false;
}

bool Store::BulkUpdateWithdraw(Json::Value& value) {
    Json::Value values = Json::Value(Json::arrayValue);
    if (value.isArray()) {
        values = value;
    } else {
        values.append(value);
    }

    WITHDRAW_DATA* data = (WITHDRAW_DATA*)malloc(sizeof(WITHDRAW_DATA) * values.size());
    memset(data, 0, sizeof(WITHDRAW_DATA) * values.size());
    unsigned int array_size = values.size();
    size_t row_size = sizeof(WITHDRAW_DATA);

    for (unsigned i = 0; i < values.size(); i++) {
        strcpy(data[i].withdraw_id, values[i]["id"].asCString());
        data[i].withdraw_id_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].amt, values[i]["amt"].asCString());
        data[i].amt_ind = STMT_INDICATOR_NTS;
    }

    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * 2);
    memset(bind, 0, sizeof(MYSQL_BIND) * 2);

    // string
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = (void*)&data[0].withdraw_id;
    bind[0].u.indicator = &data[0].withdraw_id_ind;

    bind[1].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[1].buffer = (void*)&data[0].amt;
    bind[1].u.indicator = &data[0].amt_ind;

    std::string sql = "UPDATE t_withdraw SET is_frozen=1 WHERE txno=? AND amt=?";

    MYSQL_STMT* stmt = mysql_stmt_init(mysql_);
    if (!stmt) {
        goto failure;
    }

    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length())) {
        goto stmt_failure;
    }

    mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
    mysql_stmt_attr_set(stmt, STMT_ATTR_ROW_SIZE, &row_size);

    if (mysql_stmt_bind_param(stmt, bind)) {
        goto stmt_failure;
    }

    if (mysql_stmt_execute(stmt)) {
        goto stmt_failure;
    }

    //assert(mysql_stmt_affected_rows(stmt) == values.size());

    mysql_stmt_close(stmt);

    free(data);
    free(bind);
    return true;

stmt_failure:
    show_stmt_error(stmt);
    mysql_stmt_close(stmt);

failure:
    free(data);
    free(bind);
    return false;
}

bool Store::BulkUpdateJnl(Json::Value& value) {
    Json::Value values = Json::Value(Json::arrayValue);
    if (value.isArray()) {
        values = value;
    } else {
        values.append(value);
    }

    JNL_DATA* data = (JNL_DATA*)malloc(sizeof(JNL_DATA) * values.size());
    memset(data, 0, sizeof(JNL_DATA) * values.size());
    unsigned int array_size = values.size();
    size_t row_size = sizeof(JNL_DATA);

    for (unsigned i = 0; i < values.size(); i++) {
        strcpy(data[i].jnl_id, values[i]["id"].asCString());
        data[i].jnl_id_ind = STMT_INDICATOR_NTS;
    }

    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * 1);
    memset(bind, 0, sizeof(MYSQL_BIND) * 1);

    // string
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = (void*)&data[0].jnl_id;
    bind[0].u.indicator = &data[0].jnl_id_ind;

    std::string sql = "UPDATE t_jnl_other SET status=1 WHERE txno=?";

    MYSQL_STMT* stmt = mysql_stmt_init(mysql_);
    if (!stmt) {
        goto failure;
    }

    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length())) {
        goto stmt_failure;
    }

    mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
    mysql_stmt_attr_set(stmt, STMT_ATTR_ROW_SIZE, &row_size);

    if (mysql_stmt_bind_param(stmt, bind)) {
        goto stmt_failure;
    }

    if (mysql_stmt_execute(stmt)) {
        goto stmt_failure;
    }

    //assert(mysql_stmt_affected_rows(stmt) == values.size());

    mysql_stmt_close(stmt);

    free(data);
    free(bind);
    return true;

stmt_failure:
    show_stmt_error(stmt);
    mysql_stmt_close(stmt);

failure:
    free(data);
    free(bind);
    return false;
}

bool Store::BulkUpdateLock(Json::Value& value) {
    Json::Value values = Json::Value(Json::arrayValue);
    if (value.isArray()) {
        values = value;
    } else {
        values.append(value);
    }

    LOCK_DATA* data = (LOCK_DATA*)malloc(sizeof(LOCK_DATA) * values.size());
    memset(data, 0, sizeof(LOCK_DATA) * values.size());
    unsigned int array_size = values.size();
    size_t row_size = sizeof(LOCK_DATA);

    for (unsigned i = 0; i < values.size(); i++) {
        data[i].lock_id = values[i]["op_id"].asInt();
        data[i].lock_id_ind = STMT_INDICATOR_NONE;
    }

    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * 1);
    memset(bind, 0, sizeof(MYSQL_BIND) * 1);

    // string
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = (void*)&data[0].lock_id;
    bind[0].u.indicator = &data[0].lock_id_ind;

    std::string sql = "UPDATE t_asset_lock_log SET is_frozen=1 WHERE id=? AND status IN (0, 1)";

    MYSQL_STMT* stmt = mysql_stmt_init(mysql_);
    if (!stmt) {
        goto failure;
    }

    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length())) {
        goto stmt_failure;
    }

    mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);
    mysql_stmt_attr_set(stmt, STMT_ATTR_ROW_SIZE, &row_size);

    if (mysql_stmt_bind_param(stmt, bind)) {
        goto stmt_failure;
    }

    if (mysql_stmt_execute(stmt)) {
        goto stmt_failure;
    }

    //assert(mysql_stmt_affected_rows(stmt) == values.size());

    mysql_stmt_close(stmt);

    free(data);
    free(bind);
    return true;

stmt_failure:
    show_stmt_error(stmt);
    mysql_stmt_close(stmt);

failure:
    free(data);
    free(bind);
    return false;
}

bool Store::BulkInsertJournal(Json::Value& value) {
    Json::Value values = Json::Value(Json::arrayValue);
    if (value.isArray()) {
        values = value;
    } else {
        values.append(value);
    }

    mongoc_collection_t* collection = mongoc_client_get_collection(mongo_, "admin", "m_journal");
    if (!collection)
        die("Failed to get collection");

    mongoc_bulk_operation_t* bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);

    bson_t doc;
    bson_decimal128_t available_amount;
    bson_decimal128_t available_balance;
    bson_decimal128_t frozen_amount;
    bson_decimal128_t frozen_balance;

    bool data_flag = false;
    for (unsigned i = 0; i < values.size(); i++) {
        if (maker_user_set.find(values[i]["user_id"].asInt()) != maker_user_set.end()){
            continue;
        }
        data_flag = true;

        bson_decimal128_from_string(values[i]["change_available"].asCString(), &available_amount);
        bson_decimal128_from_string(values[i]["new_available"].asCString(), &available_balance);
        bson_decimal128_from_string(values[i]["change_frozen"].asCString(), &frozen_amount);
        bson_decimal128_from_string(values[i]["new_frozen"].asCString(), &frozen_balance);

        bson_init(&doc);
        BSON_APPEND_UTF8(&doc, "txno", values[i]["id"].asCString());
        BSON_APPEND_INT64(&doc, "user_id", values[i]["user_id"].asInt64());
        BSON_APPEND_UTF8(&doc, "asset_symbol", values[i]["asset"].asCString());
        BSON_APPEND_DECIMAL128(&doc, "available_amt", &available_amount);
        BSON_APPEND_DECIMAL128(&doc, "available_bal", &available_balance);
        BSON_APPEND_UTF8(&doc, "available_bal_encryption", values[i].get("new_encode_available", "").asCString());
        BSON_APPEND_DECIMAL128(&doc, "frozen_amt", &frozen_amount);
        BSON_APPEND_DECIMAL128(&doc, "frozen_bal", &frozen_balance);
        BSON_APPEND_UTF8(&doc, "frozen_bal_encryption", values[i].get("new_encode_frozen", "").asCString());
        BSON_APPEND_INT32(&doc, "type", values[i]["jnl_type"].asInt());
        BSON_APPEND_UTF8(&doc, "remark", values[i]["remark"].asCString());
        BSON_APPEND_DATE_TIME(&doc, "created_at", values[i]["update_at"].asInt64() * 1000);

        mongoc_bulk_operation_insert(bulk, &doc);
        bson_destroy(&doc);
    }
    
    bson_error_t error;
    if (data_flag && !mongoc_bulk_operation_execute(bulk, NULL, &error)) {
        die("Error inserting data: %s\n", error.message);
    }

    mongoc_bulk_operation_destroy(bulk);
    mongoc_collection_destroy(collection);

    return true;
}

/*
bool Store::InsertJournal_(Json::Value& value) {
    // LOG(INFO) << "InsertJournal";

    Json::Value values = Json::Value(Json::arrayValue);
    if (value.isArray()) {
        values = value;
    } else {
        values.append(value);
    }

    ACCOUNT_DATA* data = (ACCOUNT_DATA*)malloc(sizeof(ACCOUNT_DATA) * values.size());
    memset(data, 0, sizeof(ACCOUNT_DATA) * values.size());

    for (unsigned i = 0; i < values.size(); i++) {
        strcpy(data[i].tx_id, values[i]["id"].asCString());
        data[i].user_id = values[i]["user_id"].asInt64();
        strcpy(data[i].asset_name, values[i]["asset"].asCString());
        strcpy(data[i].available_amount, values[i]["change_available"].asCString());
        strcpy(data[i].available_balance, values[i]["new_available"].asCString());
        strcpy(data[i].frozen_amount, values[i]["change_frozen"].asCString());
        strcpy(data[i].frozen_balance, values[i]["new_frozen"].asCString());
        data[i].type = values[i]["jnl_type"].asInt();
        strcpy(data[i].description, values[i]["remark"].asCString());

        time_t t = values[i]["create_at"].asInt64();
        struct tm local_time = {0};
        localtime_r(&t, &local_time);
        data[i].created_at.year = local_time.tm_year + 1900;
        data[i].created_at.month = local_time.tm_mon + 1;
        data[i].created_at.day = local_time.tm_mday;
        data[i].created_at.hour = local_time.tm_hour;
        data[i].created_at.minute = local_time.tm_min;
        data[i].created_at.second = local_time.tm_sec;
    }

    int col = 9;
    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * col * values.size());
    memset(bind, 0, sizeof(MYSQL_BIND) * col * values.size());

    for (unsigned i = 0; i < values.size(); i++) {
        // string
        bind[col * i + 0].buffer_type = MYSQL_TYPE_STRING;
        bind[col * i + 0].buffer = (void*)&data[i].tx_id;
        bind[col * i + 0].buffer_length = 32;
        // bigint
        bind[col * i + 1].buffer_type = MYSQL_TYPE_LONGLONG;
        bind[col * i + 1].buffer = (void*)&data[i].user_id;
        // string
        bind[col * i + 2].buffer_type = MYSQL_TYPE_STRING;
        bind[col * i + 2].buffer = (void*)&data[i].asset_name;
        bind[col * i + 2].buffer_length = 48;
        // decimal
        bind[col * i + 3].buffer_type = MYSQL_TYPE_DECIMAL;
        bind[col * i + 3].buffer = (void*)&data[i].available_amount;
        bind[col * i + 3].buffer_length = 30;
        // decimal
        bind[col * i + 4].buffer_type = MYSQL_TYPE_DECIMAL;
        bind[col * i + 4].buffer = (void*)&data[i].available_balance;
        bind[col * i + 4].buffer_length = 30;
        // decimal
        bind[col * i + 5].buffer_type = MYSQL_TYPE_DECIMAL;
        bind[col * i + 5].buffer = (void*)&data[i].frozen_amount;
        bind[col * i + 5].buffer_length = 30;
        // decimal
        bind[col * i + 6].buffer_type = MYSQL_TYPE_DECIMAL;
        bind[col * i + 6].buffer = (void*)&data[i].frozen_balance;
        bind[col * i + 6].buffer_length = 30;
        // string
        bind[col * i + 7].buffer_type = MYSQL_TYPE_STRING;
        bind[col * i + 7].buffer = (void*)&data[i].description;
        bind[col * i + 7].buffer_length = 128;
        // datetime
        bind[col * i + 8].buffer_type = MYSQL_TYPE_DATETIME;
        bind[col * i + 8].buffer = (void*)&data[i].created_at;
        bind[col * i + 8].buffer_length = 30;
    }

    std::string sql = "INSERT INTO t_journal (tx_id, user_id, asset_symbol, available_amount, available_bal, "
                      "frozen_amount, frozen_bal, remark, created_at) VALUES ";
    for (unsigned i = 0; i < values.size(); i++) {
        sql.append("(?,?,?,?,?,?,?,?,?),");
    }
    sql.erase(sql.end() - 1);

    MYSQL_STMT* stmt = mysql_stmt_init(mysql_);
    if (!stmt) {
        goto failure;
    }
    
    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length())) {
        show_stmt_error(stmt);
        mysql_stmt_close(stmt);
        goto failure;
    }

    if (mysql_stmt_bind_param(stmt, bind)) {
        show_stmt_error(stmt);
        mysql_stmt_close(stmt);
        goto failure;
    }

    if (mysql_stmt_execute(stmt)) {
        show_stmt_error(stmt);
        mysql_stmt_close(stmt);
        goto failure;
    }

    assert(mysql_stmt_affected_rows(stmt) == values.size());

    mysql_stmt_close(stmt);

    free(data);
    free(bind);
    return true;

failure:
    free(data);
    free(bind);
    return false;
}
*/

void Store::TestInsert() {
    Json::Value values = Json::Value(Json::arrayValue);
    for (int i = 0; i < 100; i++) {
        values.append(TestOrder());
    }
    for (int i = 0; i < 20; i++) {
        values.append(TestTrade());
    }
    for (int i = 0; i < 20; i++) {
        values.append(TestAccount());
    }

    DoWork(values);
}

Json::Value Store::TestOrder() {
    char a[20];
    Json::Value value;

    long order_id = random();
    sprintf(a, "%ld", order_id);

    int64_t user_id = random();
    value["type"] = "order";
    value["id"] = a;
    value["user_id"] = user_id;
    value["base_asset"] = "EOS";
    value["quote_asset"] = "ETH";
    value["order_type"] = 1;
    value["order_op"] = 1;
    value["price"] = "12899.111";
    value["origin_amount"] = "9999.111212";
    value["amount"] = "9999.111212";
    value["executed_price"] = "0.12121";
    value["executed_quote_amount"] = "1.29";
    value["status"] = 1;
    value["source"] = 1;
    value["ip"] = "";
    value["create_at"] = now_microseconds() / 1000000;

    return value;
}

Json::Value Store::TestTrade() {
    char a[20], b[20];
    Json::Value value;

    long trade_id = random();
    sprintf(a, "%ld", trade_id);

    long order_id = random();
    sprintf(b, "%ld", order_id);

    int64_t user_id = random();
    value["type"] = "trade";
    value["id"] = a;
    value["order_id"] = b;
    value["user_id"] = user_id;
    value["base_asset"] = "EOS";
    value["quote_asset"] = "ETH";
    value["is_buy"] = 1;
    value["price"] = "12899.111";
    value["amount"] = "9999.111212";
    value["quote_amount"] = "0.12121";
    value["fee_amount"] = "1.29";
    value["fee_asset"] = "EOS";
    value["is_maker"] = 1;
    value["create_at"] = now_microseconds() / 1000000;

    return value;
}

Json::Value Store::TestAccount() {
    char a[20];
    Json::Value value;

    long trade_id = random();
    sprintf(a, "%ld", trade_id);

    int64_t user_id = random();
    value["type"] = "account";
    value["id"] = a;
    value["user_id"] = user_id;
    value["asset"] = "EOS";
    value["change_available"] = "12899.111";
    value["change_frozen"] = "9999.111212";
    value["new_available"] = "0.12121";
    value["new_frozen"] = "1.29";
    value["remark"] = "EOS";
    value["create_at"] = now_microseconds() / 1000000;

    return value;
}