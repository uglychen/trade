#include "con_store.h"

#include <stdlib.h>
#include <algorithm>

#include "utils.h"
#include "mysql_utils.h"
#include "config.h"
#include "logging.h"

#define SUMMARY_EVERY_US 1000000

struct ORDER_DATA {
    int contract_id;
    char contract_id_ind;
    char contract_name[32];
    char contract_name_ind;
    char asset_symbol[32];
    char asset_symbol_ind;
    char unit_amount[30];
    char unit_amount_ind;
    char lever_rate;
    char lever_rate_ind;
    char order_id[32];
    char order_id_ind;
    long long user_id;
    char user_id_ind;
    char type;
    char type_ind;
    char isbbo;
    char isbbo_ind;
    char side;
    char side_ind;
    char price[30];
    char price_ind;
    char origin_amount[30];
    char origin_amount_ind;
    char take_profit[30];
    char take_profit_ind;
    char stop_loss[30];
    char stop_loss_ind;
    char executed_price[30];
    char executed_price_ind;
    char executed_amount[30];
    char executed_amount_ind;
    char executed_quote_amount[30];
    char executed_quote_amount_ind;
    char system_type;
    char system_type_ind;
    char status;
    char status_ind;
    char source;
    char source_ind;
    char ip[128];
    char ip_ind;
    MYSQL_TIME created_at;
    char created_at_ind;
};

struct ORDER_AREA_DATA {
    int contract_id;
    char contract_id_ind;
    char contract_name[32];
    char contract_name_ind;
    char asset_symbol[32];
    char asset_symbol_ind;
    char unit_amount[30];
    char unit_amount_ind;
    char lever_rate;
    char lever_rate_ind;
    char trading_area[32];
    char trading_area_ind;
    char order_id[32];
    char order_id_ind;
    long long user_id;
    char user_id_ind;
    char type;
    char type_ind;
    char isbbo;
    char isbbo_ind;
    char side;
    char side_ind;
    char price[30];
    char price_ind;
    char origin_amount[30];
    char origin_amount_ind;
    char take_profit[30];
    char take_profit_ind;
    char stop_loss[30];
    char stop_loss_ind;
    char executed_price[30];
    char executed_price_ind;
    char executed_amount[30];
    char executed_amount_ind;
    char executed_quote_amount[30];
    char executed_quote_amount_ind;
    char executed_settle_amount[30];
    char executed_settle_amount_ind;
    char system_type;
    char system_type_ind;
    char status;
    char status_ind;
    char source;
    char source_ind;
    char ip[128];
    char ip_ind;
    MYSQL_TIME created_at;
    char created_at_ind;
};

struct TRADE_DATA {
    int contract_id;
    char contract_id_ind;
    char contract_name[32];
    char contract_name_ind;
    char asset_symbol[32];
    char asset_symbol_ind;
    char unit_amount[30];
    char unit_amount_ind;
    char lever_rate;
    char lever_rate_ind;
    char trade_id[32];
    char trade_id_ind;
    char order_id[32];
    char order_id_ind;
    long long user_id;
    char user_id_ind;
    char side;
    char side_ind;
    char price[30];
    char price_ind;
    char amount[30];
    char amount_ind;
    char quote_amount[30];
    char quote_amount_ind;
    char profit[30];
    char profit_ind;
    char fee[30];
    char fee_ind;
    char fee_asset[48];
    char fee_asset_ind;
    char is_maker;
    char is_maker_ind;
    MYSQL_TIME created_at;
    char created_at_ind;
};


struct FUNDING_FEE_DATA {
    int user_id;
    char user_id_ind;
    char trading_area[32];
    char trading_area_ind;
    int contract_id;
    char contract_id_ind;
    char contract_name[32];
    char contract_name_ind;
    char asset_symbol[32];
    char asset_symbol_ind;
    char unit_amount[30];
    char unit_amount_ind;
    char lever_rate;
    char lever_rate_ind;
    int position_type;
    char position_type_ind;
    char amount[30];
    char amount_ind;
    char position_value[30];
    char position_value_ind;
    char fund_rate[30];
    char fund_rate_ind;
    char fund_fee[30];
    char fund_fee_ind;
    MYSQL_TIME created_at;
    char created_at_ind;
};

struct TRADE_AREA_DATA {
    int contract_id;
    char contract_id_ind;
    char contract_name[32];
    char contract_name_ind;
    char asset_symbol[32];
    char asset_symbol_ind;
    char unit_amount[30];
    char unit_amount_ind;
    char lever_rate;
    char lever_rate_ind;
    char trading_area[32];
    char trading_area_ind;
    char trade_id[32];
    char trade_id_ind;
    char order_id[32];
    char order_id_ind;
    long long user_id;
    char user_id_ind;
    char side;
    char side_ind;
    char price[30];
    char price_ind;
    char amount[30];
    char amount_ind;
    char quote_amount[30];
    char quote_amount_ind;
    char settle_amount[30];
    char settle_amount_ind;
    char profit[30];
    char profit_ind;
    char fee[30];
    char fee_ind;
    char fee_asset[48];
    char fee_asset_ind;
    char is_maker;
    char is_maker_ind;
    MYSQL_TIME created_at;
    char created_at_ind;
};

struct POSITION_DATA {
    long long user_id;
    char user_id_ind;
    int contract_id;
    char contract_id_ind;
    char contract_name[32];
    char contract_name_ind;
    char asset_symbol[32];
    char asset_symbol_ind;
    char unit_amount[30];
    char unit_amount_ind;
    char lever_rate;
    char lever_rate_ind;
    char buy_frozen[30];
    char buy_frozen_ind;
    char buy_margin[30];
    char buy_margin_ind;
    char buy_amount[30];
    char buy_amount_ind;
    char buy_available[30];
    char buy_available_ind;
    char buy_quote_amount[30];
    char buy_quote_amount_ind;
    char buy_quote_amount_settle[30];
    char buy_quote_amount_settle_ind;
    char buy_take_profit[30];
    char buy_take_profit_ind;
    char buy_stop_loss[30];
    char buy_stop_loss_ind;
    char sell_frozen[30];
    char sell_frozen_ind;
    char sell_margin[30];
    char sell_margin_ind;
    char sell_amount[30];
    char sell_amount_ind;
    char sell_available[30];
    char sell_available_ind;
    char sell_quote_amount[30];
    char sell_quote_amount_ind;
    char sell_quote_amount_settle[30];
    char sell_quote_amount_settle_ind;
    char sell_take_profit[30];
    char sell_take_profit_ind;
    char sell_stop_loss[30];
    char sell_stop_loss_ind;
    MYSQL_TIME created_at;
    char created_at_ind;
};

struct POSITION_AREA_DATA {
    long long user_id;
    char user_id_ind;
    int contract_id;
    char contract_id_ind;
    char contract_name[32];
    char contract_name_ind;
    char asset_symbol[32];
    char asset_symbol_ind;
    char unit_amount[30];
    char unit_amount_ind;
    char lever_rate;
    char lever_rate_ind;
    char trading_area[32];
    char trading_area_ind;
    char buy_frozen[30];
    char buy_frozen_ind;
    char buy_margin[30];
    char buy_margin_ind;
    char buy_amount[30];
    char buy_amount_ind;
    char buy_available[30];
    char buy_available_ind;
    char buy_quote_amount[30];
    char buy_quote_amount_ind;
    char buy_quote_amount_settle[30];
    char buy_quote_amount_settle_ind;
    char buy_take_profit[30];
    char buy_take_profit_ind;
    char buy_stop_loss[30];
    char buy_stop_loss_ind;
    char sell_frozen[30];
    char sell_frozen_ind;
    char sell_margin[30];
    char sell_margin_ind;
    char sell_amount[30];
    char sell_amount_ind;
    char sell_available[30];
    char sell_available_ind;
    char sell_quote_amount[30];
    char sell_quote_amount_ind;
    char sell_quote_amount_settle[30];
    char sell_quote_amount_settle_ind;
    char sell_take_profit[30];
    char sell_take_profit_ind;
    char sell_stop_loss[30];
    char sell_stop_loss_ind;
    MYSQL_TIME created_at;
    char created_at_ind;
};

struct ACCOUNT_DATA {
    long long user_id;
    char user_id_ind;
    char asset_symbol[48];
    char asset_symbol_ind;
    char balance[30];
    char balance_ind;
    char margin[30];
    char margin_ind;
    char frozen_margin[30];
    char frozen_margin_ind;
    char profit[30];
    char profit_ind;
    char balance_hash[256];
    char balance_hash_ind;
    char margin_hash[256];
    char margin_hash_ind;
    char frozen_margin_hash[256];
    char frozen_margin_hash_ind;
    char profit_hash[256];
    char profit_hash_ind;
    MYSQL_TIME created_at;
    char created_at_ind;
};

struct JNL_DATA {
    char jnl_id[32];
    char jnl_id_ind;
    char jnl_amount[30];
    char jnl_amount_ind;
};

Store::Store() 
    : previous_ping_time_(0) {
}

Store::~Store() {
}

void Store::Init() {
    InitMysql();
    InitMongo();
    InitUsdMongo();

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
    Json::Value positions = Json::Value(Json::arrayValue);
    Json::Value account_map = Json::Value(Json::objectValue);
    Json::Value accounts = Json::Value(Json::arrayValue);
    Json::Value journals = Json::Value(Json::arrayValue);
    Json::Value jnls = Json::Value(Json::arrayValue);

    Json::Value match_trade_msg = Json::Value(Json::arrayValue);

    Json::Value orders_area = Json::Value(Json::arrayValue);
    Json::Value trades_area = Json::Value(Json::arrayValue);
    Json::Value positions_area = Json::Value(Json::arrayValue);

    Json::Value funding_fee = Json::Value(Json::arrayValue);

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
            if (values[i].isMember("settle_asset") && values[i]["settle_asset"].asString() != "USDT"){
                orders_area.append(values[i]);
            }else{
                orders.append(values[i]);
            }
        } else if (type == "trade") {
            if (values[i].isMember("settle_asset") && values[i]["settle_asset"].asString() != "USDT"){
                trades_area.append(values[i]);
            }else{
                trades.append(values[i]);
            }
        } else if (type == "position") {
            if (values[i].isMember("settle_asset") && values[i]["settle_asset"].asString() != "USDT"){
                positions_area.append(values[i]);
            }else{
                positions.append(values[i]);
            }
        } else if (type == "account") {
            journals.append(values[i]);
            account_map[values[i]["user_id"].asString() + values[i]["asset"].asString()] = values[i];
            if (values[i]["jnl_type"].asInt() == USER_ACCOUNT_JNL_FUND_FEE){
                funding_fee.append(values[i]);
            }
        } else if (type == "human") {
            int jnl_type = values[i]["jnl_type"].asInt();
            if (jnl_type > 0) {
                jnls.append(values[i]);
            }
        } else if (type == "trade_msg") {
            match_trade_msg.append(values[i]);
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
    if (positions.size()) {
        if (!BulkInsertPosition(positions)) {
            goto failure;
        }
    }
    if (orders_area.size()) {
        if (!BulkInsertAreaOrder(orders_area)) {
            goto failure;
        }
    }
    if (trades_area.size()) {
        if (!BulkInsertAreaTrade(trades_area)) {
            goto failure;
        }
    }
    if (positions_area.size()) {
        if (!BulkInsertAreaPosition(positions_area)) {
            goto failure;
        }
    }
    if (account_map.size()) {
        for (Json::Value::iterator it = account_map.begin(); it != account_map.end(); it++) {
            accounts.append(*it);
        }
        if (!BulkInsertAccount(accounts)) {
            goto failure;
        }
    }
    if (funding_fee.size()) {
        if (!BulkInsertFundingFee(funding_fee)) {
            goto failure;
        }
    }
    if (jnls.size()) {
        if (!BulkUpdateJnl(jnls)) {
            goto failure;
        }
    }

    if (mysql_commit(mysql_))
        show_mysql_error(mysql_);

    if (journals.size()) {
        BulkInsertJournal(journals);
    }
    if (match_trade_msg.size()){
        BulkInsertMatchTradeMsg(match_trade_msg);
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

    mysql_ = mysql_init(NULL);
    std::string real_passwd = real_password(password);
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

    std::string username;
    std::string password;
    std::string database;
    std::string rsname ="";

    const Json::Value mongo = config["mongo"];
    if (mongo.empty())
    {
        die("missing mongo config");
    }

    const Json::Value hosts_config = mongo["hosts"];
    std::string hosts = "";
    std::string slave = "";
    unsigned int hosts_len = hosts_config.size();
    for (unsigned i = 0; i < hosts_len; ++i)
    {
        hosts = hosts + hosts_config[i]["host"].asString() + ":" + hosts_config[i]["port"].asString();
        if (i < hosts_len - 1)
        {
            hosts = hosts + ",";
        }
    }
    if (hosts_len <= 0)
    {
        die("missing mongo hosts config");
    }

    rsname = mongo.get("rsname", "").asString();

    if (hosts_len > 1)
    {
        slave = "?";
        if(rsname.length() >0)
        {
            slave = "?replicaSet="+rsname;
        }
    }
    
    
    username = mongo.get("username", "").asString();
    password = mongo.get("password", "").asString();
    database = mongo.get("database", "kkcoin").asString();
    m_mongo_database = database;

    char uri[1280];
    if (!username.length() || !password.length())
    {
        sprintf(uri, "mongodb://%s/%s%s", hosts.c_str(), database.c_str(), slave.c_str());
    }
    else
    {
        std::string real_passwd = real_password(password);
        sprintf(uri, "mongodb://%s:%s@%s/%s%s", username.c_str(), real_passwd.c_str(), hosts.c_str(), database.c_str(), slave.c_str());
    }

    mongo_ = mongoc_client_new(uri);
    if (!mongo_)
    {
        die("Failed to parse URI");
    }
    else
    {
        LOG(INFO) << "mongodb Init Ok!";
    }
}

void Store::InitUsdMongo() {
    Config config;

    std::string username;
    std::string password;
    std::string database;
    std::string rsname ="";

    const Json::Value mongo = config["mongo_usd"];
    if (mongo.empty())
    {
        die("missing mongo config");
    }

    const Json::Value hosts_config = mongo["hosts"];
    std::string hosts = "";
    std::string slave = "";
    unsigned int hosts_len = hosts_config.size();
    for (unsigned i = 0; i < hosts_len; ++i)
    {
        hosts = hosts + hosts_config[i]["host"].asString() + ":" + hosts_config[i]["port"].asString();
        if (i < hosts_len - 1)
        {
            hosts = hosts + ",";
        }
    }
    if (hosts_len <= 0)
    {
        die("missing mongo hosts config");
    }

    rsname = mongo.get("rsname", "").asString();

    if (hosts_len > 1)
    {
        slave = "?";
        if(rsname.length() >0)
        {
            slave = "?replicaSet="+rsname;
        }
    }
    
    
    username = mongo.get("username", "").asString();
    password = mongo.get("password", "").asString();
    database = mongo.get("database", "kkcoin").asString();
    m_mongo_usd_database = database;

    char uri[1280];
    if (!username.length() || !password.length())
    {
        sprintf(uri, "mongodb://%s/%s%s", hosts.c_str(), database.c_str(), slave.c_str());
    }
    else
    {
        std::string real_passwd = real_password(password);
        sprintf(uri, "mongodb://%s:%s@%s/%s%s", username.c_str(), real_passwd.c_str(), hosts.c_str(), database.c_str(), slave.c_str());
    }

    mongo_usd_ = mongoc_client_new(uri);
    if (!mongo_usd_)
    {
        die("Failed to parse URI");
    }
    else
    {
        LOG(INFO) << "mongodb Init Ok!";
    }
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
        data[i].contract_id = values[i].get("contract_id", 0).asInt();
        data[i].contract_id_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].contract_name, values[i].get("contract_name", "").asCString());
        data[i].contract_name_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].asset_symbol, values[i].get("asset_symbol", "").asCString());
        data[i].asset_symbol_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].unit_amount, values[i].get("unit_amount", "0.0").asCString());
        data[i].unit_amount_ind = STMT_INDICATOR_NTS;
        data[i].lever_rate = values[i].get("lever_rate", 0).asInt();
        data[i].lever_rate_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].order_id, values[i]["id"].asCString());
        data[i].order_id_ind = STMT_INDICATOR_NTS;
        data[i].user_id = values[i].get("user_id", 0).asInt64();
        data[i].user_id_ind = STMT_INDICATOR_NONE;
        data[i].type = values[i].get("order_type", 0).asInt();
        data[i].type_ind = STMT_INDICATOR_NONE;
        data[i].isbbo = values[i].get("isbbo", 0).asInt();
        data[i].isbbo_ind = STMT_INDICATOR_NONE;
        data[i].side = values[i].get("order_op", 0).asInt();
        data[i].side_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].price, values[i].get("price", "0.0").asCString());
        data[i].price_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].origin_amount, values[i].get("origin_amount", "0.0").asCString());
        data[i].origin_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].take_profit, values[i].get("profit_limit", "0.0").asCString());
        data[i].take_profit_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].stop_loss, values[i].get("lose_limit", "0.0").asCString());
        data[i].stop_loss_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].executed_price, values[i]["executed_price"].asCString());
        data[i].executed_price_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].executed_amount, values[i]["executed_amount"].asCString());
        data[i].executed_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].executed_quote_amount, values[i]["executed_quote_amount"].asCString());
        data[i].executed_quote_amount_ind = STMT_INDICATOR_NTS;
        data[i].system_type = values[i].get("system_type", 1).asInt();
        data[i].system_type_ind = STMT_INDICATOR_NONE;
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

    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * 22);
    memset(bind, 0, sizeof(MYSQL_BIND) * 22);

    // int
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = (void*)&data[0].contract_id;
    bind[0].u.indicator = &data[0].contract_id_ind;
    // string
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = (void*)&data[0].contract_name;
    bind[1].u.indicator = &data[0].contract_name_ind;
    // string
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = (void*)&data[0].asset_symbol;
    bind[2].u.indicator = &data[0].asset_symbol_ind;
    // decimal
    bind[3].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[3].buffer = (void*)&data[0].unit_amount;
    bind[3].u.indicator = &data[0].unit_amount_ind;
    // tinyint
    bind[4].buffer_type = MYSQL_TYPE_TINY;
    bind[4].buffer = (void*)&data[0].lever_rate;
    bind[4].u.indicator = &data[0].lever_rate_ind;
    // string
    bind[5].buffer_type = MYSQL_TYPE_STRING;
    bind[5].buffer = (void*)&data[0].order_id;
    bind[5].u.indicator = &data[0].order_id_ind;
    // bigint
    bind[6].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[6].buffer = (void*)&data[0].user_id;
    bind[6].u.indicator = &data[0].user_id_ind;
    // tinyint
    bind[7].buffer_type = MYSQL_TYPE_TINY;
    bind[7].buffer = (void*)&data[0].type;
    bind[7].u.indicator = &data[0].type_ind;
    // tinyint
    bind[8].buffer_type = MYSQL_TYPE_TINY;
    bind[8].buffer = (void*)&data[0].isbbo;
    bind[8].u.indicator = &data[0].isbbo_ind;
    // tinyint
    bind[9].buffer_type = MYSQL_TYPE_TINY;
    bind[9].buffer = (void*)&data[0].side;
    bind[9].u.indicator = &data[0].side_ind;
    // decimal
    bind[10].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[10].buffer = (void*)&data[0].price;
    bind[10].u.indicator = &data[0].price_ind;
    // decimal
    bind[11].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[11].buffer = &data[0].origin_amount;
    bind[11].u.indicator = &data[0].origin_amount_ind;
    // decimal
    bind[12].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[12].buffer = &data[0].take_profit;
    bind[12].u.indicator = &data[0].take_profit_ind;
    // decimal
    bind[13].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[13].buffer = &data[0].stop_loss;
    bind[13].u.indicator = &data[0].stop_loss_ind;
    // decimal
    bind[14].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[14].buffer = (void*)&data[0].executed_price;
    bind[14].u.indicator = &data[0].executed_price_ind;
    // decimal
    bind[15].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[15].buffer = (void*)&data[0].executed_amount;
    bind[15].u.indicator = &data[0].executed_amount_ind;
    // decimal
    bind[16].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[16].buffer = (void*)&data[0].executed_quote_amount;
    bind[16].u.indicator = &data[0].executed_quote_amount_ind;
    // tinyint
    bind[17].buffer_type = MYSQL_TYPE_TINY;
    bind[17].buffer = (void*)&data[0].system_type;
    bind[17].u.indicator = &data[0].system_type_ind;
    // tinyint
    bind[18].buffer_type = MYSQL_TYPE_TINY;
    bind[18].buffer = (void*)&data[0].status;
    bind[18].u.indicator = &data[0].status_ind;
    // tinyint
    bind[19].buffer_type = MYSQL_TYPE_TINY;
    bind[19].buffer = (void*)&data[0].source;
    bind[19].u.indicator = &data[0].source_ind;
    // string
    bind[20].buffer_type = MYSQL_TYPE_STRING;
    bind[20].buffer = (void*)&data[0].ip;
    bind[20].u.indicator = &data[0].ip_ind;
    // datetime
    bind[21].buffer_type = MYSQL_TYPE_DATETIME;
    bind[21].buffer = (void*)&data[0].created_at;
    bind[21].u.indicator = &data[0].created_at_ind;

    std::string sql = "INSERT INTO t_order (contract_id, contract_name, asset_symbol, unit_amount, lever_rate, order_id, user_id, "
                      "type, is_bbo, side, price, origin_amount, take_profit, stop_loss, executed_price, executed_amount, executed_quote_amount, system_type, "
                      "status, source, ip, created_at) "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                      "ON DUPLICATE KEY UPDATE executed_price=VALUES(executed_price), executed_amount=VALUES(executed_amount), "
                      "executed_quote_amount=VALUES(executed_quote_amount), status=VALUES(status), updated_at=VALUES(created_at)";

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

bool Store::BulkInsertAreaOrder(Json::Value& value) {
    Json::Value values = Json::Value(Json::arrayValue);
    if (value.isArray()) {
        values = value;
    } else {
        values.append(value);
    }

    ORDER_AREA_DATA* data = (ORDER_AREA_DATA*)malloc(sizeof(ORDER_AREA_DATA) * values.size());
    memset(data, 0, sizeof(ORDER_AREA_DATA) * values.size());
    unsigned int array_size = values.size();
    size_t row_size = sizeof(ORDER_AREA_DATA);

    for (unsigned i = 0; i < values.size(); i++) {
        data[i].contract_id = values[i].get("contract_id", 0).asInt();
        data[i].contract_id_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].contract_name, values[i].get("contract_name", "").asCString());
        data[i].contract_name_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].asset_symbol, values[i].get("asset_symbol", "").asCString());
        data[i].asset_symbol_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].unit_amount, values[i].get("unit_amount", "0.0").asCString());
        data[i].unit_amount_ind = STMT_INDICATOR_NTS;
        data[i].lever_rate = values[i].get("lever_rate", 0).asInt();
        data[i].lever_rate_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].trading_area, values[i].get("settle_asset", "").asCString());
        data[i].trading_area_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].order_id, values[i]["id"].asCString());
        data[i].order_id_ind = STMT_INDICATOR_NTS;
        data[i].user_id = values[i].get("user_id", 0).asInt64();
        data[i].user_id_ind = STMT_INDICATOR_NONE;
        data[i].type = values[i].get("order_type", 0).asInt();
        data[i].type_ind = STMT_INDICATOR_NONE;
        data[i].isbbo = values[i].get("isbbo", 0).asInt();
        data[i].isbbo_ind = STMT_INDICATOR_NONE;
        data[i].side = values[i].get("order_op", 0).asInt();
        data[i].side_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].price, values[i].get("price", "0.0").asCString());
        data[i].price_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].origin_amount, values[i].get("origin_amount", "0.0").asCString());
        data[i].origin_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].take_profit, values[i].get("profit_limit", "0.0").asCString());
        data[i].take_profit_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].stop_loss, values[i].get("lose_limit", "0.0").asCString());
        data[i].stop_loss_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].executed_price, values[i]["executed_price"].asCString());
        data[i].executed_price_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].executed_amount, values[i]["executed_amount"].asCString());
        data[i].executed_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].executed_quote_amount, values[i]["executed_quote_amount"].asCString());
        data[i].executed_quote_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].executed_settle_amount, values[i]["executed_settle_amount"].asCString());
        data[i].executed_settle_amount_ind = STMT_INDICATOR_NTS;
        data[i].system_type = values[i].get("system_type", 1).asInt();
        data[i].system_type_ind = STMT_INDICATOR_NONE;
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

    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * 24);
    memset(bind, 0, sizeof(MYSQL_BIND) * 24);

    // int
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = (void*)&data[0].contract_id;
    bind[0].u.indicator = &data[0].contract_id_ind;
    // string
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = (void*)&data[0].contract_name;
    bind[1].u.indicator = &data[0].contract_name_ind;
    // string
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = (void*)&data[0].asset_symbol;
    bind[2].u.indicator = &data[0].asset_symbol_ind;
    // decimal
    bind[3].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[3].buffer = (void*)&data[0].unit_amount;
    bind[3].u.indicator = &data[0].unit_amount_ind;
    // tinyint
    bind[4].buffer_type = MYSQL_TYPE_TINY;
    bind[4].buffer = (void*)&data[0].lever_rate;
    bind[4].u.indicator = &data[0].lever_rate_ind;
    // string
    bind[5].buffer_type = MYSQL_TYPE_STRING;
    bind[5].buffer = (void*)&data[0].trading_area;
    bind[5].u.indicator = &data[0].trading_area_ind;
    // string
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = (void*)&data[0].order_id;
    bind[6].u.indicator = &data[0].order_id_ind;
    // bigint
    bind[7].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[7].buffer = (void*)&data[0].user_id;
    bind[7].u.indicator = &data[0].user_id_ind;
    // tinyint
    bind[8].buffer_type = MYSQL_TYPE_TINY;
    bind[8].buffer = (void*)&data[0].type;
    bind[8].u.indicator = &data[0].type_ind;
    // tinyint
    bind[9].buffer_type = MYSQL_TYPE_TINY;
    bind[9].buffer = (void*)&data[0].isbbo;
    bind[9].u.indicator = &data[0].isbbo_ind;
    // tinyint
    bind[10].buffer_type = MYSQL_TYPE_TINY;
    bind[10].buffer = (void*)&data[0].side;
    bind[10].u.indicator = &data[0].side_ind;
    // decimal
    bind[11].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[11].buffer = (void*)&data[0].price;
    bind[11].u.indicator = &data[0].price_ind;
    // decimal
    bind[12].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[12].buffer = &data[0].origin_amount;
    bind[12].u.indicator = &data[0].origin_amount_ind;
    // decimal
    bind[13].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[13].buffer = &data[0].take_profit;
    bind[13].u.indicator = &data[0].take_profit_ind;
    // decimal
    bind[14].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[14].buffer = &data[0].stop_loss;
    bind[14].u.indicator = &data[0].stop_loss_ind;
    // decimal
    bind[15].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[15].buffer = (void*)&data[0].executed_price;
    bind[15].u.indicator = &data[0].executed_price_ind;
    // decimal
    bind[16].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[16].buffer = (void*)&data[0].executed_amount;
    bind[16].u.indicator = &data[0].executed_amount_ind;
    // decimal
    bind[17].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[17].buffer = (void*)&data[0].executed_quote_amount;
    bind[17].u.indicator = &data[0].executed_quote_amount_ind;
    // decimal
    bind[18].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[18].buffer = (void*)&data[0].executed_settle_amount;
    bind[18].u.indicator = &data[0].executed_settle_amount_ind;
    // tinyint
    bind[19].buffer_type = MYSQL_TYPE_TINY;
    bind[19].buffer = (void*)&data[0].system_type;
    bind[19].u.indicator = &data[0].system_type_ind;
    // tinyint
    bind[20].buffer_type = MYSQL_TYPE_TINY;
    bind[20].buffer = (void*)&data[0].status;
    bind[20].u.indicator = &data[0].status_ind;
    // tinyint
    bind[21].buffer_type = MYSQL_TYPE_TINY;
    bind[21].buffer = (void*)&data[0].source;
    bind[21].u.indicator = &data[0].source_ind;
    // string
    bind[22].buffer_type = MYSQL_TYPE_STRING;
    bind[22].buffer = (void*)&data[0].ip;
    bind[22].u.indicator = &data[0].ip_ind;
    // datetime
    bind[23].buffer_type = MYSQL_TYPE_DATETIME;
    bind[23].buffer = (void*)&data[0].created_at;
    bind[23].u.indicator = &data[0].created_at_ind;

    std::string sql = "INSERT INTO t_order (contract_id, contract_name, asset_symbol, unit_amount, lever_rate, trading_area, order_id, user_id, "
                      "type, is_bbo, side, price, origin_amount, take_profit, stop_loss, executed_price, executed_amount, executed_quote_amount, executed_settle_amount, system_type, "
                      "status, source, ip, created_at) "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                      "ON DUPLICATE KEY UPDATE executed_price=VALUES(executed_price), executed_amount=VALUES(executed_amount), "
                      "executed_quote_amount=VALUES(executed_quote_amount), executed_settle_amount=VALUES(executed_settle_amount), status=VALUES(status), updated_at=VALUES(created_at)";

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
        data[i].contract_id = values[i].get("contract_id", 0).asInt();
        data[i].contract_id_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].contract_name, values[i].get("contract_name", "").asCString());
        data[i].contract_name_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].asset_symbol, values[i].get("asset_symbol", "").asCString());
        data[i].asset_symbol_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].unit_amount, values[i].get("unit_amount", "0.0").asCString());
        data[i].unit_amount_ind = STMT_INDICATOR_NTS;
        data[i].lever_rate = values[i].get("lever_rate", 0).asInt();
        data[i].lever_rate_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].trade_id, values[i]["id"].asCString());
        data[i].trade_id_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].order_id, values[i]["order_id"].asCString());
        data[i].order_id_ind = STMT_INDICATOR_NTS;
        data[i].user_id = values[i]["user_id"].asInt64();
        data[i].user_id_ind = STMT_INDICATOR_NONE;
        data[i].side = values[i]["order_op"].asInt();
        data[i].side_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].price, values[i]["price"].asCString());
        data[i].price_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].amount, values[i]["amount"].asCString());
        data[i].amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].quote_amount, values[i]["quote_amount"].asCString());
        data[i].quote_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].profit, values[i].get("profit", "0.0").asCString());
        data[i].profit_ind = STMT_INDICATOR_NTS;
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

    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * 17);
    memset(bind, 0, sizeof(MYSQL_BIND) * 17);

    // int
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = (void*)&data[0].contract_id;
    bind[0].u.indicator = &data[0].contract_id_ind;
    // string
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = (void*)&data[0].contract_name;
    bind[1].u.indicator = &data[0].contract_name_ind;
    // string
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = (void*)&data[0].asset_symbol;
    bind[2].u.indicator = &data[0].asset_symbol_ind;
    // decimal
    bind[3].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[3].buffer = (void*)&data[0].unit_amount;
    bind[3].u.indicator = &data[0].unit_amount_ind;
    // tinyint
    bind[4].buffer_type = MYSQL_TYPE_TINY;
    bind[4].buffer = (void*)&data[0].lever_rate;
    bind[4].u.indicator = &data[0].lever_rate_ind;
    // string
    bind[5].buffer_type = MYSQL_TYPE_STRING;
    bind[5].buffer = (void*)&data[0].trade_id;
    bind[5].u.indicator = &data[0].trade_id_ind;
    // string
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = (void*)&data[0].order_id;
    bind[6].u.indicator = &data[0].order_id_ind;
    // bigint
    bind[7].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[7].buffer = (void*)&data[0].user_id;
    bind[7].u.indicator = &data[0].user_id_ind;
    // tinyint
    bind[8].buffer_type = MYSQL_TYPE_TINY;
    bind[8].buffer = (void*)&data[0].side;
    bind[8].u.indicator = &data[0].side_ind;
    // decimal
    bind[9].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[9].buffer = (void*)&data[0].price;
    bind[9].u.indicator = &data[0].price_ind;
    // decimal
    bind[10].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[10].buffer = (void*)&data[0].amount;
    bind[10].u.indicator = &data[0].amount_ind;
    // decimal
    bind[11].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[11].buffer = (void*)&data[0].quote_amount;
    bind[11].u.indicator = &data[0].quote_amount_ind;
    // decimal
    bind[12].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[12].buffer = (void*)&data[0].profit;
    bind[12].u.indicator = &data[0].profit_ind;
    // decimal
    bind[13].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[13].buffer = (void*)&data[0].fee;
    bind[13].u.indicator = &data[0].fee_ind;
    // string
    bind[14].buffer_type = MYSQL_TYPE_STRING;
    bind[14].buffer = (void*)&data[0].fee_asset;
    bind[14].u.indicator = &data[0].fee_asset_ind;
    // tinyint
    bind[15].buffer_type = MYSQL_TYPE_TINY;
    bind[15].buffer = (void*)&data[0].is_maker;
    bind[15].u.indicator = &data[0].is_maker_ind;
    // datetime
    bind[16].buffer_type = MYSQL_TYPE_DATETIME;
    bind[16].buffer = (void*)&data[0].created_at;
    bind[16].u.indicator = &data[0].created_at_ind;

    std::string sql = "INSERT INTO t_trade (contract_id, contract_name, asset_symbol, unit_amount, lever_rate, trade_id, "
                      "order_id, user_id, side, price, amount, quote_amount, profit, fee, fee_asset, is_maker, created_at) "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
    return true;

stmt_failure:
    show_stmt_error(stmt);
    mysql_stmt_close(stmt);

failure:
    free(data);
    free(bind);
    return false;
}

bool Store::BulkInsertAreaTrade(Json::Value& value) {
    Json::Value values = Json::Value(Json::arrayValue);
    if (value.isArray()) {
        values = value;
    } else {
        values.append(value);
    }

    TRADE_AREA_DATA* data = (TRADE_AREA_DATA*)malloc(sizeof(TRADE_AREA_DATA) * values.size());
    memset(data, 0, sizeof(TRADE_AREA_DATA) * values.size());
    unsigned int array_size = values.size();
    size_t row_size = sizeof(TRADE_AREA_DATA);

    for (unsigned i = 0; i < values.size(); i++) {
        data[i].contract_id = values[i].get("contract_id", 0).asInt();
        data[i].contract_id_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].contract_name, values[i].get("contract_name", "").asCString());
        data[i].contract_name_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].asset_symbol, values[i].get("asset_symbol", "").asCString());
        data[i].asset_symbol_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].unit_amount, values[i].get("unit_amount", "0.0").asCString());
        data[i].unit_amount_ind = STMT_INDICATOR_NTS;
        data[i].lever_rate = values[i].get("lever_rate", 0).asInt();
        data[i].lever_rate_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].trading_area, values[i].get("settle_asset", "").asCString());
        data[i].trading_area_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].trade_id, values[i]["id"].asCString());
        data[i].trade_id_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].order_id, values[i]["order_id"].asCString());
        data[i].order_id_ind = STMT_INDICATOR_NTS;
        data[i].user_id = values[i]["user_id"].asInt64();
        data[i].user_id_ind = STMT_INDICATOR_NONE;
        data[i].side = values[i]["order_op"].asInt();
        data[i].side_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].price, values[i]["price"].asCString());
        data[i].price_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].amount, values[i]["amount"].asCString());
        data[i].amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].quote_amount, values[i]["quote_amount"].asCString());
        data[i].quote_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].settle_amount, values[i]["settle_amount"].asCString());
        data[i].settle_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].profit, values[i].get("profit", "0.0").asCString());
        data[i].profit_ind = STMT_INDICATOR_NTS;
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

    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * 19);
    memset(bind, 0, sizeof(MYSQL_BIND) * 19);

    // int
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = (void*)&data[0].contract_id;
    bind[0].u.indicator = &data[0].contract_id_ind;
    // string
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = (void*)&data[0].contract_name;
    bind[1].u.indicator = &data[0].contract_name_ind;
    // string
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = (void*)&data[0].asset_symbol;
    bind[2].u.indicator = &data[0].asset_symbol_ind;
    // decimal
    bind[3].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[3].buffer = (void*)&data[0].unit_amount;
    bind[3].u.indicator = &data[0].unit_amount_ind;
    // tinyint
    bind[4].buffer_type = MYSQL_TYPE_TINY;
    bind[4].buffer = (void*)&data[0].lever_rate;
    bind[4].u.indicator = &data[0].lever_rate_ind;
    // string
    bind[5].buffer_type = MYSQL_TYPE_STRING;
    bind[5].buffer = (void*)&data[0].trading_area;
    bind[5].u.indicator = &data[0].trading_area_ind;
    // string
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = (void*)&data[0].trade_id;
    bind[6].u.indicator = &data[0].trade_id_ind;
    // string
    bind[7].buffer_type = MYSQL_TYPE_STRING;
    bind[7].buffer = (void*)&data[0].order_id;
    bind[7].u.indicator = &data[0].order_id_ind;
    // bigint
    bind[8].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[8].buffer = (void*)&data[0].user_id;
    bind[8].u.indicator = &data[0].user_id_ind;
    // tinyint
    bind[9].buffer_type = MYSQL_TYPE_TINY;
    bind[9].buffer = (void*)&data[0].side;
    bind[9].u.indicator = &data[0].side_ind;
    // decimal
    bind[10].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[10].buffer = (void*)&data[0].price;
    bind[10].u.indicator = &data[0].price_ind;
    // decimal
    bind[11].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[11].buffer = (void*)&data[0].amount;
    bind[11].u.indicator = &data[0].amount_ind;
    // decimal
    bind[12].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[12].buffer = (void*)&data[0].quote_amount;
    bind[12].u.indicator = &data[0].quote_amount_ind;
    // decimal
    bind[13].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[13].buffer = (void*)&data[0].settle_amount;
    bind[13].u.indicator = &data[0].settle_amount_ind;
    // decimal
    bind[14].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[14].buffer = (void*)&data[0].profit;
    bind[14].u.indicator = &data[0].profit_ind;
    // decimal
    bind[15].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[15].buffer = (void*)&data[0].fee;
    bind[15].u.indicator = &data[0].fee_ind;
    // string
    bind[16].buffer_type = MYSQL_TYPE_STRING;
    bind[16].buffer = (void*)&data[0].fee_asset;
    bind[16].u.indicator = &data[0].fee_asset_ind;
    // tinyint
    bind[17].buffer_type = MYSQL_TYPE_TINY;
    bind[17].buffer = (void*)&data[0].is_maker;
    bind[17].u.indicator = &data[0].is_maker_ind;
    // datetime
    bind[18].buffer_type = MYSQL_TYPE_DATETIME;
    bind[18].buffer = (void*)&data[0].created_at;
    bind[18].u.indicator = &data[0].created_at_ind;

    std::string sql = "INSERT INTO t_trade (contract_id, contract_name, asset_symbol, unit_amount, lever_rate, trading_area, trade_id, "
                      "order_id, user_id, side, price, amount, quote_amount, settle_amount, profit, fee, fee_asset, is_maker, created_at) "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
    return true;

stmt_failure:
    show_stmt_error(stmt);
    mysql_stmt_close(stmt);

failure:
    free(data);
    free(bind);
    return false;
}

bool Store::BulkInsertPosition(Json::Value& value) {
    Json::Value values = Json::Value(Json::arrayValue);
    if (value.isArray()) {
        values = value;
    } else {
        values.append(value);
    }

    POSITION_DATA* data = (POSITION_DATA*)malloc(sizeof(POSITION_DATA) * values.size());
    memset(data, 0, sizeof(POSITION_DATA) * values.size());
    unsigned int array_size = values.size();
    size_t row_size = sizeof(POSITION_DATA);

    for (unsigned i = 0; i < values.size(); i++) {
        data[i].user_id = values[i]["user_id"].asInt64();
        data[i].user_id_ind = STMT_INDICATOR_NONE;
        data[i].contract_id = values[i].get("contract_id", 0).asInt();
        data[i].contract_id_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].contract_name, values[i].get("contract_name", "").asCString());
        data[i].contract_name_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].asset_symbol, values[i].get("asset_symbol", "").asCString());
        data[i].asset_symbol_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].unit_amount, values[i].get("unit_amount", "0.0").asCString());
        data[i].unit_amount_ind = STMT_INDICATOR_NTS;
        data[i].lever_rate = values[i].get("lever_rate", 0).asInt();
        data[i].lever_rate_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].buy_frozen, values[i]["buy_frozen"].asCString());
        data[i].buy_frozen_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_margin, values[i]["buy_margin"].asCString());
        data[i].buy_margin_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_amount, values[i]["buy_amount"].asCString());
        data[i].buy_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_available, values[i]["buy_available"].asCString());
        data[i].buy_available_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_quote_amount, values[i]["buy_quote_amount"].asCString());
        data[i].buy_quote_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_quote_amount_settle, values[i]["buy_quote_amount_settle"].asCString());
        data[i].buy_quote_amount_settle_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_take_profit, values[i]["buy_take_profit"].asCString());
        data[i].buy_take_profit_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_stop_loss, values[i]["buy_stop_loss"].asCString());
        data[i].buy_stop_loss_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_frozen, values[i]["sell_frozen"].asCString());
        data[i].sell_frozen_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_margin, values[i]["sell_margin"].asCString());
        data[i].sell_margin_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_amount, values[i]["sell_amount"].asCString());
        data[i].sell_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_available, values[i]["sell_available"].asCString());
        data[i].sell_available_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_quote_amount, values[i]["sell_quote_amount"].asCString());
        data[i].sell_quote_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_quote_amount_settle, values[i]["sell_quote_amount_settle"].asCString());
        data[i].sell_quote_amount_settle_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_take_profit, values[i]["sell_take_profit"].asCString());
        data[i].sell_take_profit_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_stop_loss, values[i]["sell_stop_loss"].asCString());
        data[i].sell_stop_loss_ind = STMT_INDICATOR_NTS;

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

    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * 23);
    memset(bind, 0, sizeof(MYSQL_BIND) * 23);

    // bigint
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = (void*)&data[0].user_id;
    bind[0].u.indicator = &data[0].user_id_ind;
    // int
    bind[1].buffer_type = MYSQL_TYPE_LONG;
    bind[1].buffer = (void*)&data[0].contract_id;
    bind[1].u.indicator = &data[0].contract_id_ind;
    // string
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = (void*)&data[0].contract_name;
    bind[2].u.indicator = &data[0].contract_name_ind;
    // string
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = (void*)&data[0].asset_symbol;
    bind[3].u.indicator = &data[0].asset_symbol_ind;
    // decimal
    bind[4].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[4].buffer = (void*)&data[0].unit_amount;
    bind[4].u.indicator = &data[0].unit_amount_ind;
    // tinyint
    bind[5].buffer_type = MYSQL_TYPE_TINY;
    bind[5].buffer = (void*)&data[0].lever_rate;
    bind[5].u.indicator = &data[0].lever_rate_ind;
    // decimal
    bind[6].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[6].buffer = (void*)&data[0].buy_frozen;
    bind[6].u.indicator = &data[0].buy_frozen_ind;
    // decimal
    bind[7].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[7].buffer = (void*)&data[0].buy_margin;
    bind[7].u.indicator = &data[0].buy_margin_ind;
    // decimal
    bind[8].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[8].buffer = (void*)&data[0].buy_amount;
    bind[8].u.indicator = &data[0].buy_amount_ind;
    // decimal
    bind[9].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[9].buffer = (void*)&data[0].buy_available;
    bind[9].u.indicator = &data[0].buy_available_ind;
    // decimal
    bind[10].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[10].buffer = (void*)&data[0].buy_quote_amount;
    bind[10].u.indicator = &data[0].buy_quote_amount_ind;
    // decimal
    bind[11].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[11].buffer = (void*)&data[0].buy_quote_amount_settle;
    bind[11].u.indicator = &data[0].buy_quote_amount_settle_ind;
    // decimal
    bind[12].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[12].buffer = (void*)&data[0].buy_take_profit;
    bind[12].u.indicator = &data[0].buy_take_profit_ind;
    // decimal
    bind[13].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[13].buffer = (void*)&data[0].buy_stop_loss;
    bind[13].u.indicator = &data[0].buy_stop_loss_ind;
    // decimal
    bind[14].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[14].buffer = (void*)&data[0].sell_frozen;
    bind[14].u.indicator = &data[0].sell_frozen_ind;
    // decimal
    bind[15].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[15].buffer = (void*)&data[0].sell_margin;
    bind[15].u.indicator = &data[0].sell_margin_ind;
    // decimal
    bind[16].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[16].buffer = (void*)&data[0].sell_amount;
    bind[16].u.indicator = &data[0].sell_amount_ind;
    // decimal
    bind[17].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[17].buffer = (void*)&data[0].sell_available;
    bind[17].u.indicator = &data[0].sell_available_ind;
    // decimal
    bind[18].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[18].buffer = (void*)&data[0].sell_quote_amount;
    bind[18].u.indicator = &data[0].sell_quote_amount_ind;
    // decimal
    bind[19].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[19].buffer = (void*)&data[0].sell_quote_amount_settle;
    bind[19].u.indicator = &data[0].sell_quote_amount_settle_ind;
    // decimal
    bind[20].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[20].buffer = (void*)&data[0].sell_take_profit;
    bind[20].u.indicator = &data[0].sell_take_profit_ind;
    // decimal
    bind[21].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[21].buffer = (void*)&data[0].sell_stop_loss;
    bind[21].u.indicator = &data[0].sell_stop_loss_ind;
    // datetime
    bind[22].buffer_type = MYSQL_TYPE_DATETIME;
    bind[22].buffer = (void*)&data[0].created_at;
    bind[22].u.indicator = &data[0].created_at_ind;

    std::string sql = "INSERT INTO t_position (user_id, contract_id, contract_name, asset_symbol, unit_amount, lever_rate, "
                      "buy_frozen, buy_margin, buy_amount, buy_available, buy_quote_amount, buy_quote_amount_settle, buy_take_profit, buy_stop_loss, "
                      "sell_frozen, sell_margin, sell_amount, sell_available, sell_quote_amount, sell_quote_amount_settle, sell_take_profit, sell_stop_loss, created_at) "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                      "ON DUPLICATE KEY UPDATE buy_frozen=VALUES(buy_frozen), buy_margin=VALUES(buy_margin), buy_amount=VALUES(buy_amount), "
                      "buy_available=VALUES(buy_available), buy_quote_amount=VALUES(buy_quote_amount), buy_quote_amount_settle=VALUES(buy_quote_amount_settle), "
                      "buy_take_profit=VALUES(buy_take_profit), buy_stop_loss=VALUES(buy_stop_loss), "
                      "sell_frozen=VALUES(sell_frozen), sell_margin=VALUES(sell_margin), sell_amount=VALUES(sell_amount), "
                      "sell_available=VALUES(sell_available), sell_quote_amount=VALUES(sell_quote_amount), sell_quote_amount_settle=VALUES(sell_quote_amount_settle), "
                      "sell_take_profit=VALUES(sell_take_profit), sell_stop_loss=VALUES(sell_stop_loss), "
                      "updated_at=VALUES(created_at)";

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

bool Store::BulkInsertAreaPosition(Json::Value& value) {
    Json::Value values = Json::Value(Json::arrayValue);
    if (value.isArray()) {
        values = value;
    } else {
        values.append(value);
    }

    POSITION_AREA_DATA* data = (POSITION_AREA_DATA*)malloc(sizeof(POSITION_AREA_DATA) * values.size());
    memset(data, 0, sizeof(POSITION_AREA_DATA) * values.size());
    unsigned int array_size = values.size();
    size_t row_size = sizeof(POSITION_AREA_DATA);

    for (unsigned i = 0; i < values.size(); i++) {
        data[i].user_id = values[i]["user_id"].asInt64();
        data[i].user_id_ind = STMT_INDICATOR_NONE;
        data[i].contract_id = values[i].get("contract_id", 0).asInt();
        data[i].contract_id_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].contract_name, values[i].get("contract_name", "").asCString());
        data[i].contract_name_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].asset_symbol, values[i].get("asset_symbol", "").asCString());
        data[i].asset_symbol_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].unit_amount, values[i].get("unit_amount", "0.0").asCString());
        data[i].unit_amount_ind = STMT_INDICATOR_NTS;
        data[i].lever_rate = values[i].get("lever_rate", 0).asInt();
        data[i].lever_rate_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].trading_area, values[i].get("settle_asset", "").asCString());
        data[i].trading_area_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_frozen, values[i]["buy_frozen"].asCString());
        data[i].buy_frozen_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_margin, values[i]["buy_margin"].asCString());
        data[i].buy_margin_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_amount, values[i]["buy_amount"].asCString());
        data[i].buy_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_available, values[i]["buy_available"].asCString());
        data[i].buy_available_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_quote_amount, values[i]["buy_quote_amount"].asCString());
        data[i].buy_quote_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_quote_amount_settle, values[i]["buy_quote_amount_settle"].asCString());
        data[i].buy_quote_amount_settle_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_take_profit, values[i]["buy_take_profit"].asCString());
        data[i].buy_take_profit_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].buy_stop_loss, values[i]["buy_stop_loss"].asCString());
        data[i].buy_stop_loss_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_frozen, values[i]["sell_frozen"].asCString());
        data[i].sell_frozen_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_margin, values[i]["sell_margin"].asCString());
        data[i].sell_margin_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_amount, values[i]["sell_amount"].asCString());
        data[i].sell_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_available, values[i]["sell_available"].asCString());
        data[i].sell_available_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_quote_amount, values[i]["sell_quote_amount"].asCString());
        data[i].sell_quote_amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_quote_amount_settle, values[i]["sell_quote_amount_settle"].asCString());
        data[i].sell_quote_amount_settle_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_take_profit, values[i]["sell_take_profit"].asCString());
        data[i].sell_take_profit_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].sell_stop_loss, values[i]["sell_stop_loss"].asCString());
        data[i].sell_stop_loss_ind = STMT_INDICATOR_NTS;

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

    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * 24);
    memset(bind, 0, sizeof(MYSQL_BIND) * 24);

    // bigint
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = (void*)&data[0].user_id;
    bind[0].u.indicator = &data[0].user_id_ind;
    // int
    bind[1].buffer_type = MYSQL_TYPE_LONG;
    bind[1].buffer = (void*)&data[0].contract_id;
    bind[1].u.indicator = &data[0].contract_id_ind;
    // string
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = (void*)&data[0].contract_name;
    bind[2].u.indicator = &data[0].contract_name_ind;
    // string
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = (void*)&data[0].asset_symbol;
    bind[3].u.indicator = &data[0].asset_symbol_ind;
    // decimal
    bind[4].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[4].buffer = (void*)&data[0].unit_amount;
    bind[4].u.indicator = &data[0].unit_amount_ind;
    // tinyint
    bind[5].buffer_type = MYSQL_TYPE_TINY;
    bind[5].buffer = (void*)&data[0].lever_rate;
    bind[5].u.indicator = &data[0].lever_rate_ind;
    // string
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = (void*)&data[0].trading_area;
    bind[6].u.indicator = &data[0].trading_area_ind;
    // decimal
    bind[7].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[7].buffer = (void*)&data[0].buy_frozen;
    bind[7].u.indicator = &data[0].buy_frozen_ind;
    // decimal
    bind[8].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[8].buffer = (void*)&data[0].buy_margin;
    bind[8].u.indicator = &data[0].buy_margin_ind;
    // decimal
    bind[9].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[9].buffer = (void*)&data[0].buy_amount;
    bind[9].u.indicator = &data[0].buy_amount_ind;
    // decimal
    bind[10].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[10].buffer = (void*)&data[0].buy_available;
    bind[10].u.indicator = &data[0].buy_available_ind;
    // decimal
    bind[11].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[11].buffer = (void*)&data[0].buy_quote_amount;
    bind[11].u.indicator = &data[0].buy_quote_amount_ind;
    // decimal
    bind[12].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[12].buffer = (void*)&data[0].buy_quote_amount_settle;
    bind[12].u.indicator = &data[0].buy_quote_amount_settle_ind;
    // decimal
    bind[13].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[13].buffer = (void*)&data[0].buy_take_profit;
    bind[13].u.indicator = &data[0].buy_take_profit_ind;
    // decimal
    bind[14].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[14].buffer = (void*)&data[0].buy_stop_loss;
    bind[14].u.indicator = &data[0].buy_stop_loss_ind;
    // decimal
    bind[15].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[15].buffer = (void*)&data[0].sell_frozen;
    bind[15].u.indicator = &data[0].sell_frozen_ind;
    // decimal
    bind[16].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[16].buffer = (void*)&data[0].sell_margin;
    bind[16].u.indicator = &data[0].sell_margin_ind;
    // decimal
    bind[17].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[17].buffer = (void*)&data[0].sell_amount;
    bind[17].u.indicator = &data[0].sell_amount_ind;
    // decimal
    bind[18].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[18].buffer = (void*)&data[0].sell_available;
    bind[18].u.indicator = &data[0].sell_available_ind;
    // decimal
    bind[19].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[19].buffer = (void*)&data[0].sell_quote_amount;
    bind[19].u.indicator = &data[0].sell_quote_amount_ind;
    // decimal
    bind[20].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[20].buffer = (void*)&data[0].sell_quote_amount_settle;
    bind[20].u.indicator = &data[0].sell_quote_amount_settle_ind;
    // decimal
    bind[21].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[21].buffer = (void*)&data[0].sell_take_profit;
    bind[21].u.indicator = &data[0].sell_take_profit_ind;
    // decimal
    bind[22].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[22].buffer = (void*)&data[0].sell_stop_loss;
    bind[22].u.indicator = &data[0].sell_stop_loss_ind;
    // datetime
    bind[23].buffer_type = MYSQL_TYPE_DATETIME;
    bind[23].buffer = (void*)&data[0].created_at;
    bind[23].u.indicator = &data[0].created_at_ind;

    std::string sql = "INSERT INTO t_position (user_id, contract_id, contract_name, asset_symbol, unit_amount, lever_rate, trading_area, "
                      "buy_frozen, buy_margin, buy_amount, buy_available, buy_quote_amount, buy_quote_amount_settle, buy_take_profit, buy_stop_loss, "
                      "sell_frozen, sell_margin, sell_amount, sell_available, sell_quote_amount, sell_quote_amount_settle, sell_take_profit, sell_stop_loss, created_at) "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                      "ON DUPLICATE KEY UPDATE buy_frozen=VALUES(buy_frozen), buy_margin=VALUES(buy_margin), buy_amount=VALUES(buy_amount), "
                      "buy_available=VALUES(buy_available), buy_quote_amount=VALUES(buy_quote_amount), buy_quote_amount_settle=VALUES(buy_quote_amount_settle), "
                      "buy_take_profit=VALUES(buy_take_profit), buy_stop_loss=VALUES(buy_stop_loss), "
                      "sell_frozen=VALUES(sell_frozen), sell_margin=VALUES(sell_margin), sell_amount=VALUES(sell_amount), "
                      "sell_available=VALUES(sell_available), sell_quote_amount=VALUES(sell_quote_amount), sell_quote_amount_settle=VALUES(sell_quote_amount_settle), "
                      "sell_take_profit=VALUES(sell_take_profit), sell_stop_loss=VALUES(sell_stop_loss), "
                      "updated_at=VALUES(created_at)";

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
        strcpy(data[i].asset_symbol, values[i]["asset"].asCString());
        data[i].asset_symbol_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].balance, values[i]["new_balance"].asCString());
        data[i].balance_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].margin, values[i]["new_margin"].asCString());
        data[i].margin_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].frozen_margin, values[i]["new_frozen_margin"].asCString());
        data[i].frozen_margin_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].profit, values[i]["new_profit"].asCString());
        data[i].profit_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].balance_hash, values[i]["new_encode_balance"].asCString());
        data[i].balance_hash_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].margin_hash, values[i]["new_encode_margin"].asCString());
        data[i].margin_hash_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].frozen_margin_hash, values[i]["new_encode_frozen_margin"].asCString());
        data[i].frozen_margin_hash_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].profit_hash, values[i]["new_encode_profit"].asCString());
        data[i].profit_hash_ind = STMT_INDICATOR_NTS;

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

    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * 11);
    memset(bind, 0, sizeof(MYSQL_BIND) * 11);

    // bigint
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = (void*)&data[0].user_id;
    bind[0].u.indicator = &data[0].user_id_ind;
    // string
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = (void*)&data[0].asset_symbol;
    bind[1].u.indicator = &data[0].asset_symbol_ind;
    // decimal
    bind[2].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[2].buffer = (void*)&data[0].balance;
    bind[2].u.indicator = &data[0].balance_ind;
    // decimal
    bind[3].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[3].buffer = (void*)&data[0].margin;
    bind[3].u.indicator = &data[0].margin_ind;
    // decimal
    bind[4].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[4].buffer = (void*)&data[0].frozen_margin;
    bind[4].u.indicator = &data[0].frozen_margin_ind;
    // decimal
    bind[5].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[5].buffer = (void*)&data[0].profit;
    bind[5].u.indicator = &data[0].profit_ind;
    // string
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = (void*)&data[0].balance_hash;
    bind[6].u.indicator = &data[0].balance_hash_ind;
    // string
    bind[7].buffer_type = MYSQL_TYPE_STRING;
    bind[7].buffer = (void*)&data[0].margin_hash;
    bind[7].u.indicator = &data[0].margin_hash_ind;
    // string
    bind[8].buffer_type = MYSQL_TYPE_STRING;
    bind[8].buffer = (void*)&data[0].frozen_margin_hash;
    bind[8].u.indicator = &data[0].frozen_margin_hash_ind;
    // string
    bind[9].buffer_type = MYSQL_TYPE_STRING;
    bind[9].buffer = (void*)&data[0].profit_hash;
    bind[9].u.indicator = &data[0].profit_hash_ind;
    // datetime
    bind[10].buffer_type = MYSQL_TYPE_DATETIME;
    bind[10].buffer = (void*)&data[0].created_at;
    bind[10].u.indicator = &data[0].created_at_ind;

    std::string sql = "INSERT INTO t_account (user_id, asset_symbol, balance, margin, frozen_margin, profit, "
                      "balance_encryption, margin_encryption, frozen_margin_encryption, profit_encryption, created_at) "
                      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " 
                      "ON DUPLICATE KEY UPDATE balance=VALUES(balance), margin=VALUES(margin), frozen_margin=VALUES(frozen_margin), "
                      "profit=VALUES(profit), balance_encryption=VALUES(balance_encryption), margin_encryption=VALUES(margin_encryption), "
                      "frozen_margin_encryption=VALUES(frozen_margin_encryption), profit_encryption=VALUES(profit_encryption), "
                      "updated_at=VALUES(created_at)";

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


bool Store::BulkInsertFundingFee(Json::Value& value) {
    Json::Value values = Json::Value(Json::arrayValue);
    if (value.isArray()) {
        values = value;
    } else {
        values.append(value);
    }

    FUNDING_FEE_DATA* data = (FUNDING_FEE_DATA*)malloc(sizeof(FUNDING_FEE_DATA) * values.size());
    memset(data, 0, sizeof(FUNDING_FEE_DATA) * values.size());
    unsigned int array_size = values.size();
    size_t row_size = sizeof(FUNDING_FEE_DATA);

    for (unsigned i = 0; i < values.size(); i++) {
        data[i].user_id = values[i]["user_id"].asInt64();
        data[i].user_id_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].trading_area, values[i].get("trading_area", "").asCString());
        data[i].trading_area_ind = STMT_INDICATOR_NTS;
        data[i].contract_id = values[i].get("contract_id", 0).asInt();
        data[i].contract_id_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].contract_name, values[i].get("contract_name", "").asCString());
        data[i].contract_name_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].asset_symbol, values[i].get("asset_symbol", "").asCString());
        data[i].asset_symbol_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].unit_amount, values[i].get("unit_amount", "0.0").asCString());
        data[i].unit_amount_ind = STMT_INDICATOR_NTS;
        data[i].lever_rate = values[i].get("lever_rate", 0).asInt();
        data[i].lever_rate_ind = STMT_INDICATOR_NONE;
        data[i].position_type = values[i].get("position_type", 0).asInt();
        data[i].position_type_ind = STMT_INDICATOR_NONE;
        strcpy(data[i].amount, values[i]["amount"].asCString());
        data[i].amount_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].position_value, values[i]["position_value"].asCString());
        data[i].position_value_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].fund_rate, values[i]["fund_rate"].asCString());
        data[i].fund_rate_ind = STMT_INDICATOR_NTS;
        strcpy(data[i].fund_fee, values[i]["fund_fee"].asCString());
        data[i].fund_fee_ind = STMT_INDICATOR_NTS;

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

    // bigint
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = (void*)&data[0].user_id;
    bind[0].u.indicator = &data[0].user_id_ind;
    // string
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = (void*)&data[0].trading_area;
    bind[1].u.indicator = &data[0].trading_area_ind;
    // int
    bind[2].buffer_type = MYSQL_TYPE_LONG;
    bind[2].buffer = (void*)&data[0].contract_id;
    bind[2].u.indicator = &data[0].contract_id_ind;
    // string
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = (void*)&data[0].contract_name;
    bind[3].u.indicator = &data[0].contract_name_ind;
    // string
    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = (void*)&data[0].asset_symbol;
    bind[4].u.indicator = &data[0].asset_symbol_ind;
    // decimal
    bind[5].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[5].buffer = (void*)&data[0].unit_amount;
    bind[5].u.indicator = &data[0].unit_amount_ind;
    // tinyint
    bind[6].buffer_type = MYSQL_TYPE_TINY;
    bind[6].buffer = (void*)&data[0].lever_rate;
    bind[6].u.indicator = &data[0].lever_rate_ind;
    // tinyint
    bind[7].buffer_type = MYSQL_TYPE_TINY;
    bind[7].buffer = (void*)&data[0].position_type;
    bind[7].u.indicator = &data[0].position_type_ind;
    // decimal
    bind[8].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[8].buffer = (void*)&data[0].amount;
    bind[8].u.indicator = &data[0].amount_ind;
    // decimal
    bind[9].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[9].buffer = (void*)&data[0].position_value;
    bind[9].u.indicator = &data[0].position_value_ind;
    // decimal
    bind[10].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[10].buffer = (void*)&data[0].fund_rate;
    bind[10].u.indicator = &data[0].fund_rate_ind;
    // decimal
    bind[11].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[11].buffer = (void*)&data[0].fund_fee;
    bind[11].u.indicator = &data[0].fund_fee_ind;
    // datetime
    bind[12].buffer_type = MYSQL_TYPE_DATETIME;
    bind[12].buffer = (void*)&data[0].created_at;
    bind[12].u.indicator = &data[0].created_at_ind;

    std::string sql = "INSERT INTO t_fund_fee (user_id, trading_area, contract_id, contract_name, asset_symbol, unit_amount, lever_rate, position_type, "
                      "amount, position_value, fund_rate, fund_fee, created_at) "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
        strcpy(data[i].jnl_amount, values[i]["amount"].asCString());
        data[i].jnl_amount_ind = STMT_INDICATOR_NTS;
    }

    MYSQL_BIND* bind = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND) * 2);
    memset(bind, 0, sizeof(MYSQL_BIND) * 2);

    // string
    bind[0].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[0].buffer = (void*)&data[0].jnl_amount;
    bind[0].u.indicator = &data[0].jnl_amount_ind;
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = (void*)&data[0].jnl_id;
    bind[1].u.indicator = &data[0].jnl_id_ind;

    std::string sql = "UPDATE t_jnl_human SET status=1, true_change_amt=? WHERE txno=?";

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

    mongoc_collection_t* collection = mongoc_client_get_collection(mongo_, m_mongo_database.c_str(), "m_futures_journal");
    if (!collection)
        die("Failed to get collection");

    mongoc_bulk_operation_t* bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);

    bson_t doc;
    bson_decimal128_t change_balance;
    bson_decimal128_t new_balance;
    bson_decimal128_t change_margin;
    bson_decimal128_t new_margin;
    bson_decimal128_t change_frozen_margin;
    bson_decimal128_t new_frozen_margin;
    bson_decimal128_t change_profit;
    bson_decimal128_t new_profit;

    bool data_flag = false;
    for (unsigned i = 0; i < values.size(); i++) {
        if (maker_user_set.find(values[i]["user_id"].asInt()) != maker_user_set.end()){
            continue;
        }
        data_flag = true;

        bson_decimal128_from_string(values[i]["change_balance"].asCString(), &change_balance);
        bson_decimal128_from_string(values[i]["new_balance"].asCString(), &new_balance);
        bson_decimal128_from_string(values[i]["change_margin"].asCString(), &change_margin);
        bson_decimal128_from_string(values[i]["new_margin"].asCString(), &new_margin);
        bson_decimal128_from_string(values[i]["change_frozen_margin"].asCString(), &change_frozen_margin);
        bson_decimal128_from_string(values[i]["new_frozen_margin"].asCString(), &new_frozen_margin);
        bson_decimal128_from_string(values[i]["change_profit"].asCString(), &change_profit);
        bson_decimal128_from_string(values[i]["new_profit"].asCString(), &new_profit);

        bson_init(&doc);
        BSON_APPEND_UTF8(&doc, "txno", values[i]["id"].asCString());
        BSON_APPEND_INT64(&doc, "user_id", values[i]["user_id"].asInt64());
        BSON_APPEND_UTF8(&doc, "asset_symbol", values[i]["asset"].asCString());
        BSON_APPEND_UTF8(&doc, "contract", values[i]["contract"].asCString());
        BSON_APPEND_DECIMAL128(&doc, "balance_amt", &change_balance);
        BSON_APPEND_DECIMAL128(&doc, "balance_bal", &new_balance);
        BSON_APPEND_DECIMAL128(&doc, "margin_amt", &change_margin);
        BSON_APPEND_DECIMAL128(&doc, "margin_bal", &new_margin);
        BSON_APPEND_DECIMAL128(&doc, "frozen_margin_amt", &change_frozen_margin);
        BSON_APPEND_DECIMAL128(&doc, "frozen_margin_bal", &new_frozen_margin);
        BSON_APPEND_DECIMAL128(&doc, "profit_amt", &change_profit);
        BSON_APPEND_DECIMAL128(&doc, "profit_bal", &new_profit);
        BSON_APPEND_INT32(&doc, "type", values[i]["jnl_type"].asInt());
        BSON_APPEND_UTF8(&doc, "remark", values[i]["remark"].asCString());
        BSON_APPEND_UTF8(&doc, "remark_tw", values[i]["remark_tw"].asCString());
        BSON_APPEND_UTF8(&doc, "remark_en", values[i]["remark_en"].asCString());
        BSON_APPEND_UTF8(&doc, "remark_vi", values[i]["remark_vi"].asCString());
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

bool Store::BulkInsertMatchTradeMsg(Json::Value& value) {
    Json::Value values = Json::Value(Json::arrayValue);
    if (value.isArray()) {
        values = value;
    } else {
        values.append(value);
    }
    long long time_now = time(0);
    mongoc_collection_t* collection = mongoc_client_get_collection(mongo_usd_, m_mongo_usd_database.c_str(), "m_message");
    if (!collection)
        die("Failed to get collection");

    mongoc_bulk_operation_t* bulk = mongoc_collection_create_bulk_operation_with_opts (collection, NULL);

    bson_t doc;

    bool data_flag = false;
    for (unsigned i = 0; i < values.size(); i++) {
        if (maker_user_set.find(values[i]["user_id"].asInt()) != maker_user_set.end()){
            continue;
        }
        data_flag = true;

        bson_init(&doc);
        BSON_APPEND_INT32(&doc, "user_id", values[i]["user_id"].asInt());
        BSON_APPEND_INT32(&doc, "type", values[i]["msg_type"].asInt());
        BSON_APPEND_UTF8(&doc, "title", values[i]["title"].asCString());
        BSON_APPEND_UTF8(&doc, "title_tw", values[i]["title_tw"].asCString());
        BSON_APPEND_UTF8(&doc, "title_en", values[i]["title_en"].asCString());
        BSON_APPEND_UTF8(&doc, "title_vi", values[i]["title_vi"].asCString());
        BSON_APPEND_UTF8(&doc, "currency", values[i]["currency"].asCString());
        BSON_APPEND_DATE_TIME(&doc, "created_at", time_now * 1000);

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