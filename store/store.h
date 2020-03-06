#ifndef STORE_H_
#define STORE_H_

#include <mariadb/mysql.h>
#include <mongoc.h>
#include <json/json.h>
#include <string.h>
#include <set>

#define WL4435_STRING_SIZE 30

class Store {
  public:
    Store();
    ~Store();

    void Init();

    void Run();

    void CleanUp();

    bool DoWork(Json::Value& value);
    bool DoIdleWork();

    void TestInsert();

  private:
    void InitMysql();
    void InitMongo();
    bool BulkInsertOrder(Json::Value& value);
    bool BulkInsertTrade(Json::Value& value);
    bool BulkInsertAccount(Json::Value& value);
    bool BulkUpdateWithdraw(Json::Value& value);
    bool BulkUpdateJnl(Json::Value& value);
    bool BulkUpdateLock(Json::Value& value);
    bool BulkInsertJournal(Json::Value& value);
    bool BulkUpdateTrans(Json::Value& value);
    bool Del143_order(Json::Value& value);

    // bool InsertJournal_(Json::Value& value);
    Json::Value TestOrder();
    Json::Value TestTrade();
    Json::Value TestAccount();

    MYSQL* mysql_;

    mongoc_client_t* mongo_;

    uint64_t previous_ping_time_;
    std::set<int> maker_user_set;
};

#endif  // STORE_H_