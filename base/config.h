#ifndef CONFIG_H_
#define CONFIG_H_

#include <string>
#include <cstring>
#include <sstream>

#include <json/json.h>

#define		EPS									0.000000001
#define		VALUE_DECIMAL						8
#define		EPS_DECIMAL							9

#define		USER_ACCOUNT_JNL_DEPOSIT			0
#define		USER_ACCOUNT_JNL_WITHDRAW			1
#define		USER_ACCOUNT_JNL_FEE				2
#define		USER_ACCOUNT_JNL_BUY				3
#define		USER_ACCOUNT_JNL_SELL				4
#define		USER_ACCOUNT_JNL_REGISTER_SEND		5
#define		USER_ACCOUNT_JNL_DEPOSIT_SEND		6
#define		USER_ACCOUNT_JNL_HUMAN_ADD			7
#define		USER_ACCOUNT_JNL_HUMAN_REDUCE		8
#define		USER_ACCOUNT_JNL_ORDER_FROZEN		9
#define		USER_ACCOUNT_JNL_CANCEL_ORDER		10
#define		USER_ACCOUNT_JNL_WITHDRAW_FROZEN	11
#define		USER_ACCOUNT_JNL_CANCEL_WITHDRAW	12
#define		USER_ACCOUNT_JNL_LOCK_BALANCE		19
#define		USER_ACCOUNT_JNL_CANCEL_LOCK		20
#define		USER_ACCOUNT_JNL_LOCK_BALANCE_END	21
#define		USER_ACCOUNT_JNL_OPEN_LONG				23
#define		USER_ACCOUNT_JNL_OPEN_SHORT				24
#define		USER_ACCOUNT_JNL_CLOSE_LONG				25
#define		USER_ACCOUNT_JNL_CLOSE_SHORT			26

#define		USER_ACCOUNT_JNL_POSITION_SETTLE	27
#define		USER_ACCOUNT_JNL_PROFIT_SETTLE		28
#define		USER_ACCOUNT_JNL_DELAY_SETTLE			29
#define		USER_ACCOUNT_JNL_LOSE_SHARE       30
#define		USER_ACCOUNT_JNL_LOSE_ZERO        31
#define		USER_ACCOUNT_JNL_COIN_CONTRACT		32
#define		USER_ACCOUNT_JNL_CONTRACT_COIN		33

#define		USER_ACCOUNT_JNL_FUND_FEE		      37

bool InitConfig(const char * conf_file);

class Config {
  public:
    Config();

    ~Config();

    std::string& str() { return string_; }

    const Json::Value& operator[](const char* key) const;

  private:
    void Init();

    std::string string_;
    Json::Value value_;
};

#endif  // CONFIG_H_