#include <iostream>
#include <utility>
#include <vector>

#include "config.h"
#include "logging.h"
#include "trade.h"
#include "utils.h"

void InitConf(char* conf_file);
void InitLog(char* log_file);
void InitRedisLock();

int main(int argc, char** argv) {
    if (argc < 3) {
        printf("Usage: trade_match conf log\n");
        return 1;
    }
    
    InitConf(argv[1]);
    InitLog(argv[2]);
    InitRedisLock();

    Trade* trade = new Trade();
    trade->Init();
    trade->Run();
    trade->CleanUp();

    return 0;
}

void InitConf(char* conf_file) {
    InitConfig(conf_file);
}

void InitLog(char* log_file) {
    LoggingSettings settings;
    settings.logging_dest = LOG_TO_FILE;
    settings.log_file = log_file;
    settings.delete_old = APPEND_TO_OLD_LOG_FILE;
    InitLogging(settings);
}

void InitRedisLock() {
    Config config;
    const Json::Value redis_config = config["lockRedis"];
    const Json::Value sentinel_config = redis_config["sentinels"];
    std::vector<std::pair<std::string, int> > sentinels;
    for (unsigned i = 0; i < sentinel_config.size(); ++i) {
        sentinels.push_back(std::make_pair(sentinel_config[i]["host"].asString(), sentinel_config[i]["port"].asInt()));
    }
    std::string encode_password = redis_config["password"].asString();
    std::string password = real_password(encode_password);

	InitLock(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
}
