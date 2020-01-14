#include <iostream>
#include <utility>
#include <vector>

#include "config.h"
#include "logging.h"
#include "trade.h"
#include "utils.h"

void InitConf(char* conf_file);
void InitLog(char* log_file);

int main(int argc, char** argv) {
    if (argc < 3) {
        printf("Usage: trade_match conf log\n");
        return 1;
    }
    
    InitConf(argv[1]);
    InitLog(argv[2]);

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
