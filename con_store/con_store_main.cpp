#include <iostream>

#include "config.h"
#include "logging.h"
#include "con_store_loop.h"

void InitConf(char* conf_file);
void InitLog(char* log_file);

int main(int argc, char** argv) {
    if (argc < 2) {
        printf("Usage: trade_store conf log\n");
        return 1;
    }
    
    InitConf(argv[1]);
    InitLog(argv[2]);

    StoreLoop* store_loop = new StoreLoop();
    store_loop->Init();
    store_loop->Run();
    store_loop->CleanUp();

    return 0;
}

void InitConf(char* conf_file) {
    InitConfig(conf_file);
}

void InitLog(char* log_file) {
    LoggingSettings settings;
    settings.logging_dest = LOG_TO_ALL;
    settings.log_file = log_file;
    settings.delete_old = APPEND_TO_OLD_LOG_FILE;
    InitLogging(settings);
}