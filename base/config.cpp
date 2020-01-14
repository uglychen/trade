#include "config.h"

#include <string>

std::string* g_conf_file_name = NULL;

FILE* g_conf_file = NULL;

bool InitializeConfFileHandle() {
    if (g_conf_file)
        return true;
    
    if (!g_conf_file_name) {
        return false;
    }

    g_conf_file = fopen(g_conf_file_name->c_str(), "r");
    if (g_conf_file == NULL)
        return false;

    return true;
}

void CloseConfFile() {
    if (!g_conf_file)
        return;

    fclose(g_conf_file);
    g_conf_file = NULL;
}

bool InitConfig(const char* conf_file) {
    CloseConfFile();

    if (!g_conf_file_name)
        g_conf_file_name = new std::string();
    *g_conf_file_name = conf_file;

    return InitializeConfFileHandle();
}

Config::Config() {
    Init();
}

Config::~Config() {

}

void Config::Init() {
    fseek(g_conf_file, 0, SEEK_END);
    long fsize = ftell(g_conf_file);
    fseek(g_conf_file, 0, SEEK_SET);

    Json::CharReaderBuilder b;
    Json::CharReader* reader(b.newCharReader());
    JSONCPP_STRING errs;
    Json::Value value;
    char* buffer = (char*)calloc(fsize, sizeof(char));
    fread(buffer, sizeof(char), fsize, g_conf_file);
    bool ret = reader->parse(buffer, buffer + fsize, &value, &errs);
    if (ret && errs.size() == 0) {
        string_.assign(buffer, fsize);
        value_ = value;
    }
    free(buffer);
    delete reader;
}

const Json::Value& Config::operator[](const char* key) const {
    return value_[key];
}