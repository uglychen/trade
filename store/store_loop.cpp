#include "store_loop.h"

#include <memory>
#include "store.h"
#include "utils.h"
#include "config.h"
#include "logging.h"
#include "redis_utils.h"

#define SUMMARY_EVERY_US 1000000

StoreLoop::StoreLoop() : redis_(NULL) {
}

StoreLoop::~StoreLoop() {
}

void StoreLoop::Init() {
    InitRedis();

    store_ = new Store();
    store_->Init();
}

void StoreLoop::Run() {
    int received = 0;
    int previous_received = 0;
    uint64_t start_time = now_microseconds();
    uint64_t previous_report_time = start_time;
    uint64_t next_summary_time = start_time + SUMMARY_EVERY_US;
    uint64_t now;

    RunSlowCheck();

    std::vector<std::string> messages;
    for (;;) {
        if (redis_ == NULL) {
            ReconnectRedis();
        }

        now = now_microseconds();
        if (now > next_summary_time) {
            if (received > previous_received) {
                int countOverInterval = received - previous_received;
                double intervalRate = countOverInterval / ((now - previous_report_time) / 1000000.0);
                LOG(INFO) << (uint64_t)(now - start_time) / 1000 << " ms: Received " << received << " - " 
                        << countOverInterval << " since last report (" << (int) intervalRate << " Hz)";
            }

            previous_received = received;
            previous_report_time = now;
            next_summary_time += SUMMARY_EVERY_US;
        }

        redisReply* reply;
        std::string message;
        reply = (redisReply*)redisCommand(redis_, "RPOPLPUSH %s %s", queue_.c_str(), pending_queue_.c_str());
        if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            if (reply) {
                LOG(ERROR) << "Command error: " << reply->str;
                freeReplyObject(reply);
            } else {
                LOG(ERROR) << "Connection error: " << redis_->errstr;
                redisFree(redis_);
                redis_ = NULL;
            }
            continue;
        } else if (reply->type == REDIS_REPLY_STRING) {
            message = reply->str; 
            LOG(INFO) <<"========== message:   " << message;
        } else if (reply->type == REDIS_REPLY_NIL) {
            message = "";
        } else {
            LOG(ERROR) << "Unknown error";
        }
        freeReplyObject(reply);

        if (!message.length()) {
            if (messages.size()) {
                received += messages.size();
                RunPending(messages);
                messages.clear();
            }
            store_->DoIdleWork();
            microsleep(10000);
            continue;
        }

        messages.push_back(message);
        if (messages.size() >= 10) {
            RunPending(messages);
            received += messages.size();
            messages.clear();
        }
    }
}

void StoreLoop::RunPending(std::vector<std::string>& messages) {
    Json::CharReaderBuilder rbuilder;
    std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
    JSONCPP_STRING error;
    Json::Value value;
    Json::Value values;
    for (auto it = messages.cbegin(); it != messages.cend(); ++it) {
        bool ret = reader->parse(it->c_str(), it->c_str() + it->length(), &value, &error);
        if (ret && error.size() == 0 && value.isArray()) {
            for(Json::Value::const_iterator iter = value.begin(); iter != value.end(); iter++) {
                values.append(*iter);
            }
        } else {
            LOG(ERROR) << *it;
        }
    }

    if (values.size() > 0) {
        if (!store_->DoWork(values)) {
            RunSlowCheck();
            return;
        }
    }

    redisReply* reply;
    while (messages.size()) {
        if (redis_ == NULL) {
            ReconnectRedis();
        }

        auto it = messages.begin();
        reply = (redisReply*)redisCommand(redis_, "LREM %s 1 %s", pending_queue_.c_str(), it->c_str());
        if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            if (reply) {
                LOG(ERROR) << "Command error: " << reply->str;
                freeReplyObject(reply);
            } else {
                LOG(ERROR) << "Connection error: %s" << redis_->errstr;
                redisFree(redis_);
                redis_ = NULL;
            }
            continue;
        }
        freeReplyObject(reply);
        messages.erase(it);
    }
}

void StoreLoop::RunSlowCheck() {
    LOG(INFO) << "Enter RunSlowCheck";
    for (;;) {
        if (redis_ == NULL) {
			if (redis_ != NULL){
				LOG(INFO) << "redis_ connetct faild !  ReconnectRedis";
			}
            ReconnectRedis();
        }

		if (redis_ != NULL){
			LOG(INFO) << "redis_ connetct ok !!!!!";
		}

        redisReply* reply;
        std::string message;

        LOG(INFO) << "pending_queue_.c_str(): " << pending_queue_.c_str() ;
        LOG(INFO) << "failure_queue_.c_str(): " << failure_queue_.c_str() ;
        reply = (redisReply*)redisCommand(redis_, "RPOPLPUSH %s %s", pending_queue_.c_str(), failure_queue_.c_str());
        if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            if (reply) {
                LOG(ERROR) << "Command error: " << reply->str;
                freeReplyObject(reply);
            } else {
                LOG(ERROR) << "Connection error: %s" << redis_->errstr;
                redisFree(redis_);
                redis_ = NULL;
                exit(1);
            }
            continue;
        } else if (reply->type == REDIS_REPLY_STRING) {
            message = reply->str;
        } else if (reply->type == REDIS_REPLY_NIL) {
            message = "";
        } else {
            die("Unknown error");
        }
        freeReplyObject(reply);

        if (!message.length()) {
            LOG(INFO) << "Exit RunSlowCheck";
            return;
        }

        Json::CharReaderBuilder rbuilder;
        std::unique_ptr<Json::CharReader> const reader(rbuilder.newCharReader());
        JSONCPP_STRING error;
        Json::Value value;
        bool ret = reader->parse(message.c_str(), message.c_str() + message.length(), &value, &error);
        if (ret && error.size() == 0) {
            if (store_->DoWork(value)) {
                reply = (redisReply*)redisCommand(redis_, "LREM %s 1 %s", failure_queue_.c_str(), message.c_str());
                if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
                    if (reply) {
                        LOG(ERROR) << "Command error: " << reply->str;
                        freeReplyObject(reply);
                    } else {
                        LOG(ERROR) << "Connection error: %s" << redis_->errstr;
                        redisFree(redis_);
                        redis_ = NULL;
                    }
                    continue;
                }
                freeReplyObject(reply);
            }
        } else {
            LOG(ERROR) << message;
        }
    }
}

void StoreLoop::CleanUp() {
    redisFree(redis_);
    store_->CleanUp();
    delete store_;
}

void StoreLoop::InitRedis() {
    Config config;

    const Json::Value redis_config = config["redis"];
    if (redis_config.empty()) {
        die("missing redis config");
    }
    queue_ = redis_config.get("queue", "").asString();
    pending_queue_ = "pending." + queue_;
    failure_queue_ = "failure." + queue_;

    ReconnectRedis();
}

void StoreLoop::ReconnectRedis() {
    assert(redis_ == NULL);

    Config config;
    const Json::Value redis_config = config["redis"];
    if (redis_config.empty()) {
        LOG(ERROR) << "missing redis config";
        exit(1);
    }

    const Json::Value sentinel_config = redis_config["sentinels"];
    std::vector<std::pair<std::string, int> > sentinels;
    for (unsigned i = 0; i < sentinel_config.size(); ++i) {
        sentinels.push_back(std::make_pair(sentinel_config[i]["host"].asString(), sentinel_config[i]["port"].asInt()));
    }
    std::string encode_password = redis_config["password"].asString();
    //std::string password = real_password(encode_password);
    std::string password = encode_password;

    //redis_ = SentinelRedisConnect(sentinels, redis_config["master_name"].asCString(), password.c_str(), redis_config["database"].asInt());
    for (auto it = sentinels.begin(); it != sentinels.end(); ++it) {
        redis_ = redisConnect(it->first.c_str(), it->second) ;  
		LOG(INFO) << "ReconnectRedis :" << it->first.c_str();
		LOG(INFO) << "ReconnectRedis :" << it->second;

		if(redis_ != NULL && redis_->err)
     	{
         	LOG(INFO) << "connection error1111111111111111:" << redis_->errstr;
     	}
    }

    if (redis_ == NULL) {
        LOG(ERROR) << "redis_ connect faild";
        exit(1);
    }else{
        //std::cout << "m_redis connect ok" << std::endl;
        LOG(INFO) << "redis_ connect ok";
    }
}