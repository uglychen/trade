#include "lock.h"

#include <sys/time.h>
#include <hiredis.h>
#include "logging.h"
#include "redis_utils.h"

const int LOCK_TIMEOUT = 10000000;
const int LOCK_SLEEPTIME = 1000;

std::vector<std::pair<std::string, int> > g_sentinels;
std::string g_master_name;
std::string g_redis_password;
int g_redis_database = 0;

redisContext *g_redis_context = NULL;

inline uint64_t now_microseconds(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (uint64_t) tv.tv_sec * 1000000 + (uint64_t) tv.tv_usec;
}

inline void microsleep(int usec) {
  struct timespec req;
  req.tv_sec = 0;
  req.tv_nsec = 1000 * usec;
  nanosleep(&req, NULL);
}

void CloseRedis() {
    if (!g_redis_context)
        return;

    redisFree(g_redis_context);
    g_redis_context = NULL;
}

void ReconnectRedis() {
    CloseRedis();
    //g_redis_context = SentinelRedisConnect(g_sentinels, g_master_name, g_redis_password, g_redis_database);

    for (auto it = g_sentinels.begin(); it != g_sentinels.end(); ++it) {
        g_redis_context = redisConnect(it->first.c_str(), it->second) ; 
		LOG(INFO) << "ReconnectRedis:" << it->first.c_str();
		LOG(INFO) << "ReconnectRedis:" << it->second;
    }
    
    if (g_redis_context == NULL) {
        LOG(INFO) << "lock redis connect failed!";
        exit(1);
    }else{
        LOG(INFO) << "redis connect!!!!";
    }
}


bool InitLock(std::vector<std::pair<std::string, int> > sentinels, const char* master_name, const char * password, int database) {
    assert(g_redis_context == NULL);

    g_sentinels = sentinels;
    g_master_name = master_name;
    g_redis_password = password;
    g_redis_database = database;

    ReconnectRedis();
    return g_redis_context != NULL;
}

bool Try(std::string key) {
    key = "lock." + key;

    uint64_t now = now_microseconds();
    uint64_t expired = 0;
    int acquired = 0;

    redisReply* reply;
    reply = (redisReply*)redisCommand(g_redis_context, "SETNX %s %ld", key.c_str(), now + LOCK_TIMEOUT + 1);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        if (reply) {
            LOG(ERROR) << "Command error: " << reply->str;
            freeReplyObject(reply);
        } else {
            LOG(ERROR) << "Connection error: " << g_redis_context->errstr;
            ReconnectRedis();
        }
        return false;
    } else if (reply->type == REDIS_REPLY_INTEGER) {
        acquired = reply->integer;
    } else {
        return false;
    }
    freeReplyObject(reply);
    
    if (acquired == 1) {  // acquired the lock
        return true;
    }
    
    reply = (redisReply*)redisCommand(g_redis_context, "GET %s", key.c_str());
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        if (reply) {
            LOG(ERROR) << "Command error: " << reply->str;
            freeReplyObject(reply);
        } else {
            LOG(ERROR) << "Connection error: " << g_redis_context->errstr;
            ReconnectRedis();
        }
        return false;
    } else if (reply->type == REDIS_REPLY_STRING) {
        expired = atol(reply->str);
    } else if (reply->type == REDIS_REPLY_NIL) {
        expired = 0;
    } else {
        return false;
    }
    freeReplyObject(reply);

    if (expired >= now) {  // not expired
        return false;
    }
        
    reply = (redisReply*)redisCommand(g_redis_context, "GETSET %s %ld", key.c_str(), now + LOCK_TIMEOUT + 1);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        if (reply) {
            LOG(ERROR) << "Command error: " << reply->str;
            freeReplyObject(reply);
        } else {
            LOG(ERROR) << "Connection error: " << g_redis_context->errstr;
            ReconnectRedis();
        }
        return false;
    } else if (reply->type == REDIS_REPLY_STRING) { 
        expired = atol(reply->str);
    } else if (reply->type == REDIS_REPLY_NIL) {
        expired = 0;
    } else {
        return false;
    }
    freeReplyObject(reply);

    if (expired >= now) {  // not expired
        return false;
    }

    return true;
}

void Lock(std::string key) {
    int loop = 0;
    for (;;) {
        loop++;
        if (Try(key))
            break;

        microsleep(1000);
    }
    if (loop > 1)
        LOG(INFO) << "Lock: " << key << " , try count: " << loop;
}

void Unlock(std::string key) {
    key = "lock." + key;

    uint64_t now = now_microseconds();
    uint64_t expired = 0;

    redisReply* reply;
    reply = (redisReply*)redisCommand(g_redis_context, "GET %s", key.c_str());
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        if (reply) {
            LOG(ERROR) << "Command error: " << reply->str;
            freeReplyObject(reply);
        } else {
            LOG(ERROR) << "Connection error: " << g_redis_context->errstr;
            ReconnectRedis();
        }
        return;
    } else if (reply->type == REDIS_REPLY_STRING) {
        expired = atol(reply->str);
    } else if (reply->type == REDIS_REPLY_NIL) {
        expired = 0;
    } else {
        return;
    }
    freeReplyObject(reply);

    if (expired < now) {  // expired
        return;
    }

    reply = (redisReply*)redisCommand(g_redis_context, "DEL %s", key.c_str());
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        if (reply) {
            LOG(ERROR) << "Command error: " << reply->str;
            freeReplyObject(reply);
        } else {
            LOG(ERROR) << "Connection error: " << g_redis_context->errstr;
            ReconnectRedis();
        }
        return;
    }
    freeReplyObject(reply);
}
