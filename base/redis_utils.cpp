#include "redis_utils.h"

#include "logging.h"

const int SHORT_TIMEOUT = 500000;
const int RETRY_TIMES = 15;

inline void microsleep(int usec) {
    struct timespec req;
    req.tv_sec = 0;
    req.tv_nsec = 1000 * usec;
    nanosleep(&req, NULL);
}

redisContext* SentinelRedisConnect(std::vector<std::pair<std::string, int> > sentinels, std::string master_name, std::string password, int database) {
    redisContext* redis_context = NULL;

    for (int i = 0; i < RETRY_TIMES; i++){
        if (sentinels.empty()) {
            return NULL;
        }
        // wait a short time before reconnect
        microsleep(SHORT_TIMEOUT);

        std::string ip;
        int port = 0;
        for (auto it = sentinels.begin(); it != sentinels.end(); ++it) {
            struct timeval timeout = { 0, SHORT_TIMEOUT };
            LOG(INFO) << "sentinel " << it->first << ":" << it->second;
            redisContext* sentinel_context = redisConnectWithTimeout(it->first.c_str(), it->second, timeout);
            if (sentinel_context == NULL || sentinel_context->err) {
                if (sentinel_context) {
                    LOG(ERROR) << "Connection error: " << sentinel_context->errstr;
                    redisFree(sentinel_context);
                    sentinel_context = NULL;
                } else {
                    LOG(ERROR) << "Connection error: can't allocate redis context";
                }
                continue;
            }

            redisReply* reply = (redisReply*)redisCommand(sentinel_context, "SENTINEL get-master-addr-by-name %s", master_name.c_str());
            if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
                if (reply) {
                    LOG(ERROR) << "Command error: " << reply->str;
                    freeReplyObject(reply);
                } else {
                    LOG(ERROR) << "Connection error: " << redis_context->errstr;
                }
                continue;
            } else if (reply->type == REDIS_REPLY_NIL) {
                LOG(ERROR) << "get master " << master_name << " failed";
            } else if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 2) {
                ip = reply->element[0]->str;
                port = atoi(reply->element[1]->str);
            }
            freeReplyObject(reply);
            redisFree(sentinel_context);
            sentinel_context = NULL;

            if (ip.length() != 0 && port != 0) {
                if (it != sentinels.begin()) {
                    auto x = std::move(*it);
                    sentinels.erase(it);
                    sentinels.insert(sentinels.begin(), std::move(x));
                }
                break;
            }
        }

        if (ip.length() == 0 || port == 0) {
            return NULL;
        }

        LOG(INFO) << "master " << ip << ":" << port;
        struct timeval timeout = { 0, SHORT_TIMEOUT }; // 0.5 seconds
        redis_context = redisConnectWithTimeout(ip.c_str(), port, timeout);
        if (redis_context == NULL || redis_context->err) {
            if (redis_context) {
                LOG(ERROR) << "Connection error: " << redis_context->errstr;
                redisFree(redis_context);
                redis_context = NULL;
            } else {
                LOG(ERROR) << "Connection error: can't allocate redis context";
            }
            goto ERROR;
        }
            
        redisReply* reply;
        if (password.length()) {
            reply = (redisReply*)redisCommand(redis_context, "AUTH %s", password.c_str());
            if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
                if (reply) {
                    LOG(ERROR) << "Command error: " << reply->str;
                    freeReplyObject(reply);
                } else {
                    LOG(ERROR) << "Connection error: " << redis_context->errstr;
                }
                goto ERROR;
            }
            freeReplyObject(reply);
        }

        reply = (redisReply*)redisCommand(redis_context, "ROLE");
        if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
            if (reply) {
                LOG(ERROR) << "Command error: " << reply->str;
                freeReplyObject(reply);
            } else {
                LOG(ERROR) << "Connection error: " << redis_context->errstr;
            }
            goto ERROR;
        } else if (reply->type == REDIS_REPLY_ARRAY && reply->elements > 0 && reply->element[0]->type == REDIS_REPLY_STRING) {
            if (strcmp(reply->element[0]->str, "master") != 0) {
                LOG(ERROR) << "Role: " << reply->str;
                freeReplyObject(reply);
                goto ERROR;
            }
        }
        freeReplyObject(reply);

        if (database) {
            reply = (redisReply*)redisCommand(redis_context, "SELECT %d", database);
            if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
                if (reply) {
                    LOG(ERROR) << "Command error: " << reply->str;
                    freeReplyObject(reply);
                } else {
                    LOG(ERROR) << "Connection error: " << redis_context->errstr;
                }
                goto ERROR;
            }
            freeReplyObject(reply);
        }
        return redis_context;

ERROR:
        if (redis_context) {
            redisFree(redis_context);
            redis_context = NULL;
        }
        // microsleep(SHORT_TIMEOUT);
    }

    return NULL;
}

redisContext *redis_connect(const char *ip, unsigned int port, const char *passwd)
{
    redisContext *c = NULL;
    redisReply *replay = NULL;
    
    /*connect*/
    c = redisConnect(ip, port);
    if(c==NULL)
    {
        printf("Error: redisConnect() error!\n");
        LOG(INFO) << "Error: redisConnect() error! " ;
        return NULL;
    }
    if(c->err != 0) 
    {
        printf("Error: %s\n", c->errstr);
        LOG(INFO) << "Error: redisConnect() error! " << c->errstr ; 
        redisFree(c);
    }
    
    /*auth if passwd is not NULL*/
    if(passwd != NULL)
    {
        replay  = (redisReply *)redisCommand(c, "AUTH %s", passwd);
        if( replay == NULL)
        {
            printf("Error: AUTH error!\n");
            LOG(INFO) << "Error: AUTH error!! " ;
            redisFree(c);
            printf("redisFree\n");
            LOG(INFO) << "redisFree! " ;
            return NULL;
        }
        if( !(replay->type==REDIS_REPLY_STATUS && memcmp(replay->str, "OK", 2)==0) )
        {
            printf("Error: AUTH error!\n");
            LOG(INFO) << "Error: AUTH error!! " ;
            freeReplyObject(replay);
            redisFree(c);
            printf("redisFree\n");
            LOG(INFO) << "redisFree!! " ;
            return NULL;
        }
    }
    
    return c; /*connect success*/

}