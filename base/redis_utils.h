#ifndef REDIS_UTILS_H_
#define REDIS_UTILS_H_

#include <string>
#include <utility>
#include <vector>

#include <hiredis.h>

redisContext* SentinelRedisConnect(std::vector<std::pair<std::string, int> > sentinels, std::string master_name, std::string password, int database);

#endif  // REDIS_UTILS_H_