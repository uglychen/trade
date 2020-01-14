#ifndef LOCK_H_
#define LOCK_H_

#include <string>
#include <utility>
#include <vector>

bool InitLock(std::vector<std::pair<std::string, int> > sentinels, const char* master_name, const char * password, int database);

bool Try(std::string key);

void Lock(std::string key);

void Unlock(std::string key);

#endif  // LOCK_H_