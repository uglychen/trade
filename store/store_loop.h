#ifndef STORE_LOOP_H_
#define STORE_LOOP_H_

#include <string>
#include <vector>
#include <hiredis.h>

class Store;

class StoreLoop {
  public:
    StoreLoop();
    ~StoreLoop();

    void Init();

    void Run();

    void CleanUp();

  private:
    void InitRedis();
    void ReconnectRedis();
    void RunPending(std::vector<std::string>& messages);
    void RunSlowCheck();

    redisContext* redis_;
    std::string queue_;
    std::string pending_queue_;
    std::string failure_queue_;

    Store* store_;
};

#endif  // STORE_LOOP_H_