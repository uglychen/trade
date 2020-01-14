#ifndef TRADE_H_
#define TRADE_H_

#include <amqp.h>
#include "match.h"
#include "config.h"

class Trade {
  public:
    Trade();
    ~Trade();

    void Init();

    void Run();

    void CleanUp();

    void SendMessage(std::string msg);

  private:
    amqp_connection_state_t InitMQ(Json::Value config, bool consumer);

    amqp_connection_state_t consumer_;
    amqp_connection_state_t producer_;
    std::string producer_exchange_;
    std::string producer_routing_key_;

	  Match* m_match;
};

#endif  // TRADE_H_