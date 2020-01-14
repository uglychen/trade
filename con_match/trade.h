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

    void SendStatisticsMessage(std::string msg);
    void SendTradeMessage(std::string msg);
    void SendEmailMessage(std::string msg);

  private:
    bool HeartBeat();
    amqp_connection_state_t InitMQ(Json::Value config, int sign);

    amqp_connection_state_t consumer_;
    amqp_connection_state_t producer_statistics_;
    amqp_connection_state_t producer_trade_msg_;
    amqp_connection_state_t producer_email_msg_;

    std::string producer_statistics_exchange_;
    std::string producer_statistics_routing_key_;
    std::string producer_trade_msg_exchange_;
    std::string producer_trade_msg_routing_key_;
    std::string producer_email_msg_exchange_;
    std::string producer_email_msg_routing_key_;

	  Match* m_match;
};

#endif  // TRADE_H_