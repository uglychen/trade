#ifndef TRADE_H_
#define TRADE_H_

#include <amqp.h>
#include "statistics.h"
#include "config.h"

class Trade {
  public:
    Trade();
    ~Trade();

    void Init();

    void Run();

    void CleanUp();

    void SendMessage(std::string msg, std::string type);

  private:
    amqp_connection_state_t InitMQ(Json::Value config, bool consumer);

    amqp_connection_state_t consumer_;
    amqp_connection_state_t orderBookProducer_;
    amqp_connection_state_t orderChangeProducer_;
    amqp_connection_state_t tradeProducer_;
    amqp_connection_state_t klineProducer_;
    amqp_connection_state_t tickerProducer_;
    std::string producer_book_exchange_;
    std::string producer_change_exchange_;
    std::string producer_trade_exchange_;
    std::string producer_kline_exchange_;
    std::string producer_ticker_exchange_;

	Statistics * m_statistics;
};

#endif  // TRADE_H_