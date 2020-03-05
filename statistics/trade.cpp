#include "trade.h"

#include <string.h>

#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "amqp_utils.h"
#include "lock.h"
#include "logging.h"
#include "utils.h"

#define SUMMARY_EVERY_US 1000000

Trade::Trade() {

}

Trade::~Trade() {

}

void Trade::Init() {
    m_statistics = new Statistics(this);
    Config config;

    const Json::Value consumer = config["consumermq"];
    if (consumer.empty()) {
        die("missing consumer config");
    }
    
    const Json::Value producer_order_book = config["producermq_order_book"];
    if (producer_order_book.empty()) {
        die("missing producer_order_book config");
    }
    
    const Json::Value producer_order_change = config["producermq_order_change"];
    if (producer_order_change.empty()) {
        die("missing producer_order_change config");
    }
    
    const Json::Value producer_trade = config["producermq_trade"];
    if (producer_trade.empty()) {
        die("missing producer_trade config");
    }
    
    const Json::Value producer_kline = config["producermq_kline"];
    if (producer_kline.empty()) {
        die("missing producer_kline config");
    }
    
    const Json::Value producer_ticker = config["producermq_ticker"];
    if (producer_ticker.empty()) {
        die("missing producer_ticker config");
    }

    consumer_ = InitMQ(consumer, true);
    orderBookProducer_ = InitMQ(producer_order_book, false);
    orderChangeProducer_ = InitMQ(producer_order_change, false);
    tradeProducer_ = InitMQ(producer_trade, false);
    klineProducer_ = InitMQ(producer_kline, false);
    tickerProducer_ = InitMQ(producer_ticker, false);
    producer_book_exchange_ = producer_order_book.get("exchange", "").asString();
    producer_change_exchange_ = producer_order_change.get("exchange", "").asString();
    producer_trade_exchange_ = producer_trade.get("exchange", "").asString();
    producer_kline_exchange_ = producer_kline.get("exchange", "").asString();
    producer_ticker_exchange_ = producer_ticker.get("exchange", "").asString();
    
    return;
}

void Trade::SendMessage(std::string msg, std::string type) {
    if (type == "order_book"){
        die_on_error(amqp_basic_publish(orderBookProducer_,
                                        1,
                                        amqp_cstring_bytes(producer_book_exchange_.c_str()),
                                        amqp_cstring_bytes(""),
                                        0,
                                        0,
                                        NULL,
                                        amqp_cstring_bytes(msg.c_str())),
                    "Publishing");
    }else if (type == "order_change"){
        die_on_error(amqp_basic_publish(orderChangeProducer_,
                                        1,
                                        amqp_cstring_bytes(producer_change_exchange_.c_str()),
                                        amqp_cstring_bytes(""),
                                        0,
                                        0,
                                        NULL,
                                        amqp_cstring_bytes(msg.c_str())),
                    "Publishing");
    }else if (type == "trade"){
        die_on_error(amqp_basic_publish(tradeProducer_,
                                        1,
                                        amqp_cstring_bytes(producer_trade_exchange_.c_str()),
                                        amqp_cstring_bytes(""),
                                        0,
                                        0,
                                        NULL,
                                        amqp_cstring_bytes(msg.c_str())),
                    "Publishing");
    }else if (type == "kline"){
        die_on_error(amqp_basic_publish(klineProducer_,
                                        1,
                                        amqp_cstring_bytes(producer_kline_exchange_.c_str()),
                                        amqp_cstring_bytes(""),
                                        0,
                                        0,
                                        NULL,
                                        amqp_cstring_bytes(msg.c_str())),
                    "Publishing");
    }else if (type == "ticker"){
        die_on_error(amqp_basic_publish(tickerProducer_,
                                        1,
                                        amqp_cstring_bytes(producer_ticker_exchange_.c_str()),
                                        amqp_cstring_bytes(""),
                                        0,
                                        0,
                                        NULL,
                                        amqp_cstring_bytes(msg.c_str())),
                    "Publishing");
    }else{
    }
}

void Trade::Run() {
    int received = 0;
    int previous_received = 0;
    uint64_t start_time = now_microseconds();
    uint64_t previous_report_time = start_time;
    uint64_t next_summary_time = start_time + SUMMARY_EVERY_US;
    uint64_t now;

    amqp_frame_t frame;

    for (;;) {
        amqp_rpc_reply_t ret;
        amqp_envelope_t envelope;

        now = now_microseconds();
        if (now > next_summary_time) {
            if (received > previous_received) {
                int countOverInterval = received - previous_received;
                double intervalRate = countOverInterval / ((now - previous_report_time) / 1000000.0);
                printf("%d ms: Received %d - %d since last report (%d Hz)\n",
                   (int)(now - start_time) / 1000, received, countOverInterval, (int) intervalRate);
            }

            previous_received = received;
            previous_report_time = now;
            next_summary_time += SUMMARY_EVERY_US;
        }

        amqp_maybe_release_buffers(consumer_);
        struct timeval timeout = {0, 10000};
        
        ret = amqp_consume_message(consumer_, &envelope, &timeout, 0);

        if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
            if (AMQP_RESPONSE_LIBRARY_EXCEPTION == ret.reply_type &&
                AMQP_STATUS_UNEXPECTED_STATE == ret.library_error) {
                if (AMQP_STATUS_OK != amqp_simple_wait_frame(consumer_, &frame)) {
                    return;
                }
                if (AMQP_FRAME_METHOD == frame.frame_type) {
                    switch (frame.payload.method.id) {
                        case AMQP_BASIC_ACK_METHOD: {
                            // if we've turned publisher confirms on, and we've published a message
                            // here is a message being confirmed
                            break;
                        }
                        case AMQP_BASIC_RETURN_METHOD: {
                            // if a published message couldn't be routed and the mandatory flag was set
                            // this is what would be returned. The message then needs to be read.
                            amqp_message_t message;
                            ret = amqp_read_message(consumer_, frame.channel, &message, 0);
                            if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
                                return;
                            }

                            amqp_destroy_message(&message);
                            break;
                        }
                        case AMQP_CHANNEL_CLOSE_METHOD: {
                            // a channel.close method happens when a channel exception occurs, this
                            // can happen by publishing to an exchange that doesn't exist for example
                            //
                            // In this case you would need to open another channel redeclare any queues
                            // that were declared auto-delete, and restart any consumers that were attached
                            // to the previous channel
                            return;
                        }
                        case AMQP_CONNECTION_CLOSE_METHOD: {
                            // a connection.close method happens when a connection exception occurs,
                            // this can happen by trying to use a channel that isn't open for example.
                            //
                            // In this case the whole connection must be restarted.
                            return;
                        }
                        default: {
                            fprintf(stderr ,"An unexpected method was received %u\n", frame.payload.method.id);
                            return;
                        }
                    }
                }
            }else if (AMQP_RESPONSE_LIBRARY_EXCEPTION == ret.reply_type && AMQP_STATUS_TIMEOUT == ret.library_error){
                m_statistics->SendAllTicker();
                continue;
            }
            LOG(ERROR) << "ret.reply_type: " << ret.reply_type;
            LOG(ERROR) << "ret.library_error: " << ret.library_error;
            die("mq error");
        } else {
            m_statistics->SendAllTicker();
            amqp_bytes_t& body = envelope.message.body;
            if (body.len > 0) {
                std::string msg((char*)body.bytes, body.len);
                m_statistics->Msg(msg);
                received++;
            }
            amqp_destroy_envelope(&envelope);
        }

    }
}

void Trade::CleanUp() {
    die_on_amqp_error(amqp_channel_close(consumer_, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(consumer_, AMQP_REPLY_SUCCESS), "Closing connection");
    die_on_error(amqp_destroy_connection(consumer_), "Ending connection");

    die_on_amqp_error(amqp_channel_close(orderBookProducer_, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(orderBookProducer_, AMQP_REPLY_SUCCESS), "Closing connection");
    die_on_error(amqp_destroy_connection(orderBookProducer_), "Ending connection");

    die_on_amqp_error(amqp_channel_close(orderChangeProducer_, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(orderChangeProducer_, AMQP_REPLY_SUCCESS), "Closing connection");
    die_on_error(amqp_destroy_connection(orderChangeProducer_), "Ending connection");
    
    die_on_amqp_error(amqp_channel_close(tradeProducer_, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(tradeProducer_, AMQP_REPLY_SUCCESS), "Closing connection");
    die_on_error(amqp_destroy_connection(tradeProducer_), "Ending connection");
    
    die_on_amqp_error(amqp_channel_close(klineProducer_, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(klineProducer_, AMQP_REPLY_SUCCESS), "Closing connection");
    die_on_error(amqp_destroy_connection(klineProducer_), "Ending connection");
    
    die_on_amqp_error(amqp_channel_close(tickerProducer_, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(tickerProducer_, AMQP_REPLY_SUCCESS), "Closing connection");
    die_on_error(amqp_destroy_connection(tickerProducer_), "Ending connection");
}

amqp_connection_state_t Trade::InitMQ(Json::Value config, bool consumer) {
    std::string host;
    int port, status;
    std::string vhost;
    std::string username;
    std::string encode_password;
    std::string password;
    std::string exchange;
    std::string routing_key;
    std::string queue;
    amqp_socket_t *socket = NULL;
    amqp_connection_state_t conn = NULL;

    host = config.get("host", "").asString();
    port = config.get("port", 0).asInt();
    vhost = config.get("vhost", "").asString();
    username = config.get("username", "").asString();
    encode_password = config.get("password", "").asString();
    //password = real_password(encode_password);
	password = encode_password;
    exchange = config.get("exchange", "").asString();
    routing_key = config.get("routing_key", "").asString();
    queue = config.get("queue", "").asString();

	LOG(ERROR) << "InitMQ: exchange " << exchange;

    conn = amqp_new_connection();

    socket = amqp_tcp_socket_new(conn);
    if (!socket) {
        die("creating TCP socket");
    }

    status = amqp_socket_open(socket, host.c_str(), port);
    if (status) {
        die("opening TCP socket");
    }

    die_on_amqp_error(amqp_login(conn, vhost.c_str(), 0, 131072, 60, AMQP_SASL_METHOD_PLAIN, username.c_str(), password.c_str()), "Logging in");
    amqp_channel_open(conn, 1);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");


    if (consumer) {
        amqp_exchange_declare(conn, 1, amqp_cstring_bytes(exchange.c_str()), amqp_cstring_bytes("direct"), 0, 1, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring exchange");

        amqp_queue_declare(conn, 1, amqp_cstring_bytes(queue.c_str()), 0, 1, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");

        amqp_queue_bind(conn, 1, amqp_cstring_bytes(queue.c_str()), amqp_cstring_bytes(exchange.c_str()), amqp_cstring_bytes(routing_key.c_str()), amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Binding queue");
        amqp_basic_consume(conn, 1, amqp_cstring_bytes(queue.c_str()), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");
    } else {
        amqp_exchange_declare(conn, 1, amqp_cstring_bytes(exchange.c_str()), amqp_cstring_bytes("fanout"), 0, 0, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring exchange");
    }
    
    LOG(ERROR) << "mq ok";
    return conn;
}