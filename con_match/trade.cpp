#include "trade.h"

#include <string.h>

#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "amqp_utils.h"
#include "logging.h"
#include "utils.h"

#define SUMMARY_EVERY_US 1000000

Trade::Trade() {

}

Trade::~Trade() {

}

void Trade::Init() {
    m_match = new Match(this);
    Config config;

    const Json::Value consumer = config["consumermq"];
    if (consumer.empty()) {
        die("missing consumer config");
    }
    
    const Json::Value producer_statistics = config["producermq_statistics"];
    if (producer_statistics.empty()) {
        die("missing producer_statistics config");
    }

    const Json::Value producer_trade_msg = config["producermq_trade_msg"];
    if (producer_trade_msg.empty()) {
        die("missing producer_trade_msg config");
    }

    const Json::Value producer_email_msg = config["producermq_email_msg"];
    if (producer_email_msg.empty()) {
        die("missing producer_email_msg config");
    }

    consumer_ = InitMQ(consumer, 0);
    producer_statistics_ = InitMQ(producer_statistics, 1);
    producer_trade_msg_ = InitMQ(producer_trade_msg, 2);
    producer_email_msg_ = InitMQ(producer_email_msg, 3);
}

void Trade::SendStatisticsMessage(std::string msg) {
    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes("text/plain");
    props.delivery_mode = 2; /* persistent delivery mode */
    die_on_error(amqp_basic_publish(producer_statistics_,
                                    1,
                                    amqp_cstring_bytes(producer_statistics_exchange_.c_str()),
                                    amqp_cstring_bytes(producer_statistics_routing_key_.c_str()),
                                    0,
                                    0,
                                    &props,
                                    amqp_cstring_bytes(msg.c_str())),
                 "Publishing");
}

void Trade::SendTradeMessage(std::string msg) {
    die_on_error(amqp_basic_publish(producer_trade_msg_,
                                    1,
                                    amqp_cstring_bytes(producer_trade_msg_exchange_.c_str()),
                                    amqp_cstring_bytes(producer_trade_msg_routing_key_.c_str()),
                                    0,
                                    0,
                                    NULL,
                                    amqp_cstring_bytes(msg.c_str())),
                 "Publishing");
}

void Trade::SendEmailMessage(std::string msg) {
    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes("text/plain");
    props.delivery_mode = 2; /* persistent delivery mode */
    die_on_error(amqp_basic_publish(producer_email_msg_,
                                    1,
                                    amqp_cstring_bytes(producer_email_msg_exchange_.c_str()),
                                    amqp_cstring_bytes(producer_email_msg_routing_key_.c_str()),
                                    0,
                                    0,
                                    &props,
                                    amqp_cstring_bytes(msg.c_str())),
                 "Publishing");
}

bool Trade::HeartBeat( )
{
    static uint64_t now = now_microseconds();
    static uint64_t next_heart_time = now;

    now = now_microseconds();
    if(now > (next_heart_time + (10*1000000)))
    {
        next_heart_time = now;
       
        amqp_frame_t heartbeat;
        heartbeat.channel = 0;
        heartbeat.frame_type = AMQP_FRAME_HEARTBEAT;

        int res = amqp_send_frame(consumer_, &heartbeat);
        if (AMQP_STATUS_OK != res) {
            LOG(ERROR) << "consumer_ heart_failed: " << now;
            return false;
        }
        
        res = amqp_send_frame(producer_statistics_, &heartbeat);
        if (AMQP_STATUS_OK != res) {
            LOG(ERROR) << "producer_statistics_ heart_failed: " << now;
            return false;
        }
        
        res = amqp_send_frame(producer_trade_msg_, &heartbeat);
        if (AMQP_STATUS_OK != res) {
            LOG(ERROR) << "producer_trade_msg_ heart_failed: " << now;
            return false;
        }
        
        res = amqp_send_frame(producer_email_msg_, &heartbeat);
        if (AMQP_STATUS_OK != res) {
            LOG(ERROR) << "producer_email_msg_ heart_failed: " << now;
            return false;
        }
        LOG(INFO) << "heart_ok: " << now;
    }

    return true;
}

void Trade::Run() {
    int received = 0;
    int previous_received = 0;
    uint64_t start_time = now_microseconds();
    uint64_t previous_report_time = start_time;
    uint64_t next_summary_time = start_time + SUMMARY_EVERY_US;
    uint64_t now;
    bool     heart_ok = false;
    amqp_frame_t frame;

    for (;;) {

        heart_ok = HeartBeat();

        if (m_match->m_pending_msg_list.size() > 0){
            std::string msg = m_match->m_pending_msg_list[0];
            m_match->m_pending_msg_list.erase(m_match->m_pending_msg_list.begin());
            m_match->Msg(msg);
        }else{
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
            struct timeval timeout = {3, 0};
            ret = amqp_consume_message(consumer_, &envelope, &timeout, 0);
            

            if (AMQP_RESPONSE_NORMAL != ret.reply_type) {

                if((ret.library_error == AMQP_STATUS_TIMEOUT) && (heart_ok))
                {
                    continue;
                }

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
                }
                LOG(ERROR) << "ret.reply_type: " << ret.reply_type;
                LOG(ERROR) << "ret.library_error: " << ret.library_error;
                //die("mq error");
                Init();
            } else {
                amqp_bytes_t& body = envelope.message.body;
                if (body.len > 0) {
                    std::string msg((char*)body.bytes, body.len);
                    m_match->Msg(msg);
                }

                amqp_destroy_envelope(&envelope);
            }

            received++;
        }
    }
}

void Trade::CleanUp() {
    die_on_amqp_error(amqp_channel_close(consumer_, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(consumer_, AMQP_REPLY_SUCCESS), "Closing connection");
    die_on_error(amqp_destroy_connection(consumer_), "Ending connection");

    die_on_amqp_error(amqp_channel_close(producer_statistics_, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(producer_statistics_, AMQP_REPLY_SUCCESS), "Closing connection");
    die_on_error(amqp_destroy_connection(producer_statistics_), "Ending connection");

    die_on_amqp_error(amqp_channel_close(producer_trade_msg_, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(producer_trade_msg_, AMQP_REPLY_SUCCESS), "Closing connection");
    die_on_error(amqp_destroy_connection(producer_trade_msg_), "Ending connection");

    die_on_amqp_error(amqp_channel_close(producer_email_msg_, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(producer_email_msg_, AMQP_REPLY_SUCCESS), "Closing connection");
    die_on_error(amqp_destroy_connection(producer_email_msg_), "Ending connection");
}

amqp_connection_state_t Trade::InitMQ(Json::Value config, int sign) {
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
    password = real_password(encode_password);
    exchange = config.get("exchange", "").asString();
    routing_key = config.get("routing_key", "").asString();
    queue = config.get("queue", "").asString();

    conn = amqp_new_connection();

    socket = amqp_tcp_socket_new(conn);
    if (!socket) {
        die("creating TCP socket");
    }

    status = amqp_socket_open(socket, host.c_str(), port);
    if (status) {
        die("opening TCP socket");
    }

    die_on_amqp_error(amqp_login(conn, vhost.c_str(), 0, 131072, 120, AMQP_SASL_METHOD_PLAIN, username.c_str(), password.c_str()), "Logging in");
    amqp_channel_open(conn, 1);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");

    if (sign == 0 || sign == 1 || sign == 3){
        amqp_exchange_declare(conn, 1, amqp_cstring_bytes(exchange.c_str()), amqp_cstring_bytes("direct"), 0, 1, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring exchange");
        
        amqp_queue_declare(conn, 1, amqp_cstring_bytes(queue.c_str()), 0, 1, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");

        amqp_queue_bind(conn, 1, amqp_cstring_bytes(queue.c_str()), amqp_cstring_bytes(exchange.c_str()), amqp_cstring_bytes(routing_key.c_str()), amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Binding queue");
    }else{
        amqp_exchange_declare(conn, 1, amqp_cstring_bytes(exchange.c_str()), amqp_cstring_bytes("fanout"), 0, 0, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring exchange");
    }

    if (sign == 0) {
        amqp_basic_consume(conn, 1, amqp_cstring_bytes(queue.c_str()), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");
    } else if (sign == 1){
        producer_statistics_exchange_ = exchange;
        producer_statistics_routing_key_ = routing_key;
    } else if (sign == 2){
        producer_trade_msg_exchange_ = exchange;
        producer_trade_msg_routing_key_ = routing_key;
    }else{
        producer_email_msg_exchange_ = exchange;
        producer_email_msg_routing_key_ = routing_key;
    }
    
    LOG(INFO) << "mq ok";
    return conn;
}