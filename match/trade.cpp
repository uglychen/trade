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
    m_match = new Match(this);
    Config config;

    const Json::Value consumer = config["consumermq"];
    if (consumer.empty()) {
        die("missing consumer config");
    }
    
    const Json::Value producer = config["producermq"];
    if (producer.empty()) {
        die("missing producer config");
    }

    consumer_ = InitMQ(consumer, true);
    producer_ = InitMQ(producer, false);
}

void Trade::SendMessage(std::string msg) {
    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes("text/plain");
    props.delivery_mode = 2; /* persistent delivery mode */
    die_on_error(amqp_basic_publish(producer_,
                                    1,
                                    amqp_cstring_bytes(producer_exchange_.c_str()),
                                    amqp_cstring_bytes(producer_routing_key_.c_str()),
                                    0,
                                    0,
                                    &props,
                                    amqp_cstring_bytes(msg.c_str())),
                 "Publishing");
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
        ret = amqp_consume_message(consumer_, &envelope, NULL, 0);

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
            }
            LOG(ERROR) << "ret.reply_type: " << ret.reply_type;
            LOG(ERROR) << "ret.library_error: " << ret.library_error;
            die("mq error");
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

void Trade::CleanUp() {
    die_on_amqp_error(amqp_channel_close(consumer_, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(consumer_, AMQP_REPLY_SUCCESS), "Closing connection");
    die_on_error(amqp_destroy_connection(consumer_), "Ending connection");

    die_on_amqp_error(amqp_channel_close(producer_, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(producer_, AMQP_REPLY_SUCCESS), "Closing connection");
    die_on_error(amqp_destroy_connection(producer_), "Ending connection");
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

	LOG(ERROR) << "InitMQ : " << host;
	LOG(ERROR) << "InitMQ : " << port;
	LOG(ERROR) << "InitMQ : " << vhost;
	LOG(ERROR) << "InitMQ : " << username;
	LOG(ERROR) << "InitMQ : " << encode_password;
	LOG(ERROR) << "InitMQ : " << exchange;
	LOG(ERROR) << "InitMQ : " << routing_key;
	LOG(ERROR) << "InitMQ : " << queue;


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

    amqp_exchange_declare(conn, 1, amqp_cstring_bytes(exchange.c_str()), amqp_cstring_bytes("direct"), 0, 1, 0, 0, amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring exchange");

    amqp_queue_declare(conn, 1, amqp_cstring_bytes(queue.c_str()), 0, 1, 0, 0, amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");

    amqp_queue_bind(conn, 1, amqp_cstring_bytes(queue.c_str()), amqp_cstring_bytes(exchange.c_str()), amqp_cstring_bytes(routing_key.c_str()), amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Binding queue");

    if (consumer) {
        amqp_basic_consume(conn, 1, amqp_cstring_bytes(queue.c_str()), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");
    } else {
        producer_exchange_ = exchange;
        producer_routing_key_ = routing_key;
    }
    
    LOG(ERROR) << "mq ok";
    return conn;
}