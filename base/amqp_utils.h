#ifndef AMQP_UTILS_H_
#define AMQP_UTILS_H_

#include <amqp.h>

#include "utils.h"

void die_on_error(int x, char const *context);
void die_on_amqp_error(amqp_rpc_reply_t x, char const *context);
void amqp_dump(void const *buffer, size_t len);

#endif  // AMQP_UTILS_H_