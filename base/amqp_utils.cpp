#include "amqp_utils.h"

#include <iomanip>

#include "logging.h"

void die_on_amqp_error(amqp_rpc_reply_t x, char const *context)
{
  switch (x.reply_type) {
  case AMQP_RESPONSE_NORMAL:
    return;

  case AMQP_RESPONSE_NONE:
    LOG(ERROR) << context << ": missing RPC reply type!";
    break;

  case AMQP_RESPONSE_LIBRARY_EXCEPTION:
    LOG(ERROR) << context << ": " << amqp_error_string2(x.library_error);
    break;

  case AMQP_RESPONSE_SERVER_EXCEPTION:
    switch (x.reply.id) {
    case AMQP_CONNECTION_CLOSE_METHOD: {
      amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
      LOG(ERROR) << context << ": server connection error " << m->reply_code << "h, message: " << std::string((char *) m->reply_text.bytes, (int) m->reply_text.len);
      break;
    }
    case AMQP_CHANNEL_CLOSE_METHOD: {
      amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
      LOG(ERROR) << context << ": server channel error " << m->reply_code << "h, message: " << std::string((char *) m->reply_text.bytes, (int) m->reply_text.len);
      break;
    }
    default:
      LOG(ERROR) << context << ": unknown server error, method id 0x" << std::hex << std::setw(8) << std::setfill('0') << x.reply.id;
      break;
    }
    break;
  }

  exit(1);
}

void die_on_error(int x, char const *context)
{
  if (x < 0) {
    LOG(ERROR) << context << ": " << amqp_error_string2(x);
    exit(1);
  }
}

static void dump_row(long count, int numinrow, int *chs)
{
  int i;

  printf("%08lX:", count - numinrow);

  if (numinrow > 0) {
    for (i = 0; i < numinrow; i++) {
      if (i == 8) {
        printf(" :");
      }
      printf(" %02X", chs[i]);
    }
    for (i = numinrow; i < 16; i++) {
      if (i == 8) {
        printf(" :");
      }
      printf("   ");
    }
    printf("  ");
    for (i = 0; i < numinrow; i++) {
      if (isprint(chs[i])) {
        printf("%c", chs[i]);
      } else {
        printf(".");
      }
    }
  }
  printf("\n");
}

static int rows_eq(int *a, int *b)
{
  int i;

  for (i=0; i<16; i++)
    if (a[i] != b[i]) {
      return 0;
    }

  return 1;
}

void amqp_dump(void const *buffer, size_t len)
{
  unsigned char *buf = (unsigned char *) buffer;
  long count = 0;
  int numinrow = 0;
  int chs[16];
  int oldchs[16] = {0};
  int showed_dots = 0;
  size_t i;

  for (i = 0; i < len; i++) {
    int ch = buf[i];

    if (numinrow == 16) {
      int j;

      if (rows_eq(oldchs, chs)) {
        if (!showed_dots) {
          showed_dots = 1;
          printf("          .. .. .. .. .. .. .. .. : .. .. .. .. .. .. .. ..\n");
        }
      } else {
        showed_dots = 0;
        dump_row(count, numinrow, chs);
      }

      for (j=0; j<16; j++) {
        oldchs[j] = chs[j];
      }

      numinrow = 0;
    }

    count++;
    chs[numinrow++] = ch;
  }

  dump_row(count, numinrow, chs);

  if (numinrow != 0) {
    printf("%08lX:\n", count);
  }
}