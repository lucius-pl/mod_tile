#ifndef STORES3_H
#define STORES3_H

#ifdef __cplusplus
extern "C"
{
#endif

#define TIMEOUT 10000

#include "store.h"

struct storage_backend* init_storage_s3(const char *connection_string);


#ifdef HAVE_LIBDSAA

typedef enum {
    CREATE_DIR = 1,
    CREATE_FILE = 2,
    READ = 3,
    UPDATE = 4,
    DELETE = 5
} MsgQueType;

int store_s3_cache_send_msg(char*, MsgQueType);

#endif

#ifdef __cplusplus
}
#endif
#endif
