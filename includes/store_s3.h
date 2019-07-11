#ifndef STORES3_H
#define STORES3_H

#ifdef __cplusplus
extern "C"
{
#endif

#define TIMEOUT 10000

#include "store.h"

struct storage_backend* init_storage_s3(const char *connection_string);

#define PIPE_DIR "/var/run/renderd"

#ifdef __cplusplus
}
#endif
#endif
