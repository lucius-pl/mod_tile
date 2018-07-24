/* wrapper for storage engines
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef HAVE_PTHREAD
#include <pthread.h>
#endif

#ifdef RENDERD
#include <syslog.h>
#else
#include <time.h>
#endif


#include "store.h"
#include "store_file.h"
#include "store_memcached.h"
#include "store_rados.h"
#include "store_ro_http_proxy.h"
#include "store_ro_composite.h"
#include "store_null.h"

#define MSG_SIZE 1000
#define LOG_SIZE MSG_SIZE + 100

static short store_log_level = STORE_LOGLVL_WARNING;

//TODO: Make this function handle different logging backends, depending on if on compiles it from apache or something else
void log_message(int log_lvl, const char *format, ...) {

    /* skip message above defined log level */
    if(log_lvl > store_log_level) {
        return;
    }

    #ifdef HAVE_PTHREAD
    pthread_t tid = pthread_self();
    #endif
    va_list ap;
    char msg[MSG_SIZE];
    char log[LOG_SIZE];

    va_start(ap, format);
    vsnprintf(msg, MSG_SIZE, format, ap);
    va_end(ap);

    #ifndef RENDERD
        int pid = getpid();
        time_t t = time(NULL);
        char* ct = ctime(&t);
        /* remove \n at the end */
        *(ct + strlen(ct) - 1) = 0;
    #endif

    switch (log_lvl) {
        case STORE_LOGLVL_DEBUG:
            #ifdef RENDERD
                #ifdef HAVE_PTHREAD
                    sprintf(log, "[debug] [%lu]: %s", tid, msg);
                #else
                    sprintf(log, "[debug]: %s", msg);
                #endif
            #else
                #ifdef HAVE_PTHREAD
                    sprintf(log, "[%s] [tile:debug] [%d][%lu]: %s\n", ct, pid, tid, msg);
                #else
                    sprintf(log, "[%s] [tile:debug] [%d]: %s\n", ct, pid, msg);
                #endif
            #endif
            break;
        case STORE_LOGLVL_INFO:
            #ifdef RENDERD
                sprintf(log, "[info]: %s", msg);
            #else
                sprintf(log, "[%s] [tile:info] [%d]: %s\n", ct, pid, msg);
            #endif
            break;
        case STORE_LOGLVL_WARNING:
            #ifdef RENDERD
                sprintf(log, "[WARNING]: %s", msg);
            #else
                sprintf(log, "[%s] [tile:warn] [%d]: %s\n", ct, pid, msg);
            #endif
            break;
        case STORE_LOGLVL_ERR:
            #ifdef RENDERD
                sprintf(log, "[ERR]: %s", msg);
            #else
                sprintf(log, "[%s] [tile:error] [%d]: %s\n", ct, pid, msg);
            #endif
            break;
    }

    #ifdef RENDERD
      syslog(log_lvl, msg);
    #else
      fputs(log, stderr);
      fflush(stderr);
    #endif
}

/**
 * In Apache 2.2, we call the init_storage_backend once per process. For mpm_worker and mpm_event multiple threads therefore use the same
 * storage context, and all storage backends need to be thread-safe in order not to cause issues with these mpm's
 *
 * In Apache 2.4, we call the init_storage_backend once per thread, and therefore each thread has its own storage context to work with.
 */
struct storage_backend * init_storage_backend(const char * options, const short store_log_level_) {
    struct stat st;
    struct storage_backend * store = NULL;
    store_log_level = store_log_level_;

    log_message(STORE_LOGLVL_DEBUG, "init_storage_backend: StoreLogLevel: %s", get_store_log_level_name(store_log_level));


    //Determine the correct storage backend based on the options string
    if (strlen(options) == 0) {
        log_message(STORE_LOGLVL_ERR, "init_storage_backend: Options string was empty");
        return NULL;
    }
    if (options[0] == '/') {
        if (stat(options, &st) != 0) {
            log_message(STORE_LOGLVL_ERR, "init_storage_backend: Failed to stat %s with error: %s", options, strerror(errno));
            return NULL;
        }
        if (S_ISDIR(st.st_mode)) {
            log_message(STORE_LOGLVL_DEBUG, "init_storage_backend: initialising file storage backend at: %s", options);
            store = init_storage_file(options);
            return store;
        } else {
            log_message(STORE_LOGLVL_ERR, "init_storage_backend: %s is not a directory", options, strerror(errno));
            return NULL;
        }
    }
    if (strstr(options,"rados://") == options) {
        log_message(STORE_LOGLVL_DEBUG, "init_storage_backend: initialising rados storage backend at: %s", options);
        store = init_storage_rados(options);
        return store;
    }
    if (strstr(options,"memcached://") == options) {
        log_message(STORE_LOGLVL_DEBUG, "init_storage_backend: initialising memcached storage backend at: %s", options);
        store = init_storage_memcached(options);
        return store;
    }
    if (strstr(options,"ro_http_proxy://") == options) {
        log_message(STORE_LOGLVL_DEBUG, "init_storage_backend: initialising ro_http_proxy storage backend at: %s", options);
        store = init_storage_ro_http_proxy(options);
        return store;
    }
    if (strstr(options,"composite:{") == options) {
        log_message(STORE_LOGLVL_DEBUG, "init_storage_backend: initialising ro_composite storage backend at: %s", options);
        store = init_storage_ro_composite(options);
        return store;
    }
    if (strstr(options,"null://") == options) {
        log_message(STORE_LOGLVL_DEBUG, "init_storage_backend: initialising null storage backend at: %s", options);
        store = init_storage_null();
        return store;
    }

    log_message(STORE_LOGLVL_ERR, "init_storage_backend: No valid storage backend found for options: %s", options);

    return store;
}

/**
 * get store log level value by name
 */
short get_store_log_level_value(const char* name) {

    if(strcasecmp(name, "debug") == 0) {
        return STORE_LOGLVL_DEBUG;
    } else if(strcasecmp(name, "info") == 0) {
        return STORE_LOGLVL_INFO;
    } else if(strcasecmp(name, "warn") == 0) {
        return STORE_LOGLVL_WARNING;
    } else if(strcasecmp(name, "error") == 0) {
        return STORE_LOGLVL_ERR;
    } else {
       return -1;
    }
}

/**
 * get store log level name by value
 */
char* get_store_log_level_name(short value) {

    switch(value) {
        case STORE_LOGLVL_DEBUG:
            return "debug";
        case STORE_LOGLVL_INFO:
            return "info";
        case STORE_LOGLVL_WARNING:
            return "warn";
        case STORE_LOGLVL_ERR:
            return "error";
        default:
            return NULL;
    }
}
