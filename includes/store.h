#ifndef STORE_H
#define STORE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <sys/types.h>
#include <syslog.h>
#include "render_config.h"

#define STORE_LOGLVL_DEBUG    LOG_DEBUG
#define STORE_LOGLVL_INFO     LOG_INFO
#define STORE_LOGLVL_WARNING  LOG_WARNING
#define STORE_LOGLVL_ERR      LOG_ERR


    typedef enum {unknow, renderd, cache, s3} tile_origin;

    struct stat_info {
        off_t     size;    /* total size, in bytes */
        time_t    atime;   /* time of last access */
        time_t    mtime;   /* time of last modification */
        time_t    ctime;   /* time of last status change */
        int       expired; /* has the tile expired */
        tile_origin origin;  /* origin of a tile */
        short aborted;		/* is a request canceled */
    };

    struct storage_backend {
        int (*tile_read)(struct storage_backend * store, const char *xmlconfig, const char *options, int x, int y, int z, char *buf, size_t sz, int * compressed, char * err_msg);
        struct stat_info (*tile_stat)(struct storage_backend * store, const char *xmlconfig, const char *options, int x, int y, int z);
        int (*metatile_write)(struct storage_backend * store, const char *xmlconfig, const char *options, int x, int y, int z, const char *buf, int sz);
        int (*metatile_delete)(struct storage_backend * store, const char *xmlconfig, int x, int y, int z);
        int (*metatile_expire)(struct storage_backend * store, const char *xmlconfig, int x, int y, int z);
        char * (*tile_storage_id)(struct storage_backend * store, const char *xmlconfig, const char *options, int x, int y, int z, char * string);
        int (*close_storage)(struct storage_backend * store);
        void (*tile_cancel)(struct storage_backend * store, const char *xmlconfig, const char *options, int x, int y, int z);
        void * storage_ctx;
        int socket;
        long timeout;
    };

    void log_message(int log_lvl, const char *format, ...);

    struct storage_backend * init_storage_backend(const char * options);


    char* tile_origin_name(tile_origin);


#ifdef __cplusplus
}
#endif
#endif
