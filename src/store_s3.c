#include "config.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>
#include <sys/types.h>
#include <pthread.h>
#include <regex.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/msg.h>
#include <time.h>
#include <pwd.h>
#include <sys/file.h>
#include <dirent.h>
#include <libgen.h>

#ifdef HAVE_LIBS3
#include <libs3.h>
#endif

#ifdef HAVE_LIBDSAA
#include <libdsaa.h>
#endif

#include "store.h"
#include "store_file_utils.h"
#include "store_s3.h"
#include "metatile.h"
#include "render_config.h"
#include "protocol.h"


#ifdef HAVE_LIBS3

#define DEFAULT_CACHE_SIZE "100MB"
#define MSG request.error_details && request.error_details->message ? request.error_details->message : ""

typedef struct s3_storage_cache {
    #ifdef HAVE_LIBDSAA
    struct list fileList;
    struct list dirList;
    long int fileSize;
    long int dirSize;
    int fileCount;
    int dirCount;
    long int size;
    #endif
    char *path;
    char *sizeUnit;
} S3Cache;


static pthread_mutex_t qLock;
static int store_s3_initialized = 0;

struct s3_tile_request {
    const char *path;
    size_t tile_size;
    char *tile;
    int64_t tile_mod_time;
    int tile_expired;
    size_t cur_offset;
    S3Status result;
    const S3ErrorDetails *error_details;
};

struct store_s3_ctx {
    S3BucketContext* ctx;
    const char *basepath;
    char *urlcopy;
    S3Cache s3Cache;
};


/*****************************************************************************/

#ifdef HAVE_LIBDSAA

#define MESSAGE_QUEUE_KEY 1975

struct msg_que_data {
   long mtype;
   char mtext[PATH_MAX];
};

#ifdef RENDERD

static pthread_mutex_t cache_cleaner_lock;
static pthread_t cache_cleaner_thread = 0;

typedef enum {SORT, LAST} list_add_mode;

struct list_data {
    char path[PATH_MAX];
    time_t atime;
    off_t size;
};

/*****************************************************************************/

int s3_cache_list_compare_item(void* v1, void* v2) {
    struct list_data *d1 = (struct list_data*)v1;
    struct list_data *d2 = (struct list_data*)v2;

    if(d1->atime < d2->atime) {
        return -1;
    } else if(d1->atime > d2->atime) {
        return 1;
    } else {
        return 0;
    }
}

/*****************************************************************************/

void s3_cache_list_print_item(int i, void* v) {
    struct list_data *d = (struct list_data*)v;
    log_message(STORE_LOGLVL_DEBUG, "%d path=%s size=%ld atime=%ld", i, d->path, d->size, d->atime);
}

/*****************************************************************************/

int s3_cache_list_find_item(void* v1, void* v2) {
    struct list_data *d = (struct list_data*)v1;
    char* path = (char*)v2;

    return strcmp(d->path, path) == 0 ? 1 : 0;
}

/*****************************************************************************/

void s3_cache_list_update_item(void* v1, void* v2, void* v3) {
    struct list_data *d = (struct list_data*)v1;
    time_t atime  = *((time_t*)v2);
    d->atime = atime;
}

/*****************************************************************************/

void s3_cache_list_release_item(void* v) {
    struct list_data *d = (struct list_data*)v;
    free(d);
}

/*****************************************************************************/

int s3_cache_add_dir(char* path, S3Cache *s3Cache) {
    struct stat fs;
    struct list_data *ld;

    if(stat(path, &fs) != 0) {
        log_message(STORE_LOGLVL_ERR, "s3_cache_add_dir: error to stat dir: %s %d %s", path, errno, strerror(errno));
        return -1;
    }

    ld = malloc(sizeof(struct list_data));
    if(ld == NULL) {
        log_message(STORE_LOGLVL_ERR, "s3_cache_add_dir: error to allocate memory: %d %s", errno, strerror(errno));
        return -1;
    }

    strncpy(ld->path, path, PATH_MAX);
    ld->size = fs.st_size;
    ld->atime = ((struct timespec)fs.st_atim).tv_sec;

    s3Cache->dirSize +=  fs.st_size;
    s3Cache->dirCount++;

    list_add(&s3Cache->dirList, ld);

    return 0;
}

/*****************************************************************************/

int s3_cache_add_file(char* path, S3Cache *s3Cache, list_add_mode mode) {
    struct stat fs;
    struct list_data *ld;

    if(stat(path, &fs) != 0) {
        log_message(STORE_LOGLVL_ERR, "s3_cache_add_file: error to stat file: %s %d %s", path, errno, strerror(errno));
        return -1;
    }

    ld = malloc(sizeof(struct list_data));
    if(ld == NULL) {
        log_message(STORE_LOGLVL_ERR, "s3_cache_add_file: error to allocate memory: %d %s", errno, strerror(errno));
        return -1;
    }

    strncpy(ld->path, path, PATH_MAX);
    ld->atime = ((struct timespec)fs.st_atim).tv_sec;
    ld->size = fs.st_size;

    if(mode == SORT) {
        list_add_sort(&s3Cache->fileList, ld);
    } else if (mode == LAST) {
        list_add(&s3Cache->fileList, ld);
    }

    s3Cache->fileSize += fs.st_size;
    s3Cache->fileCount++;

    return 0;
}

/*****************************************************************************/

int s3_cache_read_dir(char* name, S3Cache *s3Cache) {
    DIR *dir;
    struct dirent *file;
    char path[PATH_MAX];
    int r = 0;
    list_add_mode mode = SORT;

    r = s3_cache_add_dir(name, s3Cache);

    if((dir = opendir(name)) == NULL) {
        log_message(STORE_LOGLVL_ERR, "tile_cache_read_dir: error to open dir: %s %d %s", name, errno, strerror(errno));
        return -1;
    }

    while ((file = readdir(dir)) != NULL) {
        if (!strcmp(file->d_name, ".") || !strcmp(file->d_name, "..") || !strcmp(file->d_name, "lost+found")) {
            continue;
        }

        snprintf(path, PATH_MAX, "%s/%s", name, file->d_name);
        if(file->d_type == DT_REG) {
            r = s3_cache_add_file(path, s3Cache, mode);
        } else if(file->d_type == DT_DIR) {
            r = s3_cache_read_dir(path, s3Cache);
        }
    }
    closedir(dir);
    return r;
}

/*****************************************************************************/

void s3_cache_print(S3Cache* s3Cache) {

    list_print(&s3Cache->fileList, list_item_first);
    log_message(STORE_LOGLVL_DEBUG, "S3 cache file size: %ld", s3Cache->fileSize);
    log_message(STORE_LOGLVL_DEBUG, "S3 cache file count: %d", s3Cache->fileCount);

    list_print(&s3Cache->dirList, list_item_first);
    log_message(STORE_LOGLVL_DEBUG, "S3 cache dir size: %ld", s3Cache->dirSize);
    log_message(STORE_LOGLVL_DEBUG, "S3 cache dir count: %d", s3Cache->dirCount);

    log_message(STORE_LOGLVL_DEBUG, "S3 cache size: %ld, S3 cache size limit: %ld", s3Cache->fileSize + s3Cache->dirSize, s3Cache->size);
}

/*****************************************************************************/

int s3_cache_update_file(S3Cache *s3Cache, char* path) {
    struct stat fs;
    list_item_position p = list_item_last;

    if(stat(path, &fs) != 0) {
        log_message(STORE_LOGLVL_ERR, "tile_cache_update_file: error to stat file: %s %d %s", path, errno, strerror(errno));
        return -1;
    }

    time_t atime = ((struct timespec)fs.st_atim).tv_sec;
    list_move(&s3Cache->fileList, path, p, &atime, NULL);

    return 0;
}

/*****************************************************************************/

int s3_cache_remove_dir(S3Cache *s3Cache, char* path) {
    DIR *dir;
    struct dirent *file;
    struct list_data *ld;
    int i = 0;
    char* dirPath = dirname(path);

    if(! strcmp(dirPath, s3Cache->path)) {
        return 0;
    }

    if((dir = opendir(dirPath)) == NULL) {
        log_message(STORE_LOGLVL_ERR, "s3_cache_remove_dir: error to open dir: %s %d %s", ld->path, errno, strerror(errno));
        return -1;
    }

    while ((file = readdir(dir)) != NULL) {
        if ( ! (! strcmp(file->d_name, ".") || ! strcmp(file->d_name, "..")) ) {
            i++;
        }
    }
    closedir(dir);

    log_message(STORE_LOGLVL_DEBUG, "Files in dir: %s=%d", dirPath, i);

    if(i > 0) {
        return 0;
    }

    if(rmdir(dirPath) == -1) {
        log_message(STORE_LOGLVL_ERR, "s3_cache_remove_dir: error to remove dir: %s %d %s", ld->path, errno, strerror(errno));
        return -1;
    }

    list_get_find(&s3Cache->dirList, dirPath, (void**)&ld);
    s3Cache->dirSize -= ld->size;
    s3Cache->dirCount--;

    log_message(STORE_LOGLVL_DEBUG, "Deleted dir: %s", ld->path);

    list_remove_find(&s3Cache->dirList, ld->path);

    s3_cache_remove_dir(s3Cache, dirPath);

    return 1;
}

/*****************************************************************************/

int s3_cache_reduce_size(S3Cache *s3Cache) {

    list_item_position p = list_item_first;
    struct list_data *ld;

    if(s3Cache->fileSize + s3Cache->dirSize <= s3Cache->size) {
        return 0;
    }

    log_message(STORE_LOGLVL_INFO, "Reducing tile cache size is needed: %ld > %ld", s3Cache->fileSize + s3Cache->dirSize,  s3Cache->size);

    while(s3Cache->fileSize + s3Cache->dirSize > s3Cache->size) {
        list_get(&s3Cache->fileList, p, (void**)&ld);
        if(unlink(ld->path) == -1) {
            log_message(STORE_LOGLVL_ERR, "s3_cache_reduce_size: error to remove file: %s %d %s", ld->path, errno, strerror(errno));
            return -1;
        }
        s3Cache->fileSize -= ld->size;
        s3Cache->fileCount--;
        log_message(STORE_LOGLVL_DEBUG, "Deleted file from S3 cache: %s", ld->path);
        s3_cache_remove_dir(s3Cache, ld->path);
        list_remove(&s3Cache->fileList, p);
    }

    return 1;
}

/*****************************************************************************/

int s3_cache_process_event(S3Cache *s3Cache) {
    char path[PATH_MAX];
    struct list_data *ld;
    list_add_mode mode = LAST;
    int msqid;
    struct msg_que_data msqdata;

    if((msqid = msgget(MESSAGE_QUEUE_KEY, IPC_CREAT | S_IRUSR | S_IWUSR | S_IWGRP)) == -1) {
        log_message(STORE_LOGLVL_ERR, "store_s3_cache_recive_msg: failed to create meesage queue for key=%d: %s", MESSAGE_QUEUE_KEY, strerror(errno));
        return -1;
    }

    while(1) {
        if(msgrcv(msqid, &msqdata, sizeof(msqdata.mtext), 0, 0) > 0) {
            log_message(STORE_LOGLVL_DEBUG, "recived meesage from queue id=%d: mtype=%ld mtext=%s", msqid, msqdata.mtype, msqdata.mtext);
            char* path = msqdata.mtext;

            switch(msqdata.mtype) {
                case CREATE_DIR:
                    s3_cache_add_dir(path, s3Cache);
                    s3_cache_print(s3Cache);
                    break;
                case CREATE_FILE:
                    s3_cache_add_file(path, s3Cache, mode);
                    s3_cache_print(s3Cache);
                    s3_cache_reduce_size(s3Cache);
                    s3_cache_print(s3Cache);
                    break;
                case READ:
                    s3_cache_update_file(s3Cache, path);
                    s3_cache_print(s3Cache);
                    break;
            }
        }
    }

    if(msgctl(msqid, IPC_RMID, 0) == -1) {
        log_message(STORE_LOGLVL_ERR, "store_s3_cache_recive_msg: failed to remove meesage queue id=%d: %s", msqid, strerror(errno));
        return -1;
    }

    return 0;
}

/*****************************************************************************/

long s3_cache_convert_size(char* sizeUnit) {
    long defaultSize = 100 * 1048576; //100MB
    int length = strlen(sizeUnit) - 2;
    char* buf = malloc(sizeof(char));
    if(buf == NULL) {
        return defaultSize;
    }
    sprintf(buf, "%.*s", length, sizeUnit);
    long size = atol(buf);
    free(buf);
    char unit = *(sizeUnit +  length);

    switch(unit) {
        case 'K':
            return size * 1024;
        case 'M':
            return size * 1048576;
        case 'G':
            return size * 1073741824;
        case 'T':
            return size * 1099511627776;
    }

    return defaultSize;
}

/*****************************************************************************/

static void* exec_cache_cleaner_thread(void* v) {
    S3Cache s3Cache = *((S3Cache*)v);
    struct list_function listFun;

    s3Cache.fileSize = 0;
    s3Cache.dirSize = 0;
    s3Cache.fileCount = 0;
    s3Cache.dirCount = 0;
    s3Cache.size = s3_cache_convert_size(s3Cache.sizeUnit);

    listFun.compare = &s3_cache_list_compare_item;
    listFun.print = &s3_cache_list_print_item;
    listFun.find = &s3_cache_list_find_item;
    listFun.release = &s3_cache_list_release_item; 
    listFun.update = &s3_cache_list_update_item; 

    #ifdef DEBUG
    list_debug(1);
    #endif
    list_init(&s3Cache.fileList, &listFun);
    list_init(&s3Cache.dirList,  &listFun);


    if(s3_cache_read_dir(s3Cache.path, &s3Cache) == -1) {
        log_message(STORE_LOGLVL_ERR, "exec_cache_cleaner_thread: error reading tile cache directory: %s", s3Cache.path);
    }

    s3_cache_print(&s3Cache);
    s3_cache_reduce_size(&s3Cache);
    s3_cache_print(&s3Cache);

    s3_cache_process_event(&s3Cache);

    list_release(&s3Cache.fileList);
    list_release(&s3Cache.dirList);

    return 0;
}

#endif
#endif

/*****************************************************************************/

static int store_s3_xyz_to_storagekey(struct storage_backend *store, const char *xmlconfig, const char *options, int x, int y, int z, char *key, size_t keylen)
{
    int offset;
    if (options) {
        offset = xyzo_to_meta(key, keylen, ((struct store_s3_ctx*) (store->storage_ctx))->basepath, xmlconfig, options, x, y, z);
    } else {
        offset = xyz_to_meta(key, keylen, ((struct store_s3_ctx*) (store->storage_ctx))->basepath, xmlconfig, x, y, z);
    }

    return offset;
}

/*****************************************************************************/

static S3Status store_s3_properties_callback(const S3ResponseProperties *properties, void *callbackData)
{
    struct s3_tile_request *rqst = (struct s3_tile_request*) callbackData;

    rqst->tile_size = properties->contentLength;
    rqst->tile_mod_time = properties->lastModified;
    rqst->tile_expired = 0;
    const S3NameValue *respMetadata = properties->metaData;
    for (int i = 0; i < properties->metaDataCount; i++) {
        if (0 == strcmp(respMetadata[i].name, "expired")) {
            rqst->tile_expired = atoi(respMetadata[i].value);
        }
    }

    //log_message(STORE_LOGLVL_DEBUG, "store_s3_properties_callback: got properties for tile %s, length: %ld, content type: %s, expired: %d", rqst->path, rqst->tile_size, properties->contentType, rqst->tile_expired);

    return S3StatusOK;
}

/*****************************************************************************/

S3Status store_s3_object_data_callback(int bufferSize, const char *buffer, void *callbackData)
{
    struct s3_tile_request *rqst = (struct s3_tile_request*) callbackData;

    if (rqst->cur_offset == 0 && rqst->tile == NULL) {
        //log_message(STORE_LOGLVL_DEBUG, "store_s3_object_data_callback: allocating %z byte buffer for tile", rqst->tile_size);
        rqst->tile = malloc(rqst->tile_size);
        if (NULL == rqst->tile) {
            log_message(STORE_LOGLVL_ERR, "store_s3_object_data_callback: could not allocate %z byte buffer for tile!", rqst->tile_size);
            return S3StatusOutOfMemory;
        }
    }

    //log_message(STORE_LOGLVL_DEBUG, "store_s3_object_data_callback: appending %ld bytes to buffer, new offset %ld", bufferSize, rqst->cur_offset + bufferSize);
    memcpy(rqst->tile + rqst->cur_offset, buffer, bufferSize);
    rqst->cur_offset += bufferSize;
    return S3StatusOK;
}

/*****************************************************************************/

int store_s3_put_object_data_callback(int bufferSize, char *buffer, void *callbackData)
{
    struct s3_tile_request *rqst = (struct s3_tile_request*) callbackData;
    if (rqst->cur_offset == rqst->tile_size) {
        // indicate "end of data"
        log_message(STORE_LOGLVL_DEBUG, "store_s3_put_object_data_callback: completed put");
        return 0;
    }
    size_t bytesToWrite = MIN(bufferSize, rqst->tile_size - rqst->cur_offset);
    //log_message(STORE_LOGLVL_DEBUG, "store_s3_put_object_data_callback: uploading data, writing %ld bytes to buffer, cur offset %ld, new offset %ld", bytesToWrite, rqst->cur_offset, rqst->cur_offset + bytesToWrite);
    memcpy(buffer, rqst->tile + rqst->cur_offset, bytesToWrite);
    rqst->cur_offset += bytesToWrite;
    return bytesToWrite;
}

/*****************************************************************************/

void store_s3_complete_callback(S3Status status, const S3ErrorDetails *errorDetails, void *callbackData)
{
    struct s3_tile_request *rqst = (struct s3_tile_request*) callbackData;
    //log_message(STORE_LOGLVL_DEBUG, "store_s3_complete_callback: request complete, status %d (%s)", status, S3_get_status_name(status));
    //if (errorDetails && errorDetails->message && (strlen(errorDetails->message) > 0)) {
    //    log_message(STORE_LOGLVL_DEBUG, "  error details: %s", errorDetails->message);
    //}
    rqst->result = status;
    rqst->error_details = errorDetails;
}

/*****************************************************************************/

static struct s3_tile_request store_s3_get_tile_from_s3(S3BucketContext* ctx, const char* path, const char* source) {

    struct S3GetObjectHandler getObjectHandler;
    getObjectHandler.responseHandler.propertiesCallback = &store_s3_properties_callback;
    getObjectHandler.responseHandler.completeCallback = &store_s3_complete_callback;
    getObjectHandler.getObjectDataCallback = &store_s3_object_data_callback;

    struct s3_tile_request request;
    request.path = path;
    request.cur_offset = 0;
    request.tile = NULL;
    request.tile_expired = 0;
    request.tile_mod_time = 0;
    request.tile_size = 0;

    S3_get_object(ctx, path, NULL, 0, 0, NULL, TIMEOUT, &getObjectHandler, &request);

    if (request.result != S3StatusOK) {
        log_message(STORE_LOGLVL_ERR, "%s: failed to get metatile from S3: %d(%s)/%s", source, request.result, S3_get_status_name(request.result), MSG);
        return request;
    }

    log_message(STORE_LOGLVL_DEBUG, "%s: got metatile from S3: %s", source, path);

    return request;
}

/*****************************************************************************/

#ifdef HAVE_LIBDSAA

extern int store_s3_cache_send_msg(char* path, MsgQueType msqtype) {
    int msqid;
    struct msg_que_data msqdata;


    if((msqid = msgget(MESSAGE_QUEUE_KEY, 0)) == -1) {
        log_message(STORE_LOGLVL_ERR, "store_s3_cache_send_msg: failed to get meesage queue id for key=%d: %s", MESSAGE_QUEUE_KEY, strerror(errno));
        return -1;
    }

    msqdata.mtype = msqtype;
    strncpy(msqdata.mtext, path, PATH_MAX);

    if(msgsnd(msqid, &msqdata, sizeof(msqdata.mtext),  IPC_NOWAIT ) == -1) {
        log_message(STORE_LOGLVL_ERR, "store_s3_cache_send_msg: failed to send meesage to queue id=%d: %s", msqid, strerror(errno));
        return -1;
    } else {
        log_message(STORE_LOGLVL_DEBUG, "store_s3_cache_send_msg: sent meesage to queue id=%d: mtype=%ld mtext=%s", msqid, msqdata.mtype, msqdata.mtext);
    }

    return 0;
}

#endif

/*****************************************************************************/

static int store_s3_save_tile_to_cache(const char* metatile, size_t metatile_size, char* cachePath, const char* source) {

    mode_t pumask = umask(0);
    log_message(STORE_LOGLVL_DEBUG, "store_s3_save_tile_to_cache: reset umask to 0, previous umask is (%04o)", pumask);

    if (mkdirp(cachePath)) {
         log_message(STORE_LOGLVL_ERR, "%s: error creating S3 cache directory structure for meta tile: %s", source, cachePath);
         return -1;
    }

    int fd = open(cachePath, O_WRONLY | O_TRUNC | O_CREAT, 0666);
    if (fd < 0) {
        log_message(STORE_LOGLVL_ERR, "%s: error creating metatile %s in S3 cache: %s", source, cachePath, strerror(errno));
        return -1;
    }

    if(flock(fd, LOCK_EX) == -1) {
        log_message(STORE_LOGLVL_ERR, "%s: error apply lock on %s in S3 cache: %s", source, cachePath, strerror(errno));
        return -1;
    }

    int res = write(fd, metatile, metatile_size);
    if (res != metatile_size) {
        log_message(STORE_LOGLVL_ERR, "%s: error writing metatile %s to S3 cache: %s", source, cachePath, strerror(errno));
        close(fd);
        return -1;
    }
    close(fd);

    log_message(STORE_LOGLVL_DEBUG, "%s: save metatile %s to S3 cache", source, cachePath);


    #ifdef HAVE_LIBDSAA

    /* inform cleaner about a new file in S3 cache */

    MsgQueType msqtype = CREATE_FILE;
    store_s3_cache_send_msg(cachePath, msqtype);

    #endif

    return 0;
}

/*****************************************************************************/

static int store_s3_tile_read_with_cache(struct storage_backend *store, const char *xmlconfig, const char *options, int x, int y, int z, char *buf, size_t sz, int *compressed, char *log_msg) {
    char path[PATH_MAX];
    struct store_s3_ctx *ctx = (struct store_s3_ctx*) store->storage_ctx;
    struct stat st_stat;
    struct s3_tile_request request;
    char *metatile;
    size_t metatile_size;

    /* check if metatile file exists in cache */

    int meta_offset = xyzo_to_meta(path, PATH_MAX, ctx->s3Cache.path, xmlconfig, options, x, y, z);

    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_read: metatile %s does not exist in S3 cache: %s", path, strerror(errno));

        /* get metatile file from S3 */
        char s3Path[PATH_MAX];
        meta_offset = store_s3_xyz_to_storagekey(store, xmlconfig, options, x, y, z, s3Path, PATH_MAX);
        request = store_s3_get_tile_from_s3(ctx->ctx, s3Path, "store_s3_tile_read");
        if(request.tile == NULL) {
            return -1;
        }
        metatile = request.tile;
        metatile_size = request.tile_size;

        /* save metatile file to cache */

        store_s3_save_tile_to_cache(request.tile, request.tile_size, path, "store_s3_tile_read");

    } else {

        /* get metatile file from cache */

        if(fstat(fd, &st_stat)) {
            log_message(STORE_LOGLVL_ERR, "store_s3_tile_read: failed to get stat for %s metatile from cache: %s", path, strerror(errno));
            return - 1;
        }

        metatile_size = st_stat.st_size;
        metatile = (char*)malloc(sizeof(char) * metatile_size);
        if(metatile == NULL) {
            log_message(STORE_LOGLVL_ERR, "store_s3_tile_read: failed to allocate memory for %s metatile from cache: %s", path, strerror(errno));
            close(fd);
            return -1;
        }

        int pos = 0;
        while (pos < metatile_size) {
           size_t len = metatile_size - pos;
           int got = read(fd, metatile + pos, len);
           if (got < 0) {
               log_message(STORE_LOGLVL_ERR, "store_s3_tile_read: failed to read data from %s metatile from cache: %s", path, strerror(errno));
               close(fd);
               free(metatile);
               return -2;
           } else if (got > 0) {
               pos += got;
           } else {
              break;
           }
        }
        close(fd);
        log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_read: retrieved metatile from cache: %s", path);

        #ifdef HAVE_LIBDSAA

        /* inform cleaner about reading a file from S3 cache */

        MsgQueType msqtype = READ;
        store_s3_cache_send_msg(path, msqtype);

        #endif
    }

    /* extract tile from metatile */

    if (metatile_size < METATILE_HEADER_LEN) {
        snprintf(log_msg, PATH_MAX - 1, "Meta file %s too small to contain header\n", path);
        free(metatile);
        return -3;
    }
    struct meta_layout *m = (struct meta_layout*)metatile;

    if (memcmp(m->magic, META_MAGIC, strlen(META_MAGIC))) {
        if (memcmp(m->magic, META_MAGIC_COMPRESSED, strlen(META_MAGIC_COMPRESSED))) {
            snprintf(log_msg, PATH_MAX - 1, "Meta file %s header magic mismatch\n", path);
            free(metatile);
            return -4;
        } else {
            *compressed = 1;
        }
    } else {
        *compressed = 0;
    }

    if (m->count != (METATILE * METATILE)) {
        snprintf(log_msg, PATH_MAX - 1, "Meta file %s header bad count %d != %d\n", path, m->count, METATILE * METATILE);
        free(metatile);
        return -5;
    }

    int buffer_offset = m->index[meta_offset].offset;
    int tile_size = m->index[meta_offset].size;

    if (tile_size > sz) {
        snprintf(log_msg, PATH_MAX - 1, "tile of length %d too big to fit buffer of length %zd\n", tile_size, sz);
        free(metatile);
        return -6;
    }

    memcpy(buf, metatile + buffer_offset, tile_size);

    free(metatile);


    return tile_size;
}

/*****************************************************************************/

static int store_s3_tile_read_without_cache(struct storage_backend *store, const char *xmlconfig, const char *options, int x, int y, int z, char *buf, size_t sz, int *compressed, char *log_msg)
{
    struct store_s3_ctx *ctx = (struct store_s3_ctx*) store->storage_ctx;
    char *path = malloc(PATH_MAX);

    //log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_read: fetching tile");

    int tile_offset = store_s3_xyz_to_storagekey(store, xmlconfig, options, x, y, z, path, PATH_MAX);
    //log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_read: retrieving object %s", path);

    struct S3GetObjectHandler getObjectHandler;
    getObjectHandler.responseHandler.propertiesCallback = &store_s3_properties_callback;
    getObjectHandler.responseHandler.completeCallback = &store_s3_complete_callback;
    getObjectHandler.getObjectDataCallback = &store_s3_object_data_callback;

    struct s3_tile_request request;
    request.path = path;
    request.cur_offset = 0;
    request.tile = NULL;
    request.tile_expired = 0;
    request.tile_mod_time = 0;
    request.tile_size = 0;

    S3_get_object(ctx->ctx, path, NULL, 0, 0, NULL, TIMEOUT, &getObjectHandler, &request);

    if (request.result != S3StatusOK) {
        const char *msg = "";
        if (request.error_details && request.error_details->message) {
            msg = request.error_details->message;
        }
        log_message(STORE_LOGLVL_ERR, "store_s3_tile_read: failed to retrieve object: %d(%s)/%s", request.result, S3_get_status_name(request.result), msg);
        free(path);
        path = NULL;
        return -1;
    }

    log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_read: retrieved metatile %s of size %i", path, request.tile_size);

    free(path);
    path = NULL;

    // extract tile from metatile

    if (request.tile_size < METATILE_HEADER_LEN) {
        snprintf(log_msg, PATH_MAX - 1, "Meta file %s too small to contain header\n", path);
        free(request.tile);
        return -3;
    }
    struct meta_layout *m = (struct meta_layout*) request.tile;

    if (memcmp(m->magic, META_MAGIC, strlen(META_MAGIC))) {
        if (memcmp(m->magic, META_MAGIC_COMPRESSED, strlen(META_MAGIC_COMPRESSED))) {
            snprintf(log_msg, PATH_MAX - 1, "Meta file %s header magic mismatch\n", path);
            free(request.tile);
            return -4;
        } else {
            *compressed = 1;
        }
    } else {
        *compressed = 0;
    }

    if (m->count != (METATILE * METATILE)) {
        snprintf(log_msg, PATH_MAX - 1, "Meta file %s header bad count %d != %d\n", path, m->count, METATILE * METATILE);
        free(request.tile);
        return -5;
    }

    int buffer_offset = m->index[tile_offset].offset;
    int tile_size = m->index[tile_offset].size;

    if (tile_size > sz) {
        snprintf(log_msg, PATH_MAX - 1, "tile of length %d too big to fit buffer of length %zd\n", tile_size, sz);
        free(request.tile);
        return -6;
    }

    memcpy(buf, request.tile + buffer_offset, tile_size);

    free(request.tile);
    request.tile = NULL;

    return tile_size;
}

/*****************************************************************************/

static int store_s3_tile_read(struct storage_backend *store, const char *xmlconfig, const char *options, int x, int y, int z, char *buf, size_t sz, int *compressed, char *log_msg) {

    const char* cachePath = ((struct store_s3_ctx*)store->storage_ctx)->s3Cache.path;

    if(strlen(cachePath) > 0 ) {
        return store_s3_tile_read_with_cache(store, xmlconfig, options, x, y, z, buf, sz, compressed, log_msg);
    } else {
        return store_s3_tile_read_without_cache(store, xmlconfig, options, x, y, z, buf, sz, compressed, log_msg);
    }
}

/*****************************************************************************/

#if defined APACHE || defined RENDERD
static key_t createSemaphoreKey(const char* cachePath) {
  const char* c = cachePath;
  long sum = 0;

  while(*c) {
    sum += *c++;
  }

  return sum;
}
#endif

/*****************************************************************************/

static void store_s3_tile_stat_with_cache(struct storage_backend *store, const char *xmlconfig, const char *options, int x, int y, int z, struct stat_info *tile_stat) {

    char cachePath[PATH_MAX];
    struct store_s3_ctx *ctx = (struct store_s3_ctx*) store->storage_ctx;
    struct stat st_stat;

    /* get metatile file stat from cache */

    xyzo_to_meta(cachePath, PATH_MAX, ctx->s3Cache.path, xmlconfig, options, x, y, z);

    if(! stat(cachePath, &st_stat)) {
        tile_stat->size = st_stat.st_size;
        tile_stat->mtime = st_stat.st_mtime;
        tile_stat->atime = st_stat.st_atime;
        tile_stat->ctime = st_stat.st_ctime;

        log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: #1 successfully read properties of metatile from cache %s", cachePath);

        return;
    }

    #ifdef APACHE

    /* create samaphore to get matatile from S3 only be one apache process */
    /* TODO: remove semaphore if renderd is off and a metatile is needed by one process only, proposals are as follows: 
       1. don't create semaphore if renderd is off,
       2. during startup of renderd all semaphores owned by renderd are removed,
       3. others?  */

    key_t key = createSemaphoreKey(cachePath);
    long semId = semget(key, 1, IPC_CREAT | IPC_EXCL | S_IRUSR | S_IWUSR);
    if(semId == -1) {
        log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: failed to create semaphore for key=%d: %s", key, strerror(errno));
        semId = semget(key, 1, S_IRUSR);
        if(semId == -1) {
          log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: failed to get semaphore for key=%d: %s", key, strerror(errno));
          return;
        }
    } else {
        log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: semaphore id=%d created for key=%d", semId, key);
        if(semctl(semId, 0, SETVAL, 1) == -1) {
            log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: failed to set initial value of semaphore id=%d: %s", semId, strerror(errno));
            return;
        }
    }

    struct sembuf sops;
    sops.sem_num = 0;
    sops.sem_op = -1;
    sops.sem_flg = SEM_UNDO;

    struct timespec tp;
    tp.tv_sec = TIMEOUT / 1000;
    tp.tv_nsec = 0;

    int r = semtimedop(semId, &sops, 1, &tp);
    if(r == -1) {
        log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: failed to up semaphore id=%d: %s", semId, strerror(errno));
        if(errno == EIDRM) {
            /* metatile should be in cache now */
            if(! stat(cachePath, &st_stat)) {
                tile_stat->size = st_stat.st_size;
                tile_stat->mtime = st_stat.st_mtime;
                tile_stat->atime = st_stat.st_atime;
                tile_stat->ctime = st_stat.st_ctime;
                log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: #2 successfully read properties of metatile from cache %s", cachePath);
            }
        } else {
          if (errno == EAGAIN) {
              log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: timeout for semaphore id=%d: %s", semId, strerror(errno));
          }

          if(semctl(semId, 0, IPC_RMID) == -1) {
              log_message(STORE_LOGLVL_ERR, "store_s3_tile_stat: #1 failed to remove semaphore id=%d: %s", semId, strerror(errno));
          } else {
             log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: semaphore id=%d removed", semId);
          }
        }
        return;
    }

    #endif

    /* get metatile file from S3 */

    char s3Path[PATH_MAX];
    store_s3_xyz_to_storagekey(store, xmlconfig, options, x, y, z, s3Path, PATH_MAX);
    struct s3_tile_request request = store_s3_get_tile_from_s3(ctx->ctx, s3Path, "store_s3_tile_stat");
    if(request.tile == NULL) {

        #ifdef APACHE

        /* metatile doesn't exists on S3, need to render it, 
           grant permission for renderd to remove the semaphore after renderd wrote a metafile to S3 cache. */

        struct semid_ds s_ds;

        if(semctl(semId, 0,  IPC_STAT, &s_ds) == -1) {
            log_message(STORE_LOGLVL_ERR, "store_s3_tile_stat: failed to get permissions of semaphore id=%d: %s", semId, strerror(errno));
            return;
        }

        /* TODO: need to get a UID of RENDERD process without hard coding, proposals are as follows:
           1. get from UNIX socket,
           2. pass as a configuration parameter,
           3. find renderd process,
           4. others? */
        #define RENDERD_USER_NAME "osm"

        uid_t old_uid = s_ds.sem_perm.uid;
        struct passwd* pwd;
        pwd = getpwnam(RENDERD_USER_NAME);
        s_ds.sem_perm.uid = pwd->pw_uid;

        if(semctl(semId, 0,  IPC_SET, &s_ds) == -1) {
            log_message(STORE_LOGLVL_ERR, "store_s3_tile_stat: failed to set permissions of semaphore id=%d: %s", semId, strerror(errno));
            return;
        } else {
            log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: changed owner of semaphore id=%d: from uid=%d to uid=%d", semId, old_uid, s_ds.sem_perm.uid);
        }

        #endif

        return;
    }

    /* get metatile file stat from S3 */

    tile_stat->size = request.tile_size;
    tile_stat->expired = request.tile_expired;
    tile_stat->mtime = request.tile_mod_time;

    /* save metatile file to cache */

    store_s3_save_tile_to_cache(request.tile, request.tile_size, cachePath, "store_s3_tile_stat");

    /* TODO: set metatile stat in cache according to S3 if needed */

    free(request.tile);


    #ifdef APACHE

    /* remove semaphore to inform other apache processes that metatile is in cache now */

    if(semctl(semId, 0, IPC_RMID) == -1) {
        log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: #2 failed to remove semaphore id=%d: %s", semId, strerror(errno));
        return;
    } else {
        log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: semaphore id=%d removed", semId);
    }

    #endif
}

/*****************************************************************************/

static void store_s3_tile_stat_without_cache(struct storage_backend *store, const char *xmlconfig, const char *options, int x, int y, int z, struct stat_info *tile_stat) {

    char path[PATH_MAX];
    struct store_s3_ctx *ctx = (struct store_s3_ctx*) store->storage_ctx;

    store_s3_xyz_to_storagekey(store, xmlconfig, options, x, y, z, path, PATH_MAX);
    //log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: getting properties for object %s", path);

    struct S3ResponseHandler responseHandler;
    responseHandler.propertiesCallback = &store_s3_properties_callback;
    responseHandler.completeCallback = &store_s3_complete_callback;

    struct s3_tile_request request;
    request.path = path;
    request.error_details = NULL;
    request.cur_offset = 0;
    request.result = S3StatusOK;
    request.tile = NULL;
    request.tile_expired = 0;
    request.tile_mod_time = 0;
    request.tile_size = 0;

    S3_head_object(ctx->ctx, path, NULL, TIMEOUT, &responseHandler, &request);

    if (request.result != S3StatusOK) {
        if (request.result == S3StatusHttpErrorNotFound) {
            // tile does not exist
            //log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: tile not found in storage");
        } else {
            const char *msg = "";
            if (request.error_details && request.error_details->message) {
                msg = request.error_details->message;
            }
            log_message(STORE_LOGLVL_ERR, "store_s3_tile_stat: failed to retrieve object properties for %s: %d (%s) %s", path, request.result, S3_get_status_name(request.result), msg);
        }
        return;
    }

    log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: successfully read properties of %s", path);

    tile_stat->size = request.tile_size;
    tile_stat->expired = request.tile_expired;
    tile_stat->mtime = request.tile_mod_time;
}

/*****************************************************************************/

static struct stat_info store_s3_tile_stat(struct storage_backend *store, const char *xmlconfig, const char *options, int x, int y, int z)
{
    struct stat_info tile_stat;
    tile_stat.size = -1;
    tile_stat.expired = 0;
    tile_stat.mtime = 0;
    tile_stat.atime = 0;
    tile_stat.ctime = 0;

    const char* cachePath = ((struct store_s3_ctx*)store->storage_ctx)->s3Cache.path;

    if(strlen(cachePath) > 0 ) {
        store_s3_tile_stat_with_cache(store, xmlconfig, options, x, y, z, &tile_stat);
    } else {
        store_s3_tile_stat_without_cache(store, xmlconfig, options, x, y, z, &tile_stat);
    }

    return tile_stat;
}

/*****************************************************************************/

static char* store_s3_tile_storage_id(struct storage_backend *store, const char *xmlconfig, const char *options, int x, int y, int z, char *string)
{
    // FIXME: assumes PATH_MAX for length of provided string
    store_s3_xyz_to_storagekey(store, xmlconfig, options, x, y, z, string, PATH_MAX);
    return string;
}

/*****************************************************************************/

static int store_s3_metatile_write_with_cache(struct storage_backend *store, const char *xmlconfig, const char *options, int x, int y, int z, const char *buf, int sz)
{
    struct store_s3_ctx *ctx = (struct store_s3_ctx*) store->storage_ctx;
    char *path = malloc(PATH_MAX);
    char cachePath[PATH_MAX];

    /* save metatile file to S3 cache */

    xyzo_to_meta(cachePath, PATH_MAX, ctx->s3Cache.path, xmlconfig, options, x, y, z);

    store_s3_save_tile_to_cache(buf, sz, cachePath, "store_s3_metatile_write");

    /* remove semaphore to inform apache processes that metatile is in S3 cache now */
    /* TODO: determine an impact on other tools like: render_list, render_expired, etc. */

    #ifdef RENDERD

    key_t key = createSemaphoreKey(cachePath);
    long semId = semget(key, 1, S_IRUSR);
    if(semId == -1) {
        log_message(STORE_LOGLVL_ERR, "store_s3_metatile_write: failed to get semaphore for key=%d: %s", key, strerror(errno));
    } else {
        if(semctl(semId, 0, IPC_RMID) == -1) {
            log_message(STORE_LOGLVL_ERR, "store_s3_metatile_write: failed to remove semaphore id=%d: %s", semId, strerror(errno));
        } else {
            log_message(STORE_LOGLVL_DEBUG, "store_s3_metatile_write: semaphore id=%d removed", semId);
        }
    }

    #endif

    /* save metatile file to S3 */

    store_s3_xyz_to_storagekey(store, xmlconfig, options, x, y, z, path, PATH_MAX);
    log_message(STORE_LOGLVL_DEBUG, "store_s3_metatile_write: storing object %s, size %ld", path, sz);

    struct S3PutObjectHandler putObjectHandler;
    putObjectHandler.responseHandler.propertiesCallback = &store_s3_properties_callback;
    putObjectHandler.responseHandler.completeCallback = &store_s3_complete_callback;
    putObjectHandler.putObjectDataCallback = &store_s3_put_object_data_callback;

    struct s3_tile_request request;
    request.path = path;
    request.tile = (char*) buf;
    request.tile_size = sz;
    request.cur_offset = 0;
    request.tile_expired = 0;
    request.result = S3StatusOK;
    request.error_details = NULL;

    S3PutProperties props;
    props.contentType = "application/octet-stream";
    props.cacheControl = NULL;
    props.cannedAcl = S3CannedAclPrivate; // results in no ACL header in POST
    props.contentDispositionFilename = NULL;
    props.contentEncoding = NULL;
    props.expires = -1;
    props.md5 = NULL;
    props.metaData = NULL;
    props.metaDataCount = 0;
    props.useServerSideEncryption = 0;

    S3_put_object(ctx->ctx, path, sz, &props, NULL, TIMEOUT, &putObjectHandler, &request);
    free(path);

    if (request.result != S3StatusOK) {
        const char *msg = "";
        const char *msg2 = "";
        if (request.error_details) {
            if (request.error_details->message) {
                msg = request.error_details->message;
            }
            if (request.error_details->furtherDetails) {
                msg2 = request.error_details->furtherDetails;
            }
        }
        log_message(STORE_LOGLVL_ERR, "store_s3_metatile_write: failed to write object: %d(%s)/%s%s", request.result, S3_get_status_name(request.result), msg, msg2);
        return -1;
    }

    log_message(STORE_LOGLVL_DEBUG, "store_s3_metatile_write: Wrote object of size %i", sz);

    return sz;
}

/*****************************************************************************/

static int store_s3_metatile_write_without_cache(struct storage_backend *store, const char *xmlconfig, const char *options, int x, int y, int z, const char *buf, int sz)
{
    struct store_s3_ctx *ctx = (struct store_s3_ctx*) store->storage_ctx;
    char *path = malloc(PATH_MAX);
    store_s3_xyz_to_storagekey(store, xmlconfig, options, x, y, z, path, PATH_MAX);
    log_message(STORE_LOGLVL_DEBUG, "store_s3_metatile_write: storing object %s, size %ld", path, sz);

    struct S3PutObjectHandler putObjectHandler;
    putObjectHandler.responseHandler.propertiesCallback = &store_s3_properties_callback;
    putObjectHandler.responseHandler.completeCallback = &store_s3_complete_callback;
    putObjectHandler.putObjectDataCallback = &store_s3_put_object_data_callback;

    struct s3_tile_request request;
    request.path = path;
    request.tile = (char*) buf;
    request.tile_size = sz;
    request.cur_offset = 0;
    request.tile_expired = 0;
    request.result = S3StatusOK;
    request.error_details = NULL;

    S3PutProperties props;
    props.contentType = "application/octet-stream";
    props.cacheControl = NULL;
    props.cannedAcl = S3CannedAclPrivate; // results in no ACL header in POST
    props.contentDispositionFilename = NULL;
    props.contentEncoding = NULL;
    props.expires = -1;
    props.md5 = NULL;
    props.metaData = NULL;
    props.metaDataCount = 0;
    props.useServerSideEncryption = 0;

    S3_put_object(ctx->ctx, path, sz, &props, NULL, TIMEOUT, &putObjectHandler, &request);
    free(path);

    if (request.result != S3StatusOK) {
        const char *msg = "";
        const char *msg2 = "";
        if (request.error_details) {
            if (request.error_details->message) {
                msg = request.error_details->message;
            }
            if (request.error_details->furtherDetails) {
                msg2 = request.error_details->furtherDetails;
            }
        }
        log_message(STORE_LOGLVL_ERR, "store_s3_metatile_write: failed to write object: %d(%s)/%s%s", request.result, S3_get_status_name(request.result), msg, msg2);
        return -1;
    }

    log_message(STORE_LOGLVL_DEBUG, "store_s3_metatile_write: Wrote object of size %i", sz);

    return sz;
}


/*****************************************************************************/

static int store_s3_metatile_write(struct storage_backend *store, const char *xmlconfig, const char *options, int x, int y, int z, const char *buf, int sz)
{
    const char* cachePath = ((struct store_s3_ctx*)store->storage_ctx)->s3Cache.path;

    if(strlen(cachePath) > 0 ) {
        return store_s3_metatile_write_with_cache(store, xmlconfig, options, x, y, z, buf, sz);
    } else {
        return store_s3_metatile_write_without_cache(store, xmlconfig, options, x, y, z, buf, sz);
    }
}

/*****************************************************************************/

static int store_s3_metatile_delete(struct storage_backend *store, const char *xmlconfig, int x, int y, int z)
{
    struct store_s3_ctx *ctx = (struct store_s3_ctx*) store->storage_ctx;
    char *path = malloc(PATH_MAX);
    store_s3_xyz_to_storagekey(store, xmlconfig, NULL, x, y, z, path, PATH_MAX);
    log_message(STORE_LOGLVL_DEBUG, "store_s3_metatile_delete: deleting object %s", path);

    struct S3ResponseHandler responseHandler;
    responseHandler.propertiesCallback = &store_s3_properties_callback;
    responseHandler.completeCallback = &store_s3_complete_callback;

    struct s3_tile_request request;
    request.path = path;
    request.error_details = NULL;
    request.cur_offset = 0;
    request.result = S3StatusOK;
    request.tile = NULL;
    request.tile_expired = 0;
    request.tile_mod_time = 0;
    request.tile_size = 0;

    S3_delete_object(ctx->ctx, path, NULL, TIMEOUT, &responseHandler, &request);
    free(path);

    if (request.result != S3StatusOK) {
        const char *msg = "";
        if (request.error_details && request.error_details->message) {
            msg = request.error_details->message;
        }
        log_message(STORE_LOGLVL_ERR, "store_s3_metatile_delete: failed to delete object: %d(%s)/%s", request.result, S3_get_status_name(request.result), msg);
        return -1;
    }

    log_message(STORE_LOGLVL_DEBUG, "store_s3_metatile_delete: deleted object");

    return 0;
}

/*****************************************************************************/

static int store_s3_metatile_expire(struct storage_backend *store, const char *xmlconfig, int x, int y, int z)
{
    struct store_s3_ctx *ctx = (struct store_s3_ctx*) store->storage_ctx;
    char *path = malloc(PATH_MAX);
    store_s3_xyz_to_storagekey(store, xmlconfig, NULL, x, y, z, path, PATH_MAX);
    log_message(STORE_LOGLVL_DEBUG, "store_s3_metatile_expire: expiring object %s", path);

    struct S3ResponseHandler responseHandler;
    responseHandler.propertiesCallback = &store_s3_properties_callback;
    responseHandler.completeCallback = &store_s3_complete_callback;

    struct s3_tile_request request;
    request.path = path;
    request.error_details = NULL;
    request.cur_offset = 0;
    request.result = S3StatusOK;
    request.tile = NULL;
    request.tile_expired = 0;
    request.tile_mod_time = 0;
    request.tile_size = 0;

    struct S3NameValue expireTag;
    expireTag.name = "expired";
    expireTag.value = "1";

    S3PutProperties props;
    props.contentType = "application/octet-stream";
    props.cacheControl = NULL;
    props.cannedAcl = S3CannedAclPrivate; // results in no ACL header in POST
    props.contentDispositionFilename = NULL;
    props.contentEncoding = NULL;
    props.expires = -1;
    props.md5 = NULL;
    props.metaDataCount = 1;
    props.metaData = &expireTag;
    props.useServerSideEncryption = 0;

    int64_t lastModified;

    S3_copy_object(ctx->ctx, path, ctx->ctx->bucketName, path, &props, &lastModified, 0, NULL, NULL, TIMEOUT, &responseHandler, &request);
    free(path);

    if (request.result != S3StatusOK) {
        const char *msg = "";
        if (request.error_details && request.error_details->message) {
            msg = request.error_details->message;
        }
        log_message(STORE_LOGLVL_ERR, "store_s3_metatile_expire: failed to update object: %d (%s)/%s", request.result, S3_get_status_name(request.result), msg);
        return -1;
    }

    log_message(STORE_LOGLVL_DEBUG, "store_s3_metatile_expire: updated object metadata");

    return 0;
}

/*****************************************************************************/

static int store_s3_close_storage(struct storage_backend *store)
{
    struct store_s3_ctx *ctx = (struct store_s3_ctx*) store->storage_ctx;

    S3_deinitialize();
    if (NULL != ctx->urlcopy) {
        free(ctx->urlcopy);
        ctx->urlcopy = NULL;
    }
    free(ctx);
    store->storage_ctx = NULL;
    store_s3_initialized = 0;

    return 0;
}

/*****************************************************************************/

static char* url_decode(const char *src)
{
    if (NULL == src) {
        return NULL;
    }
    char *dst = (char*) malloc(strlen(src) + 1);
    dst[0] = '\0';
    while (*src) {
        int c = *src;
        if (c == '%' && isxdigit(*(src + 1)) && isxdigit(*(src + 2))) {
            char hexdigit[] =
            { *(src + 1), *(src + 2), '\0' };
            char decodedchar[2];
            sprintf(decodedchar, "%c", (char) strtol(hexdigit, NULL, 16));
            strncat(dst, decodedchar, 1);
            src += 2;
        } else {
            strncat(dst, src, 1);
        }
        src++;
    }
    return dst;
}

/*****************************************************************************/

static const char* env_expand(const char *src)
{
    if (strstr(src, "${") == src && strrchr(src, '}') == (src + strlen(src) - 1)) {
        char tmp[strlen(src) + 1];
        strcpy(tmp, src);
        tmp[strlen(tmp) - 1] = '\0';
        char *val = getenv(tmp + 2);
        if (NULL == val) {
            log_message(STORE_LOGLVL_ERR, "init_storage_s3: environment variable %s not defined when initializing S3 configuration!", tmp + 2);
            return NULL;
        }
        return val;
    }
    return src;
}

#endif //Have libs3

/*****************************************************************************/

struct storage_backend* init_storage_s3(const char *connection_string)
{
#ifndef HAVE_LIBS3
    log_message(STORE_LOGLVL_ERR,
            "init_storage_s3: Support for libs3 and therefore S3 storage has not been compiled into this program");
    return NULL;
#else
    const char* regex = "^s3://([A-Z0-9]{20}|\\$\\{[a-zA-Z_][a-zA-Z_0-9]*\\}):"                              /* accees key id */
                        "([A-Za-z0-9/+=]{40}|\\$\\{[a-zA-Z_][a-zA-Z_0-9]*\\})@?"                             /* security access key */
                        "(s3-[a-z]{2}-[a-z]{4,9}-[1-9]{1}.amazonaws.com|\\$\\{[a-zA-Z_][a-zA-Z_0-9]*\\})?/"  /* host name */
                        "([a-zA-Z0-9-]{3,63}|\\$\\{[a-zA-Z_][a-zA-Z_0-9]*\\})@?"                             /* bucket name */
                        "([a-z]{2}-[a-z]{4,9}-[1-9]{1}|\\$\\{[a-zA-Z_][a-zA-Z_0-9]*\\})?/?"                  /* auth region */
                        "([0-9a-zA-Z/!\\-_.*'()]{1,1024}|\\$\\{[a-zA-Z_][a-zA-Z_0-9]*\\})?@?"                /* base path */
                        "c?a?c?h?e?:?(/[^\\0:]*)?:?([1-9][0-9]*[KMGT]B)?$";                                  /* cache */
    regex_t preg;
    regmatch_t pmatch[9];

    int errcode = regcomp(&preg, regex, REG_EXTENDED);
    if(errcode != 0) {
        const size_t errbuf_size = 100;
        char errbuf[errbuf_size];
        regerror(errcode, &preg, errbuf, errbuf_size);
        log_message(STORE_LOGLVL_ERR, "init_storage_s3: regular expression pattern for connection string invalid: error code: %d %s", errcode, errbuf);
        return NULL;
    }

    if(regexec(&preg, connection_string, 9, pmatch, 0) == REG_NOMATCH) {
        const char* connection_string_usage = "s3://<access key id>:<secret access key>[@<hostname>]/<bucket>[@region][/<basepath>][@cache:<dir>:<size>]";
        log_message(STORE_LOGLVL_ERR, "init_storage_s3: connection string invalid for S3 storage!\nUsage: %s",connection_string_usage);
        regfree(&preg);
        return NULL;
    }

    struct storage_backend *store = malloc(sizeof(struct storage_backend));
    struct store_s3_ctx *ctx = malloc(sizeof(struct store_s3_ctx));

    S3Status res = S3StatusErrorUnknown;

    if (!store || !ctx) {
        log_message(STORE_LOGLVL_ERR, "init_storage_s3: failed to allocate memory for context");
        if (store)
            free(store);
        if (ctx)
            free(ctx);
        return NULL;
    }

    pthread_mutex_lock(&qLock);
    if (!store_s3_initialized) {
        log_message(STORE_LOGLVL_DEBUG, "init_storage_s3: global init of libs3");
        res = S3_initialize(NULL, S3_INIT_ALL, NULL);
        store_s3_initialized = 1;
    } else {
        res = S3StatusOK;
    }

    pthread_mutex_unlock(&qLock);
    if (res != S3StatusOK) {
        log_message(STORE_LOGLVL_ERR, "init_storage_s3: failed to initialize S3 library: %s", S3_get_status_name(res));
        free(ctx);
        free(store);
        return NULL;
    }

    struct S3BucketContext *bctx = ctx->ctx = malloc(sizeof(struct S3BucketContext));

    ctx->urlcopy = strdup(connection_string);
    if (NULL == ctx->urlcopy) {
        log_message(STORE_LOGLVL_ERR, "init_storage_s3: error allocating memory for connection string!");
        free(ctx);
        free(store);
        return NULL;
    }

    for(int i=1; i < 8; i++) {
        *(ctx->urlcopy + pmatch[i].rm_eo) = 0;
    }

    bctx->accessKeyId = ctx->urlcopy + pmatch[1].rm_so;
    bctx->secretAccessKey = ctx->urlcopy + pmatch[2].rm_so;
    bctx->hostName = ctx->urlcopy + pmatch[3].rm_so;
    bctx->bucketName = ctx->urlcopy + pmatch[4].rm_so;
    bctx->authRegion = ctx->urlcopy + pmatch[5].rm_so;
    ctx->basepath = ctx->urlcopy + pmatch[6].rm_so;
    ctx->s3Cache.path = ctx->urlcopy + pmatch[7].rm_so;
    ctx->s3Cache.sizeUnit = ctx->urlcopy + pmatch[8].rm_so;

    regfree(&preg);

    /* set null if empty to ignore them by libs3, otherwise the following errors occur:
        * 43 (NameLookupError)
        * 128(ErrorUnknown)/The authorization header is malformed; a non-empty region must be provided in the credential.
       TODO: it should be fixed in libs3
    */

    if (bctx->hostName != NULL && strlen(bctx->hostName) <= 0) {
        bctx->hostName = NULL;
    }

    if (bctx->authRegion != NULL && strlen(bctx->authRegion) <= 0) {
        bctx->authRegion = NULL;
    }

    /* get connection string parameters from environment variables */

    /* access key id */
    bctx->accessKeyId = env_expand(bctx->accessKeyId);
    if (bctx->accessKeyId == NULL) {
        free(ctx);
        free(store);
        return NULL;
    }
    bctx->accessKeyId = url_decode(bctx->accessKeyId);

    /* secret access key */
    bctx->secretAccessKey = env_expand(bctx->secretAccessKey);
    if (bctx->secretAccessKey == NULL) {
        free(ctx);
        free(store);
        return NULL;
    }
    bctx->secretAccessKey = url_decode(bctx->secretAccessKey);

    /* host name */
    if (bctx->hostName) {
        bctx->hostName = env_expand(bctx->hostName);
        if (bctx->hostName == NULL) {
            free(ctx);
            free(store);
            return NULL;
        }
        bctx->hostName = url_decode(bctx->hostName);
    }

    /* bucket name */
    bctx->bucketName = env_expand(bctx->bucketName);
    if (bctx->bucketName == NULL) {
        free(ctx);
        free(store);
        return NULL;
    }
    bctx->bucketName = url_decode(bctx->bucketName);

    /* auth region */
    if(bctx->authRegion) {
        bctx->authRegion = env_expand(bctx->authRegion);
        if (bctx->authRegion == NULL) {
          free(ctx);
          free(store);
          return NULL;
        }
        bctx->authRegion = url_decode(bctx->authRegion);
    }

    /* base path */
    if(ctx->basepath) {
        ctx->basepath = env_expand(ctx->basepath);
        if (ctx->basepath == NULL) {
          free(ctx);
          free(store);
          return NULL;
        }
        ctx->basepath = url_decode(ctx->basepath);
    }

    /* validation */

    if(strlen(ctx->s3Cache.path) > 0) {
        /* check if cache directory exists and it is writeable */
        if(access(ctx->s3Cache.path, W_OK) == -1) {
            log_message(STORE_LOGLVL_ERR, "init_storage_s3: cache directory path %s is inncorect: %s", ctx->s3Cache.path, strerror(errno));
            free(ctx);
            free(store);
            return NULL;
        }


        /* set default cache size if it is not provided in connection string */
        if(strlen(ctx->s3Cache.sizeUnit) <= 0) {
            ctx->s3Cache.sizeUnit = DEFAULT_CACHE_SIZE;
        }

        #if defined RENDERD && defined HAVE_LIBDSAA
        pthread_mutex_lock(&cache_cleaner_lock);
        if (cache_cleaner_thread == 0) {
            if(pthread_create(&cache_cleaner_thread, NULL, exec_cache_cleaner_thread, &ctx->s3Cache) == 0) {
                log_message(STORE_LOGLVL_INFO, "init_storage_s3: Started S3 cache cleaner thread id=%lu", cache_cleaner_thread);
            } else {
                log_message(STORE_LOGLVL_ERR, "init_storage_s3: Failed to create S3 cache cleaner thread: %s", strerror(errno));
            }
        }
        pthread_mutex_unlock(&cache_cleaner_lock);
        #endif
    }

    log_message(STORE_LOGLVL_DEBUG, "init_storage_s3 completed keyid: %s, key: %s, host: %s, region: %s, bucket: %s, basepath: %s, cachepath: %s, cachesize=%s", ctx->ctx->accessKeyId, ctx->ctx->secretAccessKey, ctx->ctx->hostName, ctx->ctx->authRegion, ctx->ctx->bucketName, ctx->basepath, ctx->s3Cache.path, ctx->s3Cache.sizeUnit);

    bctx->protocol = S3ProtocolHTTPS;
    bctx->securityToken = NULL;
    bctx->uriStyle = S3UriStyleVirtualHost;

    store->storage_ctx = ctx;

    store->tile_read = &store_s3_tile_read;
    store->tile_stat = &store_s3_tile_stat;
    store->metatile_write = &store_s3_metatile_write;
    store->metatile_delete = &store_s3_metatile_delete;
    store->metatile_expire = &store_s3_metatile_expire;
    store->tile_storage_id = &store_s3_tile_storage_id;
    store->close_storage = &store_s3_close_storage;

    return store;
#endif
}

/*****************************************************************************/



