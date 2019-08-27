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
#include <poll.h>
#include <unistd.h>

#ifdef HAVE_LIBS3
#include <libs3.h>
#endif

#ifdef HAVE_LIBDSAA
#include <libdsaa.h>
#include <sys/inotify.h>
#define INOTIFY_BUF_LEN sizeof(struct inotify_event) + NAME_MAX + 1
#endif

#include "store.h"
#include "store_file_utils.h"
#include "store_s3.h"
#include "metatile.h"
#include "render_config.h"
#include "protocol.h"
#include "log_msg.h"

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
    int inotify_d;
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
#ifdef RENDERD

static pthread_mutex_t cache_cleaner_lock;
static pthread_t cache_cleaner_thread = 0;

typedef enum {SORT, LAST} list_add_mode;

struct list_data {
    char path[PATH_MAX];
    time_t atime;
    off_t size;
    int watch_d;
    struct list_data *parent;
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
    log_message(LOG_DEBUG, "%d path=%s watch_d=%d parent_watch_d=%d size=%ld atime=%ld", i, d->path, d->watch_d, d->parent != NULL ? d->parent->watch_d : 0, d->size, d->atime);
}

/*****************************************************************************/

int s3_cache_list_find_item(void* v1, void* v2) {
    struct list_data *d = (struct list_data*)v1;
	int watch_d = *(int*)v2;
    return d->watch_d == watch_d ? 1 : 0;
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

int s3_cache_add_dir(char* path, S3Cache *s3Cache, struct list_data **ld, struct list_data *ldp) {
    struct stat fs;
    int watch_d;

    if(stat(path, &fs) != 0) {
        log_message(STORE_LOGLVL_ERR, "s3_cache_add_dir: error to stat dir: %s %d %s", path, errno, strerror(errno));
        return -1;
    }

	if((watch_d = inotify_add_watch(s3Cache->inotify_d, path, IN_CREATE | IN_DELETE_SELF )) == -1) {
		log_message(LOG_ERR, "s3_cache_add_dir: inotify_add_watch(%s) error(%d) %s", path, errno, strerror(errno) );
	    return -1;
	}

	if(list_find(&s3Cache->dirList, &watch_d)) {
		log_message(LOG_DEBUG,"s3_cache_add_dir: path=%s watch_d=%d is already on list, skipped", path, watch_d );
		return -1;
	}


    *ld = malloc(sizeof(struct list_data));
    if(*ld == NULL) {
        log_message(LOG_ERR, "s3_cache_add_dir: error to allocate memory: %d %s", errno, strerror(errno));
        return -1;
    }

    strncpy((*ld)->path, path, PATH_MAX);
    (*ld)->size = fs.st_size;
    (*ld)->atime = ((struct timespec)fs.st_atim).tv_sec;
    (*ld)->watch_d = watch_d;
    (*ld)->parent = ldp;

    s3Cache->dirSize +=  fs.st_size;
    s3Cache->dirCount++;

    list_add(&s3Cache->dirList, *ld);

    return 0;
}

/*****************************************************************************/

int s3_cache_add_file(char* path, S3Cache *s3Cache, list_add_mode mode, struct list_data *ldp) {
	struct stat fs;
	int watch_d;
	struct list_data *ld = NULL;

    if(stat(path, &fs) != 0) {
        log_message(LOG_ERR, "s3_cache_add_file: stat(%s) error: %s %d %s", path, errno, strerror(errno));
        return -1;
    }

	if((watch_d = inotify_add_watch(s3Cache->inotify_d, path, IN_ACCESS | IN_CLOSE_WRITE | IN_DELETE_SELF )) == -1) {
		log_message(LOG_ERR,"s3_cache_add_file: inotify_add_watch(%s) error %d %s", path, errno, strerror(errno) );
		return -1;
	}

	if(list_find(&s3Cache->fileList, &watch_d)) {
		log_message(LOG_DEBUG,"s3_cache_add_file: path=%s watch_d=%d is already on list, skipped", path, watch_d );
		return -1;
	}

    ld = malloc(sizeof(struct list_data));
    if(ld == NULL) {
        log_message(LOG_ERR, "s3_cache_add_file: error to allocate memory: %d %s", errno, strerror(errno));
        return -1;
    }

    strncpy(ld->path, path, PATH_MAX);
    ld->atime = ((struct timespec)fs.st_atim).tv_sec;
    ld->size = fs.st_size;
	ld->watch_d = watch_d;
	ld->parent = ldp;

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

int s3_cache_read_dir(char* name, S3Cache *s3Cache, struct list_data *pld) {
    DIR *dir;
    struct dirent *file;
    char path[PATH_MAX];
    int r = 0;
    struct list_data *ld;

    r = s3_cache_add_dir(name, s3Cache, &ld, pld);

    if((dir = opendir(name)) == NULL) {
        log_message(LOG_ERR, "tile_cache_read_dir: opendir(%s) error(%d) %s", name, errno, strerror(errno));
        return -1;
    }


    while ((file = readdir(dir)) != NULL) {
        if (!strcmp(file->d_name, ".") || !strcmp(file->d_name, "..") || !strcmp(file->d_name, "lost+found")) {
            continue;
        }

        snprintf(path, PATH_MAX, "%s/%s", name, file->d_name);
        if(file->d_type == DT_REG) {
            r = s3_cache_add_file(path, s3Cache, SORT, ld);
        } else if(file->d_type == DT_DIR) {
            r = s3_cache_read_dir(path, s3Cache, ld);
        }
    }
    closedir(dir);
    return r;
}

/*****************************************************************************/

void s3_cache_print(S3Cache* s3Cache) {

	log_message(STORE_LOGLVL_DEBUG,"-");
    list_print(&s3Cache->fileList, list_item_first);
    log_message(STORE_LOGLVL_DEBUG, "S3 cache file size: %ld", s3Cache->fileSize);
    log_message(STORE_LOGLVL_DEBUG, "S3 cache file count: %d", s3Cache->fileCount);

    list_print(&s3Cache->dirList, list_item_first);
    log_message(STORE_LOGLVL_DEBUG, "S3 cache dir size: %ld", s3Cache->dirSize);
    log_message(STORE_LOGLVL_DEBUG, "S3 cache dir count: %d", s3Cache->dirCount);

    log_message(STORE_LOGLVL_DEBUG, "S3 cache size: %ld, S3 cache size limit: %ld", s3Cache->fileSize + s3Cache->dirSize, s3Cache->size);
    log_message(STORE_LOGLVL_DEBUG,"-");
}

/*****************************************************************************/

int s3_cache_update_file_size(S3Cache *s3Cache, struct list_data *ld) {
	struct stat fs;

	if(stat(ld->path, &fs) != 0) {
        log_message(LOG_ERR, "s3_cache_update_file_size: stat(%s) error(%d) %s", ld->path, errno, strerror(errno));
		return -1;
	}

	if(ld->size != fs.st_size) {
		s3Cache->fileSize -= ld->size;
		s3Cache->fileSize += fs.st_size;
		ld->size = fs.st_size;
		log_message(LOG_DEBUG, "s3_cache_update_file_size: file(%s) size(%d) updated.", ld->path, ld->size);
	}

	return 0;
}

/*****************************************************************************/

int s3_cache_update_file_atime(S3Cache *s3Cache, struct list_data *ld) {
	struct stat fs;
	list_item_position p = list_item_last;

	if(stat(ld->path, &fs) != 0) {
        log_message(LOG_ERR, "s3_cache_update_file_atime: stat(%s) error(%d) %s", ld->path, errno, strerror(errno));
		return -1;
	}

	time_t atime = ((struct timespec)fs.st_atim).tv_sec;
	if(ld->atime < atime) {
	    list_move(&s3Cache->fileList, &ld->watch_d, p, &atime, NULL);
	    log_message(LOG_DEBUG, "s3_cache_update_file_atime: file(%s) access time(%d) updated.", ld->path, ld->atime);
	}

	return 0;
}

/*****************************************************************************/

int s3_cache_remove_file(S3Cache *s3Cache, struct list_data *ld) {

    s3Cache->fileSize -= ld->size;
    s3Cache->fileCount--;

    log_message(LOG_DEBUG, "Deleted file from S3 cache: %s", ld->path);

    list_remove_find(&s3Cache->fileList, &ld->watch_d);

    return 0;
}

/*****************************************************************************/

int s3_cache_remove_dir(S3Cache *s3Cache, struct list_data *ld) {

    s3Cache->dirSize -= ld->size;
    s3Cache->dirCount--;

    log_message(STORE_LOGLVL_DEBUG, "Deleted dir: %s", ld->path);

    list_remove_find(&s3Cache->dirList, &ld->watch_d);

    return 0;
}

/*****************************************************************************/

int s3_cache_remove_empty_dir(S3Cache *s3Cache, struct list_data *ld) {
    DIR *dir;
    struct dirent *file;
    int i = 0;

    if(! strcmp(ld->path, s3Cache->path)) {
        return 0;
    }

    if((dir = opendir(ld->path)) == NULL) {
        log_message(LOG_ERR, "s3_cache_remove_dir: opendir(%s) error(%d) %s", ld->path, errno, strerror(errno));
        return -1;
    }

    while ((file = readdir(dir)) != NULL) {
        if ( ! (! strcmp(file->d_name, ".") || ! strcmp(file->d_name, "..")) ) {
            i++;
        }
    }
    closedir(dir);

    log_message(LOG_DEBUG, "Files %d in dir %s", i, ld->path);

    if(i > 0) {
        return 0;
    }

    if(inotify_rm_watch(s3Cache->inotify_d, ld->watch_d) == -1) {
    	log_message(LOG_ERR, "s3_cache_reduce_size: inotify_rm_watch(%d) error(%d) %s", ld->watch_d, errno, strerror(errno));
    	return -1;
    }

    if(rmdir(ld->path) == -1) {
        log_message(STORE_LOGLVL_ERR, "s3_cache_remove_dir: rmdir(%s) error(%d) %s", ld->path, errno, strerror(errno));
        return -1;
    }

    s3_cache_remove_dir(s3Cache, ld);

    s3_cache_remove_empty_dir(s3Cache, ld->parent);

    return 1;
}

/*****************************************************************************/

int s3_cache_reduce_size(S3Cache *s3Cache) {
    struct list_data *ld;

    if(s3Cache->fileSize + s3Cache->dirSize <= s3Cache->size) {
        return 0;
    }

    log_message(LOG_INFO, "Reducing tile cache size is needed: %ld > %ld", s3Cache->fileSize + s3Cache->dirSize,  s3Cache->size);

    while(s3Cache->fileSize + s3Cache->dirSize > s3Cache->size) {

    	list_get(&s3Cache->fileList, list_item_first, (void**)&ld);

        if(inotify_rm_watch(s3Cache->inotify_d, ld->watch_d) == -1) {
        	log_message(LOG_ERR, "s3_cache_reduce_size: inotify_rm_watch(%d) error(%d) %s", ld->watch_d, errno, strerror(errno));
        	return -1;
        }

    	if(unlink(ld->path) == -1) {
            log_message(LOG_ERR, "s3_cache_reduce_size: unlink(%s) error(%d) %s", ld->path, errno, strerror(errno));
            return -1;
        }

    	s3_cache_remove_file(s3Cache, ld);
    	s3_cache_remove_empty_dir(s3Cache, ld->parent);

    }

    return 1;
}

/*****************************************************************************/

int s3_cache_process_event(S3Cache *s3Cache) {
	struct inotify_event *ie;
	char path[PATH_MAX];
	struct list_data *ld;
	char buf[INOTIFY_BUF_LEN] __attribute__ ((aligned(8)));
	char *p;
	ssize_t len;

	while(1) {

		if((len = read(s3Cache->inotify_d, buf, INOTIFY_BUF_LEN)) > 0 ) {
			for (p = buf; p < buf + len; p += sizeof(struct inotify_event) + ie->len) {

		        ie = (struct inotify_event *) p;

			    log_message(LOG_DEBUG, "inotify event: mask=%x len=%d name=%s wd=%d\n", ie->mask, ie->len, ie->len > 0 ? ie->name : "", ie->wd);

				if(ie->mask & IN_CREATE) {

					if(list_get_find(&s3Cache->dirList, &ie->wd, (void**)&ld)) {

				    	snprintf(path, PATH_MAX, "%s/%s", ld->path, ie->name);

						log_message(LOG_DEBUG, "IN_CREATE: %s", path);

						if(ie->mask & IN_ISDIR ) {
							s3_cache_read_dir(path, s3Cache, ld);
						} else {
							s3_cache_add_file(path, s3Cache, LAST, ld);
						}

						s3_cache_print(s3Cache);
						if(s3_cache_reduce_size(s3Cache)) {
							s3_cache_print(s3Cache);
						}
					}


				} else if (ie->mask & IN_CLOSE_WRITE) {

					if(list_get_find(&s3Cache->fileList, &ie->wd, (void**)&ld)) {

						log_message(LOG_DEBUG, "IN_CLOSE_WRITE %s", ld->path);
						s3_cache_update_file_size(s3Cache, ld);
						s3_cache_print(s3Cache);

						if(s3_cache_reduce_size(s3Cache)) {
							s3_cache_print(s3Cache);
						}
					 }

				} else if (ie->mask & IN_ACCESS) {

					if(list_get_find(&s3Cache->fileList, &ie->wd, (void**)&ld)) {
						log_message(LOG_DEBUG, "IN_ACCESS %s", ld->path);
		                s3_cache_update_file_atime(s3Cache, ld);
						s3_cache_print(s3Cache);
					}

				} else if (ie->mask & IN_DELETE_SELF) {

					if(list_get_find(&s3Cache->fileList, &ie->wd, (void**)&ld)) {

						log_message(LOG_DEBUG, "IN_DELETE_SELF %s", ld->path);
						s3_cache_remove_file(s3Cache, ld);
						s3_cache_print(s3Cache);

					} else if(list_get_find(&s3Cache->dirList, &ie->wd, (void**)&ld)) {

						log_message(LOG_DEBUG, "IN_DELETE_SELF %s", ld->path);
						s3_cache_remove_dir(s3Cache, ld);
						s3_cache_print(s3Cache);
					}
				}
			}
		}
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

    s3Cache.inotify_d = inotify_init();

    if(s3_cache_read_dir(s3Cache.path, &s3Cache, NULL) == -1) {
        log_message(STORE_LOGLVL_ERR, "exec_cache_cleaner_thread: error reading tile cache directory: %s", s3Cache.path);
    }

    s3_cache_print(&s3Cache);
    if(s3_cache_reduce_size(&s3Cache)) {
    	s3_cache_print(&s3Cache);
    }

    s3_cache_process_event(&s3Cache);

    list_release(&s3Cache.fileList);
    list_release(&s3Cache.dirList);

    close(s3Cache.inotify_d);

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

static int store_s3_save_tile_to_cache(const char* metatile, size_t metatile_size, char* cachePath, const char* source) {

    if (mkdirp(cachePath)) {
         log_message(STORE_LOGLVL_ERR, "%s: error creating S3 cache directory structure for meta tile: %s", source, cachePath);
         return -1;
    }

    int fd = open(cachePath, O_WRONLY | O_TRUNC | O_CREAT, S_IRUSR |  S_IWUSR | S_IRGRP | S_IWGRP |  S_IROTH);
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
    char path[PATH_MAX];

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
        return -1;
    }

    log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_read: retrieved metatile %s of size %i", path, request.tile_size);


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

#if defined APACHE

static short store_s3_socket_closed(int socket) {

	struct pollfd fds;

	fds.fd = socket;
	fds.events = POLLRDHUP;
	int r = poll(&fds, 1, 0);


	if(fds.revents & POLLRDHUP) {
		log_message(STORE_LOGLVL_DEBUG, "store_s3_socket_closed: peer closed connection, POLLRDUP(%x)", r);
		return 1;

	} else if(r == -1) {
		log_message(STORE_LOGLVL_ERR, "store_s3_socket_closed: poll(%d) error, %s", socket, strerror(errno));

	} else {
		log_message(STORE_LOGLVL_DEBUG, "store_s3_socket_closed: opened, poll() returns (%d)", r);
	}

	return 0;
}

#endif

/*****************************************************************************/

#if defined APACHE || defined RENDERD

static void store_s3_pipe_path(char *pipePath, size_t len, char* path) {

	  const char* c = path;
	  long sum = 0;

	  while(*c) {
	    sum += *c++;
	  }

	  snprintf(pipePath, len, "%s/%ld", PIPE_DIR, sum);
}

/*****************************************************************************/

static void store_s3_pipe_remove(char* path) {

	if(unlink(path) == -1) {
		log_message(STORE_LOGLVL_ERR, "store_s3_pipe_remove: unlink(%s) failed, %s", path, strerror(errno));
	} else {
		log_message(STORE_LOGLVL_DEBUG, "store_s3_pipe_remove: unlink(%s) successful", path);
	}
}

/*****************************************************************************/

static void store_s3_pipe_hup(char* path) {

	int fd = open(path, O_WRONLY |  O_NONBLOCK);
	if(fd == -1) {
		log_message(STORE_LOGLVL_ERR, "store_s3_pipe_hup: open(%s) failed, %s", path, strerror(errno));
	} else {
		close(fd);
		log_message(STORE_LOGLVL_DEBUG, "store_s3_pipe_hup: event close(%s) successful", path);
	}
}

/*****************************************************************************/

static void store_s3_tile_cancel(struct storage_backend *store, const char *xmlconfig, const char *options, int x, int y, int z) {

    char cachePath[PATH_MAX];
    char pipePath[PATH_MAX];
    struct store_s3_ctx *ctx = (struct store_s3_ctx*) store->storage_ctx;
    xyzo_to_meta(cachePath, PATH_MAX, ctx->s3Cache.path, xmlconfig, options, x, y, z);
    store_s3_pipe_path(pipePath, PATH_MAX, cachePath);

    log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_cancel: cancel for %s", pipePath);

    store_s3_pipe_hup(pipePath);
    store_s3_pipe_remove(pipePath);
}

/*****************************************************************************/

#endif

static void store_s3_tile_stat_with_cache(struct storage_backend *store, const char *xmlconfig, const char *options, int x, int y, int z, struct stat_info *tile_stat) {

    char cachePath[PATH_MAX];
    char pipePath[PATH_MAX];
    struct store_s3_ctx *ctx = (struct store_s3_ctx*) store->storage_ctx;
    struct stat st_stat;
    int fd;

    /* get metatile file stat from cache */

    xyzo_to_meta(cachePath, PATH_MAX, ctx->s3Cache.path, xmlconfig, options, x, y, z);


    fd = open(cachePath, O_RDONLY);
    if(fd != -1) {

    	if(flock(fd, LOCK_SH) != -1) {

        	if(fstat(fd, &st_stat) != -1) {

        		tile_stat->size = st_stat.st_size;
        		tile_stat->mtime = st_stat.st_mtime;
        		tile_stat->atime = st_stat.st_atime;
        		tile_stat->ctime = st_stat.st_ctime;
        		tile_stat->origin = cache;

        		log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: #1 successfully read properties of metatile from cache %s", cachePath);

        		close(fd);
        		return;

            } else {
            	log_message(STORE_LOGLVL_ERR, "store_s3_tile_stat: fstat(%s) error, %s", cachePath, strerror(errno));
            	close(fd);
            }

    	} else {
    		log_message(STORE_LOGLVL_ERR, "store_s3_tile_stat: flock(%s) error, %s", cachePath, strerror(errno));
    		close(fd);
    	}

    } else if(errno != ENOENT) {
    	log_message(STORE_LOGLVL_ERR, "store_s3_tile_stat: open(%s) error, %s", cachePath, strerror(errno));
    }

    #ifdef APACHE

    	if(store_s3_socket_closed(store->socket)) {
    		tile_stat->aborted = 1;
    		return;
    	}

        store_s3_pipe_path(pipePath, PATH_MAX, cachePath);
		if(mkfifo(pipePath, S_IRUSR |  S_IWUSR | S_IRGRP | S_IWGRP) != 0 ) {

			if(errno == EEXIST) {

				int fd = open(pipePath, O_RDONLY |  O_NONBLOCK);

				struct pollfd fds[2];

				fds[0].fd =  fd;

				fds[1].fd = store->socket;
				fds[1].events = POLLRDHUP;

				log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: waiting for poll(%s) event ...", pipePath);
				int r = poll(fds, 2, store->timeout);

				if(r == -1) {
					log_message(STORE_LOGLVL_ERR, "store_s3_tile_stat: poll(%s) error, %s", pipePath, strerror(errno));
					close(fd);

				} else if(r == 0) {

					log_message(STORE_LOGLVL_WARNING, "store_s3_tile_stat: poll(%s) timeout", pipePath);
					close(fd);
					tile_stat->aborted = 1;

					return;

				} else {

					log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: poll(%s) events %x", pipePath, fds[r].revents);

					if(fds[0].revents & POLLHUP) {

						log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: poll(%s) event  POLLHUP(%x)", pipePath, fds[0].revents);
						close(fd);

						if(! stat(cachePath, &st_stat)) {
							tile_stat->size = st_stat.st_size;
							tile_stat->mtime = st_stat.st_mtime;
							tile_stat->atime = st_stat.st_atime;
							tile_stat->ctime = st_stat.st_ctime;
							tile_stat->origin = cache;
							log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: #2 successfully read properties of metatile from cache %s", cachePath);
							return;
						}

					} else if(fds[1].revents & POLLRDHUP) {

						log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: poll(%s) event  POLLRDHUP(%x)", pipePath, fds[1].revents);
						tile_stat->aborted = 1;
						close(fd);
						return;
					}
				}

			} else {
				log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: mkfifo(%s) failed, %s", pipePath, strerror(errno));
			}

		} else {
			log_message(STORE_LOGLVL_DEBUG, "store_s3_tile_stat: mkfifo(%s) successful", pipePath);
		}

    #endif


    /* get metatile file from S3 */

    char s3Path[PATH_MAX];
    store_s3_xyz_to_storagekey(store, xmlconfig, options, x, y, z, s3Path, PATH_MAX);
    struct s3_tile_request request = store_s3_get_tile_from_s3(ctx->ctx, s3Path, "store_s3_tile_stat");
    if(request.tile == NULL) {
        /* metatile doesn't exists on S3, need to render it */
        return;
    }

    /* get metatile file stat from S3 */

    tile_stat->size = request.tile_size;
    tile_stat->expired = request.tile_expired;
    tile_stat->mtime = request.tile_mod_time;
    tile_stat->origin = s3;

    /* save metatile file to cache */

    store_s3_save_tile_to_cache(request.tile, request.tile_size, cachePath, "store_s3_tile_stat");

    /* TODO: set metatile stat in cache according to S3 if needed */

    free(request.tile);


    #ifdef APACHE

    /* inform other apache processes that metatile is in cache now */

    store_s3_pipe_hup(pipePath);
    store_s3_pipe_remove(pipePath);

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
    tile_stat->origin = s3;
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
    tile_stat.origin = unknow;
    tile_stat.aborted = 0;

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

    /* inform apache processes that metatile is in S3 cache now */

    #ifdef RENDERD

    	char pipePath[PATH_MAX];
    	store_s3_pipe_path(pipePath, PATH_MAX, cachePath);
    	store_s3_pipe_hup(pipePath);
    	store_s3_pipe_remove(pipePath);

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
	#if defined APACHE || defined RENDERD
    	store->tile_cancel = &store_s3_tile_cancel;
    #endif

    return store;
#endif
}

/*****************************************************************************/



