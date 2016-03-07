/**
 * A modified version of render_list.c that does the following:
 *
 * - read list of expired tiles from stdin
 * - calculate a list of all meta tiles between minZoom and maxZoom
 *   affected by that expiry
 * - for all expired meta tiles that are actually present on disk, 
 *   issue a re-render request to renderd
 *
 * If you run a tile server that servers z0-z17, it makes sense to run 
 * osm2pgsql with "-e14-14" (i.e. three zoom levels lower than your max)
 * and then run this script with maxZoom=17. Due to z17 metatiles being
 * exactly the area of a z14 tile, your expiry list just becomes unnecessarily
 * large if you use -e17-17 (although this would lead to the same tiles
 * being re-rendered).
 * 
 * Be careful about minZoom; if you set it to 0, the tile x=0,y=0,z=0 will 
 * be expired every time the script is run. Having minZoom somewhere between
 * z8 and z12 probably makes sense, and then use another, time-based mechanism
 * to expire tiles.
 * 
 * NOTE: format on stdin is one tile per line, formatted "z/x/y". This is different
 * from render_list which wants "x y z". "z/x/y" is the format written by osm2pgsql.
 *
 * See also 
 * https://subversion.nexusuk.org/trac/browser/openpistemap/trunk/scripts/expire_tiles.py
 * for a database-backed expiry solution, or 
 * http://trac.openstreetmap.org/browser/applications/utils/export/tile_expiry
 * for a solution that works outside of osm2pgsql.
 * 
 * This program made by Frederik Ramm <frederik@remote.org>. My ideas and
 * contributions are in the public domain but being based on GPL'ed code
 * this program inherits the GPL.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/un.h>
#include <getopt.h>
#include <time.h>
#include <utime.h>
#include <string.h>
#include <strings.h>
#include <limits.h>
#include <utime.h>

#include <pthread.h>

#include "protocol.h"
#include "render_config.h"
#include "store.h"
#include "render_submit_queue.h"

static const char *tile_dir_default = HASH_PATH;

// macros handling our tile marking arrays (these are essentially bit arrays
// that have one bit for each tile on the respective zoom level; since we only
// need them for meta tile levels, even if someone were to render level 20,
// we'd still only use 4^17 bits = 2 GB RAM (plus a little for the lower zoom 
// levels) - this saves us the hassle of working with a tree structure.

#define TILE_REQUESTED(z,x,y) \
   (tile_requested[z][((x)*twopow[z]+(y))/(8*sizeof(int))]>>(((x)*twopow[z]+(y))%(8*sizeof(int))))&0x01
#define SET_TILE_REQUESTED(z,x,y) \
   tile_requested[z][((x)*twopow[z]+(y))/(8*sizeof(int))] |= (0x01 << (((x)*twopow[z]+(y))%(8*sizeof(int))));


#ifndef METATILE
#warning("render_expired not implemented for non-metatile mode. Feel free to submit fix")
int main(int argc, char **argv)
{
    fprintf(stderr, "render_expired not implemented for non-metatile mode. Feel free to submit fix!\n");
    return -1;
}
#else

typedef struct expire_request {
    const char *mapname;
    int x, y, z;
    struct expire_request *next;
    int terminate;
} expire_request;

typedef struct expire_config {
    int doRender;
    int deleteFrom;
    int touchFrom;
    struct storage_backend *store;
} expire_config;

typedef struct expire_thread_stats {
    int num_ignore;
    int num_touch;
    int num_render;
    int num_unlink;
} expire_thread_stats;

static expire_request *expire_queue = NULL;

static pthread_mutex_t expire_queue_lock;
static pthread_cond_t expire_queue_item_avail;

static pthread_t *expire_threads = NULL;

// tile marking arrays
unsigned int **tile_requested;

// "two raised to the power of [...]" - don't trust pow() to be efficient 
// for base 2
unsigned long long twopow[MAX_ZOOM];

static int minZoom = 0;
static int maxZoom = 18;
static int verbose = 0;
int work_complete;
static int maxLoad = MAX_LOAD_OLD;

void display_rate(struct timeval start, struct timeval end, int num) 
{
    int d_s, d_us;
    float sec;

    d_s  = end.tv_sec  - start.tv_sec;
    d_us = end.tv_usec - start.tv_usec;

    sec = d_s + d_us / 1000000.0;

    printf("Rendered %d tiles in %.2f seconds (%.2f tiles/s)\n", num, sec, num / sec);
    fflush(NULL);
}

void* expire_worker(void *arg)
{
    expire_config *config = (expire_config*) arg;
    struct stat_info s;
    expire_request *curr_request = NULL;
    char name[PATH_MAX];
    expire_thread_stats *stats = (expire_thread_stats*) calloc(1, sizeof(expire_thread_stats));

    while (1) {
        pthread_mutex_lock(&expire_queue_lock);
        while (expire_queue == NULL) {
            pthread_cond_wait(&expire_queue_item_avail, &expire_queue_lock);
        }
        curr_request = expire_queue;
        if (curr_request->terminate) {
            pthread_mutex_unlock(&expire_queue_lock);
            return stats;
        }
        expire_queue = curr_request->next;
        pthread_mutex_unlock(&expire_queue_lock);

        s = config->store->tile_stat(config->store, curr_request->mapname, "", curr_request->x, curr_request->y, curr_request->z);

        if (s.size > 0) // Tile exists
        {
            // tile exists on disk; render it
            if (config->deleteFrom != -1 && curr_request->z >= config->deleteFrom) {
                if (verbose)
                    printf("deleting: %s\n", config->store->tile_storage_id(config->store, curr_request->mapname, "", curr_request->x, curr_request->y, curr_request->z, name));
                config->store->metatile_delete(config->store, curr_request->mapname, curr_request->x, curr_request->y, curr_request->z);
                stats->num_unlink++;
            } else if (config->touchFrom != -1 && curr_request->z >= config->touchFrom) {
                if (verbose)
                    printf("touch: %s\n", config->store->tile_storage_id(config->store, curr_request->mapname, "", curr_request->x, curr_request->y, curr_request->z, name));
                config->store->metatile_expire(config->store, curr_request->mapname, curr_request->x, curr_request->y, curr_request->z);
                stats->num_touch++;
            } else if (config->doRender) {
                printf("render: %s\n", config->store->tile_storage_id(config->store, curr_request->mapname, "", curr_request->x, curr_request->y, curr_request->z, name));
                enqueue(curr_request->mapname, curr_request->x, curr_request->y, curr_request->z);
                stats->num_render++;
            }
            /*
             if (!(num_render % 10))
             {
             gettimeofday(&end, NULL);
             printf("\n");
             printf("Meta tiles rendered: ");
             display_rate(start, end, num_render);
             printf("Total tiles rendered: ");
             display_rate(start, end, num_render * METATILE * METATILE);
             printf("Total tiles in input: %d\n", num_read);
             printf("Total tiles expanded from input: %d\n", num_all);
             printf("Total tiles ignored (not on disk): %d\n", num_ignore);
             }
             */
        } else {
            if (verbose)
                printf("not in storage: %s\n", config->store->tile_storage_id(config->store, curr_request->mapname, "", curr_request->x, curr_request->y, curr_request->z, name));
            stats->num_ignore++;
        }
        free(curr_request);
        curr_request = NULL;
    }
    return stats;
}

void spawn_expire_threads(int num_expire_threads, expire_config *config)
{
    pthread_mutex_init(&expire_queue_lock, NULL);
    pthread_cond_init(&expire_queue_item_avail, NULL);

    printf("Starting %d expire threads\n", num_expire_threads);
    expire_threads = calloc(sizeof(pthread_t), num_expire_threads);
    if (!expire_threads) {
        perror("Error allocating expire thread memory");
        exit(1);
    }
    for (int i = 0; i < num_expire_threads; i++) {
        if (pthread_create(&expire_threads[i], NULL, expire_worker, config) != 0) {
            perror("Thread creation failed");
            exit(1);
        }
    }
}

void add_expire_request(const char *xmlname, int x, int y, int z, int terminate)
{
    pthread_mutex_lock(&expire_queue_lock);
    expire_request *r = (expire_request*) calloc(1, sizeof(expire_request));
    r->mapname = xmlname;
    r->x = x;
    r->y = y;
    r->z = z;
    r->next = expire_queue;
    r->terminate = terminate;
    expire_queue = r;
    pthread_cond_broadcast(&expire_queue_item_avail);
    pthread_mutex_unlock(&expire_queue_lock);
}

void wait_expire_threads(int num_expire_threads, expire_thread_stats *totals)
{
    totals->num_ignore = 0;
    totals->num_render = 0;
    totals->num_touch = 0;
    totals->num_unlink = 0;

    for (int i = 0; i < num_expire_threads; i++) {
        void *thread_stats_ptr;
        pthread_join(expire_threads[i], &thread_stats_ptr);
        expire_thread_stats *thread_stats = (expire_thread_stats*) thread_stats_ptr;
        totals->num_ignore += thread_stats->num_ignore;
        totals->num_render += thread_stats->num_render;
        totals->num_touch += thread_stats->num_touch;
        totals->num_unlink += thread_stats->num_unlink;
        free(thread_stats);
    }
}

int main(int argc, char **argv)
{
    char *spath = strdup(RENDER_SOCKET);
    const char *mapname_default = XMLCONFIG_DEFAULT;
    const char *mapname = mapname_default;
    const char *tile_dir = tile_dir_default;
    int x, y, z;
    struct timeval start, end;
    int num_all = 0, num_read = 0;
    int c;
    int numRenderThreads = 1;
    int numExpireThreads = 1;
    expire_config config;
    int i;
    int report_interval = 100;

    // excess_zoomlevels is how many zoom levels at the large end
    // we can ignore because their tiles will share one meta tile.
    // with the default METATILE==8 this is 3.
    int excess_zoomlevels = 0;
    int mt = METATILE;
    while (mt > 1)
    {
        excess_zoomlevels++;
        mt >>= 1;
    }

    while (1)
    {
        int option_index = 0;
        static struct option long_options[] =
        {
            {"min-zoom", 1, 0, 'z'},
            {"max-zoom", 1, 0, 'Z'},
            {"socket", 1, 0, 's'},
            {"num-render-threads", 1, 0, 'n'},
            {"delete-from", 1, 0, 'd'},
            {"touch-from", 1, 0, 'T'},
            {"tile-dir", 1, 0, 't'},
            {"max-load", 1, 0, 'l'},
            {"map", 1, 0, 'm'},
            {"verbose", 0, 0, 'v'},
            {"help", 0, 0, 'h'},
            {"num-expire-threads", 1, 0, 'e'},
            {0, 0, 0, 0}
        };

        c = getopt_long(argc, argv, "hvz:Z:s:m:t:n:l:T:d:e:", long_options, &option_index);

        if (c == -1)
            break;

        switch (c) {
            case 's':   /* -s, --socket */
                spath = strdup(optarg);
                break;
            case 't':   /* -t, --tile-dir */
                tile_dir=strdup(optarg);
                break;
            case 'm':   /* -m, --map */
                mapname=strdup(optarg);
                break;
            case 'n':   /* -n, --num-render-threads */
                numRenderThreads=atoi(optarg);
                if (numRenderThreads <= 0) {
                    fprintf(stderr, "Invalid number of render threads, must be at least 1\n");
                    return 1;
                }
                break;
            case 'd':   /* -d, --delete-from */
                config.deleteFrom = atoi(optarg);
                if (config.deleteFrom < 0 || config.deleteFrom > MAX_ZOOM) {
                    fprintf(stderr, "Invalid 'delete-from' zoom, must be between 0 and %d\n", MAX_ZOOM);
                    return 1;
                }
                break;
            case 'T':   /* -T, --touch-from */
                config.touchFrom = atoi(optarg);
                if (config.touchFrom < 0 || config.touchFrom > MAX_ZOOM) {
                    fprintf(stderr, "Invalid 'touch-from' zoom, must be between 0 and %d\n", MAX_ZOOM);
                    return 1;
                }
                break;
            case 'z':   /* -z, --min-zoom */
                minZoom = atoi(optarg);
                if (minZoom < 0 || minZoom > MAX_ZOOM) {
                    fprintf(stderr, "Invalid minimum zoom selected, must be between 0 and %d\n", MAX_ZOOM);
                    return 1;
                }
                break;
            case 'Z':   /* -Z, --max-zoom */
                maxZoom = atoi(optarg);
                if (maxZoom < 0 || maxZoom > MAX_ZOOM) {
                    fprintf(stderr, "Invalid maximum zoom selected, must be between 0 and %d\n", MAX_ZOOM);
                    return 1;
                }
                break;
            case 'l':   /* -l, --max-load */
                maxLoad = atoi(optarg);
                break;
            case 'v':   /* -v, --verbose */
                verbose=1;
                break;
            case 'e':   /* -n, --num-expire-threads */
                numExpireThreads = atoi(optarg);
                if (numExpireThreads <= 0) {
                    fprintf(stderr, "Invalid number of expire threads, must be at least 1\n");
                    return 1;
                }
                break;
            case 'h':   /* -h, --help */
                fprintf(stderr, "Usage: render_expired [OPTION] ...\n");
                fprintf(stderr, "  -m, --map=MAP        render tiles in this map (defaults to '" XMLCONFIG_DEFAULT "')\n");
                fprintf(stderr, "  -s, --socket=SOCKET  unix domain socket name for contacting renderd\n");
                fprintf(stderr, "  -n, --num-render-threads=N  the number of parallel rendering request threads (default 1)\n");
                fprintf(stderr, "  -t, --tile-dir       tile cache directory (defaults to '" HASH_PATH "')\n");
                fprintf(stderr, "  -z, --min-zoom=ZOOM  filter input to only render tiles greater or equal to this zoom level (default is 0)\n");
                fprintf(stderr, "  -Z, --max-zoom=ZOOM  filter input to only render tiles less than or equal to this zoom level (default is %d)\n", 18);
                fprintf(stderr, "  -d, --delete-from=ZOOM  when expiring tiles of ZOOM or higher, delete them instead of re-rendering (default is off)\n");
                fprintf(stderr, "  -T, --touch-from=ZOOM   when expiring tiles of ZOOM or higher, touch them instead of re-rendering (default is off)\n");
                fprintf(stderr, "  -e, --num-expire-threads=N  the number of expiration processing threads (default 1)\n");
                fprintf(stderr, "Send a list of tiles to be rendered from STDIN in the format:\n");
                fprintf(stderr, "  z/x/y\n");
                fprintf(stderr, "e.g.\n");
                fprintf(stderr, "  1/0/1\n");
                fprintf(stderr, "  1/1/1\n");
                fprintf(stderr, "  1/0/0\n");
                fprintf(stderr, "  1/1/0\n");
                fprintf(stderr, "The above would cause all 4 tiles at zoom 1 to be rendered\n");
                return -1;
            default:
                fprintf(stderr, "unhandled option '%c'\n", c);
                break;
        }
    }

    if (maxZoom < minZoom) {
        fprintf(stderr, "Invalid zoom range, max zoom must be greater or equal to minimum zoom\n");
        return 1;
    }

    if (minZoom < excess_zoomlevels) minZoom = excess_zoomlevels;

    // initialise arrays for tile markings

    tile_requested = (unsigned int **) malloc((maxZoom - excess_zoomlevels + 1)*sizeof(unsigned int *));

    for (i=0; i<=maxZoom - excess_zoomlevels; i++)
    {
        // initialize twopow array
        twopow[i] = (i == 0) ? 1 : twopow[i - 1] * 2;
        unsigned long long fourpow = twopow[i] * twopow[i];
        tile_requested[i] = (unsigned int*) calloc((fourpow / METATILE) + 1, 1);
        if (NULL == tile_requested[i])
        {
            fprintf(stderr, "not enough memory available.\n");
            return 1;
        }
    }

    fprintf(stderr, "Rendering client\n");

    gettimeofday(&start, NULL);

    if (   ( config.touchFrom != -1 && minZoom < config.touchFrom )
        || (config.deleteFrom != -1 && minZoom < config.deleteFrom)
        || ( config.touchFrom == -1 && config.deleteFrom == -1) ) {
        // No need to spawn render threads, when we're not actually going to rerender tiles
        spawn_workers(numRenderThreads, spath, maxLoad);
        config.doRender = 1;
    }

    config.store = init_storage_backend(tile_dir);
    if (config.store == NULL) {
        fprintf(stderr, "failed to initialise storage backend %s\n", tile_dir);
        return 1;
    }

    spawn_expire_threads(numExpireThreads, &config);

    while (!feof(stdin)) {
        struct stat_info s;
        int n = fscanf(stdin, "%d/%d/%d", &z, &x, &y);

        if (verbose)
            printf("read: x=%d y=%d z=%d\n", x, y, z);
        if (n != 3) {
            // Discard input line
            char tmp[1024];
            char *r = fgets(tmp, sizeof(tmp), stdin);
            if (!r)
                continue;
            fprintf(stderr, "bad line %d: %s", num_all, tmp);
            continue;
        }

        while (z > maxZoom) {
            x>>=1; y>>=1; z--;
        }
        while (z < maxZoom) {
            x<<=1; y<<=1; z++;
        }
        //printf("loop: x=%d y=%d z=%d up to z=%d\n", x, y, z, minZoom);
        num_read++;
        if (num_read % 100 == 0)
            printf("Read and expanded %i tiles from list.\n", num_read);

        for (; z>= minZoom; z--, x>>=1, y>>=1)
        {
            if (verbose)
                printf("process: x=%d y=%d z=%d\n", x, y, z);

            // don't do anything if this tile was already requested.
            // renderd does keep a list internally to avoid enqueing the same tile
            // twice but in case it has already rendered the tile we don't want to
            // cause extra work.
            if (TILE_REQUESTED(z - excess_zoomlevels,x>>excess_zoomlevels,y>>excess_zoomlevels)) 
            {
                if (verbose)
                    printf("already requested\n");
                break;
            }

            // mark tile as requested. (do this even if, below, the tile is not
            // actually requested due to not being present on disk, to avoid
            // unnecessary later stat'ing).
            SET_TILE_REQUESTED(z - excess_zoomlevels,x>>excess_zoomlevels,y>>excess_zoomlevels);

            // commented out - seems to cause problems in MT environment,
            // trying to write to already-closed file
            //check_load();

            num_all++;

            add_expire_request(mapname, x, y, z, 0);
        }
    }

    // add a termination request
    add_expire_request("", 0, 0, 0, 1);

    expire_thread_stats thread_totals;
    wait_expire_threads(numExpireThreads, &thread_totals);

    if (config.doRender) {
        finish_workers();
    }

    free(spath);
    if (mapname != mapname_default) {
        free((void *)mapname);
        mapname = NULL;
    }
    if (tile_dir != tile_dir_default) {
        free((void *)tile_dir);
        tile_dir = NULL;
    }
    config.store->close_storage(config.store);
    free(config.store);
    config.store = NULL;

    for (i = 0; i <= maxZoom - excess_zoomlevels; i++) {
        free(tile_requested[i]);
    }
    free(tile_requested);
    tile_requested = NULL;

    gettimeofday(&end, NULL);
    printf("\nTotal for all tiles rendered\n");
    printf("Meta tiles rendered: ");
    display_rate(start, end, thread_totals.num_render);
    printf("Total tiles rendered: ");
    display_rate(start, end, thread_totals.num_render * METATILE * METATILE);
    printf("Total tiles in input: %d\n", num_read);
    printf("Total tiles expanded from input: %d\n", num_all);
    printf("Total meta tiles deleted: %d\n", thread_totals.num_unlink);
    printf("Total meta tiles touched: %d\n", thread_totals.num_touch);
    printf("Total tiles ignored (not in storage): %d\n", thread_totals.num_ignore);

    return 0;
}
#endif
