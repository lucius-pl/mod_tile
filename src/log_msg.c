
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#define SYSLOG_NAMES
#include <syslog.h>
#ifdef RENDERD
#include <unistd.h>
#include <sys/syscall.h>
#endif
#ifdef APACHE
#include <httpd.h>
#include <http_log.h>
APLOG_USE_MODULE(tile);
extern server_rec* ap_server;
#endif

#include "log_msg.h"

#define MSG_SIZE 1000
#define RMSG_SIZE MSG_SIZE + 50


/*****************************************************************************/

//TODO: Make this function handle different logging backends, depending on if on compiles it from apache or something else

void log_message(int log_lvl, const char *format, ...) {
    va_list ap;
    char msg[MSG_SIZE];

    va_start(ap, format);
    vsnprintf(msg, MSG_SIZE, format, ap);
    va_end(ap);

    #if defined RENDERD
		#ifdef SYS_gettid
    		char rmsg[RMSG_SIZE];
    		snprintf(rmsg, RMSG_SIZE, "[tid %d] [%s] %s", syscall(SYS_gettid),  get_log_level_name(log_lvl), msg);
    		syslog(log_lvl, rmsg);
		#else
    		syslog(log_lvl, msg);
		#endif
    #elif defined APACHE
        ap_log_error(APLOG_MARK, log_lvl, 0, ap_server, msg);
    #else
        fprintf(stderr, msg);
        fflush(stderr);
    #endif
}

/*****************************************************************************/

/* get log level value by name */
short get_log_level_value(const char* name) {

    for(int i=0; i<sizeof(prioritynames)/sizeof(prioritynames[0]) - 1; i++) {
        if(strcasecmp(name, prioritynames[i].c_name) == 0) {
            return prioritynames[i].c_val;
        }
    }
    return -1;
}

/*****************************************************************************/

/* get log level name by value */
char* get_log_level_name(short value) {

    for(int i=0; i<sizeof(prioritynames)/sizeof(prioritynames[0]) - 1; i++) {
        if(value == prioritynames[i].c_val) {
            return prioritynames[i].c_name;
        }
    }
    return NULL;
}

/*****************************************************************************/

/* get log facility value by name */
short get_log_facility_value(const char* name) {

    for(int i=0; i<sizeof(facilitynames)/sizeof(facilitynames[0]) - 1; i++) {
        if(strcasecmp(name, facilitynames[i].c_name) == 0) {
            return facilitynames[i].c_val;
        }
    }
    return -1;
}

/*****************************************************************************/

/* get log facility name by value */
char* get_log_facility_name(short value) {

    for(int i=0; i<sizeof(facilitynames)/sizeof(facilitynames[0]) - 1; i++) {
        if(value == facilitynames[i].c_val) {
            return facilitynames[i].c_name;
        }
    }
    return NULL;
}

/*****************************************************************************/
