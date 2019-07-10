#ifndef LOG_MSG_H
#define LOG_MSG_H

#ifdef __cplusplus
extern "C" {
#endif

#include <syslog.h>

#define STORE_LOGLVL_DEBUG    LOG_DEBUG
#define STORE_LOGLVL_INFO     LOG_INFO
#define STORE_LOGLVL_WARNING  LOG_WARNING
#define STORE_LOGLVL_ERR      LOG_ERR


void log_message(int log_lvl, const char *format, ...);

short get_log_level_value(const char* name);
char* get_log_level_name(short value);
short get_log_facility_value(const char* name);
char* get_log_facility_name(short value);


#ifdef __cplusplus
}
#endif
#endif
