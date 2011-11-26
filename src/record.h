#include "postgres.h"
#include "tcop/utility.h"
#include "utils/timestamp.h"

#define MAX_FILE_NAME 255

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* buffer of queries */
typedef struct buffer_t {
    
    /* lock guarding the buffer (when writing a query from different backends) */
    LWLockId    lock;
    
    /* next item */
    int next;

    /* buffer (just the first item) */
    char buffer[1];
    
} buffer_t;

typedef struct info_t {
    
    /* lock guarding the file */
    LWLockId    lock;
    
    bool enabled;
    
} info_t;

/* log file */
typedef struct log_file_t {
    
    /* lock guarding the file */
    LWLockId    lock;
    
    /* basic filename */
    char base_filename[MAX_FILE_NAME];
    
    /* current filename */
    char curr_filename[MAX_FILE_NAME];
    
    /* current file (number, bytes) */
    unsigned int  file_number;
    unsigned int  file_bytes;
    
} log_file_t;
