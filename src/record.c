#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/shm.h>
#include <sys/stat.h>

#include <sys/time.h>
#include <sys/types.h>
#include <sys/ipc.h>

#include "postgres.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/fd.h"

#include "executor/executor.h"
#include "executor/instrument.h"
#include "utils/guc.h"

#include "record.h"

static int nesting_level = 0;

static int    max_files     = 1000;         /* 100 files */
static int    max_file_size = 1073741824;   /* 1GB per file */
static int    buffer_size   = 1048576;      /* 1MB buffer */
static bool   enabled       = false;        /* disabled by default */
static bool   normalize     = false;        /* normalize the queries */

static char * base_filename;

/* private functions */
static void pg_record_shmem_startup(void);
static void pg_record_shmem_shutdown(int code, Datum arg);

static void buffer_add_query(double seconds, const char * queryString);
static void query_write(double duration, const char * query, int len,
                        const char * header, int hlen);
static void buffer_write(void);

static void format_filename(char * buffer, const char * basename,
                            int sequence, int len);
    
static void format_prefix(char * buffer, int len, int backend,
                          double duration, int qlen);

static void prepare_file(log_file_t * log, buffer_t * buff, int len);

static bool file_exists (const char * filename, off_t * size);

static bool collecting_enabled(void);

static char * normalize_query(const char * q);

#if (PG_VERSION_NUM >= 90100)
static void set_enabled(bool newEnabled, void *extra);
char * show_enabled(void);
#else
static bool set_enabled(bool newEnabled, bool doit, GucSource source);
char * show_enabled(void);
#endif

/* return from a hook */
#if (PG_VERSION_NUM >= 90100)
#define HOOK_RETURN(a)  return;
#else
#define HOOK_RETURN(a)  return (a);
#endif

#define SEGMENT_NAME        "pg_record_buffer"
#define SEGMENT_SIZE        (sizeof(buffer_t) + sizeof(log_file_t)\
                                   + sizeof(info_t) + buffer_size)
#define DEFAULT_DUMP_FILE   "/tmp/pg-queries.log"

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

void        _PG_init(void);
void        _PG_fini(void);

static void explain_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void explain_ExecutorRun(QueryDesc *queryDesc,
                    ScanDirection direction,
                    long count);
static void explain_ExecutorEnd(QueryDesc *queryDesc);

static void pg_record_ProcessUtility(Node *parsetree,
              const char *queryString, ParamListInfo params, bool isTopLevel,
                    DestReceiver *dest, char *completionTag);

#if (PG_VERSION_NUM >= 90100)
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static void explain_ExecutorFinish(QueryDesc *queryDesc);
#endif

/* the query buffer (info and data) */
static buffer_t   *query_buffer = NULL;
static log_file_t *log_file = NULL;
static info_t     *info = NULL;

/* start time of the query */
static struct timeval start_time;
static bool collect;

/*
 * Module load callback
 */
void
_PG_init(void)
{
    
    /* can be preloaded only from postgresql.conf */
    if (!process_shared_preload_libraries_in_progress)
        return;
    
    /* Define custom GUC variables. */
    DefineCustomStringVariable("query_recorder.filename",
                               "Base filename to write the recorded queries.",
                             NULL,
                             &base_filename,
                             false,
                             PGC_BACKEND,
                             0,
#if (PG_VERSION_NUM >= 90100)
                             NULL,
#endif
                             NULL,
                             NULL);
    
    DefineCustomIntVariable("query_recorder.max_files",
                            "How many files will be rotated.",
                             NULL,
                             &max_files,
                             100,
                             1, 1000,
                             PGC_BACKEND,
                             0,
#if (PG_VERSION_NUM >= 90100)
                             NULL,
#endif
                             NULL,
                             NULL);
    
    DefineCustomIntVariable("query_recorder.size_limit",
                            "File size limit (will create multiple files .000 - .999)",
                             NULL,
                             &max_file_size,
                             1024*1024*1024/BLCKSZ, /* 1GB */
                             1024*1024/BLCKSZ, 1024*1024*1024/BLCKSZ, /* 1MB - 1GB */
                             PGC_BACKEND,
                             GUC_UNIT_BLOCKS,
#if (PG_VERSION_NUM >= 90100)
                             NULL,
#endif
                             NULL,
                             NULL);
    
    DefineCustomIntVariable("query_recorder.buffer_size",
                            "Size of the buffer used to collect queries.",
                             NULL,
                             &buffer_size,
                             1024*1024/BLCKSZ, /* 1MB */
                             1, 16*1024*1024/BLCKSZ, /* 1 block - 16MB */
                             PGC_BACKEND,
                             GUC_UNIT_BLOCKS,
#if (PG_VERSION_NUM >= 90100)
                             NULL,
#endif
                             NULL,
                             NULL);
    
    /* Define custom GUC variables. */
    DefineCustomBoolVariable("query_recorder.normalize",
                             "Replace line breaks and carriage returns with spaces.",
                             NULL,
                             &normalize,
                             false,
                             PGC_BACKEND,
                             0,
#if (PG_VERSION_NUM >= 90100)
                             NULL,
#endif
                             &set_enabled,
                             NULL);
    
    /* Define custom GUC variables. */
    DefineCustomBoolVariable("query_recorder.enabled",
                             "Enable or disable recording of queries.",
                             NULL,
                             &enabled,
                             false,
                             PGC_SUSET,
                             0,
#if (PG_VERSION_NUM >= 90100)
                             NULL,
#endif
                             &set_enabled,
                             (GucShowHook)(&show_enabled));

    EmitWarningsOnPlaceholders("query_recorder");
    
    /*
     * Request additional shared resources.  (These are no-ops if we're not in
     * the postmaster process.)  We'll allocate or attach to the shared
     * resources in pg_record_shmem_startup().
     */
    RequestAddinShmemSpace(SEGMENT_SIZE);
    RequestAddinLWLocks(1);

    /* Install hooks. */
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = pg_record_shmem_startup;
    
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = explain_ExecutorStart;
    prev_ExecutorRun = ExecutorRun_hook;
    ExecutorRun_hook = explain_ExecutorRun;
#if (PG_VERSION_NUM >= 90100)
    prev_ExecutorFinish = ExecutorFinish_hook;
    ExecutorFinish_hook = explain_ExecutorFinish;
#endif
    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = explain_ExecutorEnd;
    prev_ProcessUtility = ProcessUtility_hook;
    ProcessUtility_hook = pg_record_ProcessUtility;
    
}


/*
 * Module unload callback
 */
void
_PG_fini(void)
{
    /* Uninstall hooks. */
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorRun_hook = prev_ExecutorRun;
#if (PG_VERSION_NUM >= 90100)
    ExecutorFinish_hook = prev_ExecutorFinish;
#endif
    ExecutorEnd_hook = prev_ExecutorEnd;
    shmem_startup_hook = prev_shmem_startup_hook;
}

/*
 * ExecutorStart hook: start up logging if needed
 */
static void
explain_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    /* decide */
    collect = collecting_enabled();
    
    if (collect) {
        gettimeofday(&start_time, NULL);
    }
    
    if (prev_ExecutorStart)
        prev_ExecutorStart(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);

    /* Enable the histogram whenever the histogram is dynamic or (bins>0). */
    if (collect)
    {
        /*
         * Set up to track total elapsed time in ExecutorRun.  Make sure the
         * space is allocated in the per-query context so it will go away at
         * ExecutorEnd.
         */
        if (queryDesc->totaltime == NULL)
        {
            MemoryContext oldcxt;

            oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
            queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL);
            MemoryContextSwitchTo(oldcxt);
        }
    }
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
explain_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
    nesting_level++;
    PG_TRY();
    {
        if (prev_ExecutorRun)
            prev_ExecutorRun(queryDesc, direction, count);
        else
            standard_ExecutorRun(queryDesc, direction, count);
        nesting_level--;
    }
    PG_CATCH();
    {
        nesting_level--;
        PG_RE_THROW();
    }
    PG_END_TRY();
}

#if (PG_VERSION_NUM >= 90100)
/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
explain_ExecutorFinish(QueryDesc *queryDesc)
{
    nesting_level++;
    PG_TRY();
    {
        if (prev_ExecutorFinish)
            prev_ExecutorFinish(queryDesc);
        else
            standard_ExecutorFinish(queryDesc);
        nesting_level--;
    }
    PG_CATCH();
    {
        nesting_level--;
        PG_RE_THROW();
    }
    PG_END_TRY();
}
#endif

/*
 * ExecutorEnd hook: log results if needed
 */
static void
explain_ExecutorEnd(QueryDesc *queryDesc)
{
    if (queryDesc->totaltime && (nesting_level == 0) && collect)
    {
        double seconds;
        
        /*
         * Make sure stats accumulation is done.  (Note: it's okay if several
         * levels of hook all do this.)
         */
        InstrEndLoop(queryDesc->totaltime);
        
        /* Log plan if duration is exceeded. */
        seconds = queryDesc->totaltime->total;
        
        buffer_add_query(seconds, queryDesc->sourceText);
        
    }

    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
    
}

/*
 * ProcessUtility hook
 */
static void
pg_record_ProcessUtility(Node *parsetree, const char *queryString,
                    ParamListInfo params, bool isTopLevel,
                    DestReceiver *dest, char *completionTag)
{
    if ((nesting_level == 0) && collect)
    {
        instr_time  start;
        instr_time  duration;
        float       seconds;

        INSTR_TIME_SET_CURRENT(start);

        nesting_level++;
        PG_TRY();
        {
            if (prev_ProcessUtility)
                prev_ProcessUtility(parsetree, queryString, params,
                                    isTopLevel, dest, completionTag);
            else
                standard_ProcessUtility(parsetree, queryString, params,
                                        isTopLevel, dest, completionTag);
            nesting_level--;
        }
        PG_CATCH();
        {
            nesting_level--;
            PG_RE_THROW();
        }
        PG_END_TRY();

        INSTR_TIME_SET_CURRENT(duration);
        INSTR_TIME_SUBTRACT(duration, start);
        
        seconds = INSTR_TIME_GET_DOUBLE(duration);
        
        buffer_add_query(seconds, queryString);

    }
    else
    {
        if (prev_ProcessUtility)
            prev_ProcessUtility(parsetree, queryString, params,
                                isTopLevel, dest, completionTag);
        else
            standard_ProcessUtility(parsetree, queryString, params,
                                    isTopLevel, dest, completionTag);
    }
}

/* This is probably the most important part - allocates the shared 
 * segment, initializes it etc. */
static
void pg_record_shmem_startup() {

    bool    found = FALSE;
    char   *segment = NULL;
    int     i;
    off_t   size;
    char    filename[MAX_FILE_NAME];
    
    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();
    
    /*
     * Create or attach to the shared memory state, including hash table
     */
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    segment = ShmemInitStruct(SEGMENT_NAME, SEGMENT_SIZE, &found);

    /* set the pointers */
    info         =     (info_t*)(segment);
    log_file     = (log_file_t*)(segment + sizeof(info_t));
    query_buffer =   (buffer_t*)(segment + sizeof(info_t) + sizeof(log_file_t));
        
    elog(DEBUG1, "initializing query buffer segment (size: %lu B)", SEGMENT_SIZE);

    if (! found) {
        
        /* First time through ... */
        info->lock = LWLockAssign();
        log_file->lock = LWLockAssign();
        query_buffer->lock = LWLockAssign();
        
        query_buffer->next = 0;
        
        max_file_size *= BLCKSZ;
        buffer_size   *= BLCKSZ;
        
        info->enabled = enabled;

        if (base_filename == NULL) {
            snprintf(log_file->base_filename, 255, "%s", DEFAULT_DUMP_FILE);
        } else {
            snprintf(log_file->base_filename, 255, "%s", base_filename);
        }
        
        memset(query_buffer->buffer, 0, (buffer_size)*sizeof(char));
        
        elog(DEBUG1, "shared memory segment (query buffer) successfully created");
        
        /* use the first file */
        log_file->file_number = 0;
        log_file->file_bytes  = 0;
        
        format_filename(log_file->curr_filename, log_file->base_filename,
                        log_file->file_number, MAX_FILE_NAME);
            
        /* find the last file (iterate) */
        for (i = 0; i < max_files; i++) {
            
            format_filename(filename, log_file->base_filename, i, MAX_FILE_NAME);
                
            if (! file_exists(filename, &size)) {
                break;
            }
            
            strcpy(log_file->curr_filename, filename);
            
            /* use the last found file */
            log_file->file_number = i;
            log_file->file_bytes = size;
            
        }
        
    }

    LWLockRelease(AddinShmemInitLock);

    /*
     * If we're in the postmaster (or a standalone backend...), set up a shmem
     * exit hook to dump the statistics to disk.
     */
    if (!IsUnderPostmaster)
        on_shmem_exit(pg_record_shmem_shutdown, (Datum) 0);
    
}

/* needs to be already locked */
static
void buffer_add_query(double duration, const char * query) {
    
    static char header[256];
    int len, hlen;
    char * q = NULL;
    
    /* do we need to normalize the query? */
    if (normalize) {
        q = normalize_query(q);
    } else {
        q = (char*)query;
    }
    
    len = strlen(q);
    
    /* prepare the line prefix */
    format_prefix(header, 256, MyBackendId, duration, len);
    hlen = strlen(header);
    
    /* add the query to the buffer */
    LWLockAcquire(query_buffer->lock, LW_EXCLUSIVE);
    
    if (query_buffer->next + hlen + len + 2 > buffer_size) {
        LWLockAcquire(log_file->lock, LW_EXCLUSIVE);
        buffer_write();
        LWLockRelease(log_file->lock);
        query_buffer->next = 0;
    }
    
    if (len + hlen + 2 > buffer_size) {
        LWLockAcquire(log_file->lock, LW_EXCLUSIVE);
        query_write(duration, q, len, header, hlen);
        LWLockRelease(log_file->lock);
    } else {
        
        memcpy(&(query_buffer->buffer[query_buffer->next]), header, hlen);
        query_buffer->next = query_buffer->next + hlen;
        
        memcpy(&(query_buffer->buffer[query_buffer->next]), q, len);
        query_buffer->buffer[query_buffer->next + len] = '\n';
        query_buffer->next = query_buffer->next + len + 1;
    }
    
    LWLockRelease(query_buffer->lock);
    
}

/* format the line header */
static
void format_prefix(char * buffer, int len, int backend, double duration, int qlen) {

    snprintf(buffer, len, "%u.%06u\t%d\t%f\t%d\t", (unsigned int)start_time.tv_sec,
             (unsigned int)start_time.tv_usec, backend, duration, qlen);

}

/* prepare the actual file name (append the sequence etc.) */
static
void format_filename(char * buffer, const char * basename, int sequence, int len) {
    
    snprintf(buffer, len, "%s.%.03d", basename, sequence);

}

/* check if the buffer fits into the current file, prepare the next one if needed */
static
void prepare_file(log_file_t * log, buffer_t * buff, int len) {
    
    if (log->file_bytes + len > max_file_size) {
        
        log->file_number = (log->file_number + 1) % max_files;
        log->file_bytes = len;
        
        format_filename(log->curr_filename, log->base_filename,
            log->file_number, MAX_FILE_NAME);
        
        if (file_exists(log->curr_filename, NULL)) {
            unlink(log->curr_filename);
        }
        
    } else {
        log->file_bytes += len;
    }

}

/* Dumps the histogram data into a file (with a md5 hash of the contents at the beginning). */
static
void buffer_write() {
    
    FILE * file;
    
    prepare_file(log_file, query_buffer, query_buffer->next);
    
    file = AllocateFile(log_file->curr_filename, PG_BINARY_A);
    if (file == NULL)
        goto error;
    
    /* now write the actual shared segment */
    if (fwrite(query_buffer->buffer, query_buffer->next, 1, file) != 1)
        goto error;
    
    FreeFile(file);
    
    return;

error:
    ereport(LOG,
            (errcode_for_file_access(),
             errmsg("could not write query histogram file \"%s\": %m",
                    log_file->curr_filename)));
    if (file)
        FreeFile(file);
    
}


/* Dumps the histogram data into a file (with a md5 hash of the contents at the beginning). */
static
void query_write(double duration, const char * query, int len, const char * header, int hlen) {
    
    FILE * file;
    
    /* write the buffer first */
    buffer_write();
    
    /* now write the query */
    prepare_file(log_file, query_buffer, hlen + len);
    
    file = AllocateFile(log_file->curr_filename, PG_BINARY_A);
    if (file == NULL)
        goto error;
    
    /* now write the actual shared segment */
    if (fwrite(header, hlen, 1, file) != 1)
       goto error;
    
    /* now write the actual shared segment */
    if (fwrite(query, len, 1, file) != 1)
       goto error;
    
    FreeFile(file);
    
    return;

error:
    ereport(LOG,
            (errcode_for_file_access(),
             errmsg("could not write query to the file \"%s\": %m",
                    log_file->curr_filename)));
    if (file)
        FreeFile(file);
    
}

/* Dumps the histogram data into a file (with a md5 hash of the contents at the beginning). */
static
void pg_record_shmem_shutdown(int code, Datum arg) {
    
    FILE * file;
    
    /* do we need to write the queries? */
    if (query_buffer->next == 0) {
        return;
    }
    
    prepare_file(log_file, query_buffer, query_buffer->next);
    
    file = AllocateFile(log_file->curr_filename, PG_BINARY_A);
    if (file == NULL)
        goto error;
    
    /* now write the actual shared segment */
    if (fwrite(query_buffer->buffer, query_buffer->next, 1, file) != 1)
       goto error;
    
    FreeFile(file);
    
    return;

error:
    ereport(LOG,
            (errcode_for_file_access(),
             errmsg("could not write query buffer to the file \"%s\": %m",
                    log_file->curr_filename)));
    if (file)
        FreeFile(file);
    
}

/* maybe we could do this without locking ? */
static bool collecting_enabled() {
    
    bool result;
    
    LWLockAcquire(info->lock, LW_EXCLUSIVE);
    result = info->enabled;
    LWLockRelease(info->lock);
    
    return result;

}

#if (PG_VERSION_NUM >= 90100)
static void set_enabled(bool newEnabled, void *extra) {
#else
static bool set_enabled(bool newEnabled, bool doit, GucSource source) {
#endif

    enabled = newEnabled;

    if (info != NULL) {
        
        LWLockAcquire(info->lock, LW_EXCLUSIVE);
        info->enabled = enabled;
        LWLockRelease(info->lock);
        
        /* push the current buffer to the disk */
        LWLockAcquire(log_file->lock, LW_EXCLUSIVE);
        buffer_write();
        LWLockRelease(log_file->lock);
        
    }
    
    HOOK_RETURN(true);
    
}

/* prints whether the query collection is enabled or disabled */
char * show_enabled(void) {
    
    char * val = (char*)palloc(4);
    
    LWLockAcquire(info->lock, LW_SHARED);
    strcpy(val, (info->enabled) ? "on" : "off");
    LWLockRelease(info->lock);
    
    return val;
    
}

/* checks whether the file exists (and returns size if needed) */
static
bool file_exists (const char * filename, off_t * size) {
    
    struct stat buf;
    int i = stat (filename, &buf);
    
    if ((i == 0) && (size != NULL)) {
        *size = buf.st_size;
    }
    
    return (i == 0);
    
}

/* replace the EOL characters ('\n' and '\r' with spaces) */
static char * normalize_query(const char * q) {
    int i;
    int len = strlen(q);
    char * normalized = (char*)palloc(len + 1);
    memcpy(normalized, q, len+1);
    
    for (i = 0; i < len; i++) {
        if (normalized[i] == '\r' || normalized[i] == '\n') {
            normalized[i] = ' ';
        }
    }
    
    return normalized;
    
}
