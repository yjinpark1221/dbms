#ifndef __RECOVERY_H__
#define __RECOVERY_H__

#include "buffer.h"
#include "trx.h"
#include <set>
#include <queue>

#define BEGIN 0
#define UPDATE 1
#define COMMIT 2
#define ROLLBACK 3
#define COMPENSATE 4

typedef uint64_t lsn_t;
typedef uint32_t logsize_t;
typedef int32_t type_t;
typedef std::pair<table_t, pagenum_t> table_page_t; 

struct mlog_t;
struct log_t {
    char a[300];
    log_t(mlog_t log);
    log_t();
};

struct mlog_t {
    logsize_t log_size;
    lsn_t lsn;
    lsn_t prevlsn;
    int32_t trx_id;
    u32_t type;
    bool dirty;
    
    table_t table_id;
    pagenum_t pagenum;
    u16_t offset;
    u16_t size;
    std::string old_image;
    std::string new_image;

    lsn_t nextundolsn;

    mlog_t() {
        offset = size = 0;
        type = 5;
    }

    mlog_t(logsize_t log_size, lsn_t lsn, lsn_t prevlsn, int32_t trx_id, u32_t type, table_t table_id = 0, pagenum_t pagenum = 0, u16_t offset = 0, u16_t size = 0, std::string old_image = "", std::string new_image = "") {
        this->log_size = log_size;
        this->lsn = lsn;
        this->prevlsn = prevlsn;
        this->trx_id = trx_id;
        this->type = type;
        this->dirty = false;
        this->table_id = table_id;
        this->pagenum = pagenum;
        this->offset = offset;
        this->size = size;
        this->old_image = old_image;
        this->new_image = new_image;
        this->dirty = true;
    }

    mlog_t(log_t& log) {
        size = *((logsize_t*)log.a);
        lsn = *((lsn_t*)(log.a + 4));
        prevlsn = *((lsn_t*)(log.a + 12));
        trx_id = *((u32_t*)(log.a + 20));
        type = *((type_t*)(log.a + 24));
        table_id = *((table_t*)(log.a + 28));
        pagenum = *((pagenum_t*)(log.a + 36));
        offset = *((u16_t*)(log.a + 44));
        size = *((u16_t*)(log.a + 46));
        old_image = std::string(log.a + 48, size);
        new_image = std::string(log.a + 48 + size, size);
        nextundolsn = *((lsn_t*)(log.a + 48 + 2 * size));
        dirty = 0;
    }
};

extern lsn_t cur_lsn;
extern int logfd;
extern std::map<table_page_t, lsn_t> dirty_pages;
extern pthread_mutex_t log_latch;

logsize_t get_log_size(type_t type, u16_t size = 0);
void flush_logs();
void add_log(mlog_t& log);
mlog_t get_log(lsn_t lsn);
void force();
std::set<int> analyze(FILE* logmsgfp);
void apply_redo(mlog_t log, FILE* logmsgfp);
void redo(int flag, int log_num, FILE* logmsgfp);
void apply_undo(mlog_t log, FILE* logmsgfp, std::priority_queue<lsn_t>& pq);
void undo(int flag, int log_num, FILE* logmsgfp, std::set<int> losers);

#endif