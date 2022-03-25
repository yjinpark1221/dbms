#ifndef __TRX_H__
#define __TRX_H__

#define EXCLUSIVE 1
#define SHARED 0
#include "yjinbpt.h"
#include <vector>
#include <map>
#include <set>
#include <cassert>
#include <pthread.h>
#include <unordered_map>
#include <stack>

struct lock_entry_t;

struct lock_t {
    lock_t* prev;
    lock_t* next;
    lock_entry_t* sentinel;
    pthread_cond_t condition;
    int lock_mode;
    key__t key;
    int trx_id;
    lock_t* trx_next;
    lock_t() : prev(NULL), next(NULL), sentinel(NULL), lock_mode(-1), trx_next(NULL) {
        pthread_cond_init(&condition, NULL);
    }
    lock_t(table_t table_id, pagenum_t pagenum, key__t key, int trx_id, int lock_mode) : prev(NULL), next(NULL), sentinel(NULL), lock_mode(lock_mode), key(key), trx_id(trx_id), trx_next(NULL) {
        pthread_cond_init(&condition, NULL);
    }
    ~lock_t() {
        pthread_cond_destroy(&condition);
    }
};

struct lock_entry_t {
    table_t table_id;
    pagenum_t pagenum;
    lock_t* head;
    lock_t* tail;
    lock_entry_t() : table_id(0), pagenum(0), head(NULL), tail(NULL) {}
    lock_entry_t(table_t t, pagenum_t p) : table_id(t), pagenum(p), head(NULL), tail(NULL) {}
};

struct log_t {
    table_t table_id;
    pagenum_t pagenum;
    mslot_t slot;
    std::string value;
    log_t() : table_id(0), pagenum(0) {}
    log_t(table_t table_id, pagenum_t pagenum, mslot_t slot, std::string value) : table_id(table_id), pagenum(pagenum), slot(slot), value(value) {}
};

struct trx_entry_t {
    int trx_id;
    lock_t* head;
    lock_t* tail;
    std::set<int> edge;
    std::set<int> rev_edge;
    std::vector<log_t> logs;
    trx_entry_t() : trx_id(0), head(NULL), tail(NULL) {}
    trx_entry_t(int trx_id) : trx_id(trx_id), head(NULL), tail(NULL) {}
};

struct hash_t {
    auto operator() (const std::pair<table_t, pagenum_t>& rec) const {
        return std::hash<int64_t>() (rec.first ^ 0x5555555555555555LL) ^ std::hash<int64_t>()(rec.second);
    }
};

extern pthread_mutex_t trx_latch;
extern pthread_mutex_t lock_latch;
extern std::unordered_map<int, trx_entry_t*> trx_table;
extern std::unordered_map<std::pair<table_t, pagenum_t>, lock_entry_t*, hash_t> lock_table;
extern int transaction_id;

int trx_begin();
int trx_commit(int trx_id);
int trx_abort(int trx_id);
void trx_release_locks(int trx_id);
void lock_release(lock_t* lock);
int trx_undo(int trx_id);
lock_t* lock_acquire(int trx_id, table_t table_id, pagenum_t pagenum, key__t key, int lock_mode, int* ret_slock, int* ret_xlock);
int trx_acquire(int trx_id, table_t table_id, pagenum_t pagenum, key__t key, int lock_mode);
int dfs(int trx_id);


#endif 