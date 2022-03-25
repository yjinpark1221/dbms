#ifndef __BUF_H__
#define __BUF_H__

#include "file.h"
#include "trx.h"
#include "recovery.h"

#include <pthread.h>
#include <map>

typedef std::pair<table_t, pagenum_t> table_page_t; 
struct ctrl_t;
extern std::map<table_page_t, ctrl_t*> tp2control;
extern pthread_mutex_t buf_latch;
struct ctrl_t {
    table_page_t tp;
    bool is_dirty;
    page_t* frame;
    pthread_mutex_t mutex;
    ctrl_t* next;
    ctrl_t* prev;
    ctrl_t(table_t t, pagenum_t p, page_t* f): tp({t, p}), frame(f) {
        pthread_mutex_init(&mutex, NULL);
    }
    ctrl_t(): is_dirty(0), frame(NULL), next(NULL), prev(NULL) {
        pthread_mutex_init(&mutex, NULL);
    }
    ~ctrl_t() {
        pthread_mutex_destroy(&mutex);
    }
};


int buf_init(int nb);

table_t buf_open_table_file(const char* pathname);
void buf_close_table_file();
ctrl_t* buf_alloc_page(table_t table_id);
void buf_free_page(table_t table_id, pagenum_t pagenum);
ctrl_t* buf_read_page(table_t table_id, pagenum_t pagenum, int trx_id = 0);
void buf_write_page(const page_t* src, ctrl_t* ctrl);

void flush(ctrl_t* pc);
void flush_buf();
ctrl_t* flush_LRU(table_t table_id, pagenum_t pagenum);
void flush_header(table_t table_id);
void read_header(table_t table_id);
void move_to_tail(ctrl_t* ct);

#endif  // __BUF_H__