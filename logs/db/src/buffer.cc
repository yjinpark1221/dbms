#include "buffer.h"
#include "recovery.h"

#include <cassert>

pthread_mutex_t buf_latch;
std::map<table_page_t, ctrl_t*> tp2control;
ctrl_t* control;
page_t* cache;
ctrl_t* hcontrol;
page_t* hcache;
int num_buf;
int cur_buf;

static ctrl_t head, tail;

int buf_init(int nb) {
    head.next = &tail;
    tail.prev = &head;
    head.prev = NULL;
    tail.next = NULL;
    cur_buf = 0;
    pthread_mutex_init(&buf_latch, NULL);

    num_buf = nb;
    cache = (page_t*) malloc(sizeof(page_t) * num_buf);
    control = new ctrl_t[num_buf];
    hcontrol = new ctrl_t[20];
    hcache = (page_t*) malloc(sizeof(page_t) * 20);
    
    if (cache == NULL || control == NULL || hcontrol == NULL || hcache == NULL) {
        perror("in buf_init malloc error");
        exit(0);
    }
    
    return 0;
}

table_t buf_open_table_file(const char* pathname) {
    pthread_mutex_lock(&buf_latch);
    table_t table_id = file_open_table_file(pathname);
    page_t hp;
    file_read_page(table_id, 0, &hp);
    hcache[openedFds.size() - 1] = hp;

    ctrl_t hc(table_id, 0, hcache + (int)(openedFds.size() - 1));
    hcontrol[openedFds.size() - 1].tp = {table_id, 0};
    hcontrol[openedFds.size() - 1].frame = hcache + (int)(openedFds.size() - 1);
    hcontrol[openedFds.size() - 1].next = hcontrol[openedFds.size() - 1].prev = NULL;
    hcontrol[openedFds.size() - 1].is_dirty = 0;
    pthread_mutex_unlock(&buf_latch);
    return table_id;
}

void buf_close_table_file() {
    // flush logs
    pthread_mutex_lock(&log_latch);
    flush_logs();
    pthread_mutex_unlock(&log_latch);

    // flush headers 
    pthread_mutex_lock(&buf_latch);
    for (int i = 0; i < openedFds.size(); ++i) {
        flush(hcontrol + i);
    }
    delete[] hcontrol;
    // flush frames
    for (int i = 0; i < cur_buf; ++i) {
        flush(control + i);
    }
    delete[] control;
    pthread_mutex_unlock(&buf_latch);
    pthread_mutex_destroy(&buf_latch);
    pthread_mutex_destroy(&log_latch);
    free(cache);
    free(hcache);
    
    // close files
    file_close_table_file();
}

ctrl_t* buf_alloc_page(table_t table_id) {
    pthread_mutex_lock(&buf_latch);
    flush_header(table_id);
    pagenum_t pn = file_alloc_page(table_id);
    read_header(table_id);
    pthread_mutex_unlock(&buf_latch);
    ctrl_t* ctrl = buf_read_page(table_id, pn);

    return ctrl;
}

void buf_free_page(table_t table_id, pagenum_t pagenum) {
    pthread_mutex_lock(&buf_latch);
    flush_header(table_id);
    file_free_page(table_id, pagenum);
    read_header(table_id);
    pthread_mutex_unlock(&buf_latch);
}

ctrl_t* buf_read_page(table_t table_id, pagenum_t pagenum, int trx_id ) {
    pthread_mutex_lock(&buf_latch);
    // reading header page -> must be in hcontrol block
    if (pagenum == 0) {
        for (int i = 0; i < openedFds.size(); ++i) {
            ctrl_t* hc = hcontrol + i;
            if (hc->tp.first == table_id) {
                pthread_mutex_lock(&(hc->mutex));
                pthread_mutex_unlock(&buf_latch);
                return hc;
            }
        }
        perror("in buf_read_page header page not in hcontrol");
        exit(0);
    }

    auto iter = tp2control.find({table_id, pagenum});
    ctrl_t* ct;

    // not in cache
    if (iter == tp2control.end()) {
        // LRU flush
        ct = flush_LRU(table_id, pagenum);
        tp2control[{table_id, pagenum}] = ct;
        // disk to buf
        file_read_page(table_id, pagenum, ct->frame);
    }
    // in cache
    else {
        ct = iter->second;        
    }
    move_to_tail(ct);
    pthread_mutex_lock(&(ct->mutex));
    pthread_mutex_unlock(&buf_latch);
    return ct;
}

void buf_write_page(const page_t* src, ctrl_t* ctrl) {
    ctrl_t* ct = ctrl;
    memcpy(ct->frame, src, PAGE_SIZE);
    ct->is_dirty = 1;

    // TODO : need to insert into dirty table !!! 
    // dirty_pages.erase(dirty_pages.find(ctrl->tp));
    return;
}

ctrl_t* flush_LRU(table_t table_id, pagenum_t pagenum) {
    // case : buffer not full 
    // -> no need to flush 
    // -> control[end] = {table_id, pagenum}
    if (cur_buf < num_buf) {
        control[cur_buf].tp = {table_id, pagenum};
        control[cur_buf].is_dirty = 0;
        control[cur_buf].frame = cache + cur_buf;
        control[cur_buf].next = NULL;
        control[cur_buf].prev = NULL;
        tp2control[{table_id, pagenum}] = control + cur_buf;
        ++cur_buf;
        return control + cur_buf - 1;
    }
    ctrl_t* ct;
    for (ct = head.next; ; ct = ct->next) {
        if (ct == &tail) ct = head.next;
        if (pthread_mutex_trylock(&(ct->mutex)) == EBUSY) continue;
        else {
            break;
        }
    }
    auto iter = tp2control.find(ct->tp);
    if (iter == tp2control.end()) {
        perror("in flush_LRU iter not found");
        exit(0);
    }
    tp2control.erase(iter);
    flush_logs();
    flush(ct);
    ct->tp = {table_id, pagenum};
    tp2control[{table_id, pagenum}] = ct;
    ct->is_dirty = 0;

    pthread_mutex_unlock(&ct->mutex);
    return ct;
}

// This function writes the frame to disk if it is dirty
// called flushing the head.next
void flush(ctrl_t* ctrl) {
    if (ctrl->is_dirty) {
        file_write_page(ctrl->tp.first, ctrl->tp.second, ctrl->frame);
        ctrl->is_dirty = 0;
        // dirty_pages.erase(dirty_pages.find(ctrl->tp));
    }
}

// This function flushes the header page of the table_id 
void flush_header(table_t table_id) {
    for (int i = 0; i < openedFds.size(); ++i) {
        ctrl_t* hc = hcontrol + i;
        if (hc->tp.first == 0) return;
        if (hc->tp.first == table_id) {
            file_write_page(table_id, 0, hc->frame);
            return;
        }
    }
}

// This function flushes the header page of the table_id 
void read_header(table_t table_id) {
    for (int i = 0; i < openedFds.size(); ++i) {
        ctrl_t* hc = hcontrol + i;
        if (hc->tp.first == 0) return;
        if (hc->tp.first == table_id) {
            file_read_page(table_id, 0, hc->frame);
        }
    }
}

// This function moves ct to the tail
// called when referenced
void move_to_tail(ctrl_t* ct) {
    ctrl_t* prev = ct->prev, *next = ct->next, *last = tail.prev;
    // case : no ctrl block in the list
    if (head.next == &tail) {
        assert(prev == NULL); 
        assert(next == NULL);
        assert(last == &head);
        head.next = ct;
        tail.prev = ct;
        ct->prev = &head;
        ct->next = &tail;
        return;
    }

    // case : only one ctrl block // or it is the last one
    if (last == ct) {
        assert(ct->next == &tail);
        return;
    }

    if (prev) {
        assert(next);
        prev->next = next;
        next->prev = prev;
    }

    last->next = ct;
    ct->prev = last;

    ct->next = &tail;
    tail.prev = ct;
}

void flush_buf() {
    for (int i = 0; i < cur_buf; ++i) {
        flush(control + i);
    }
}