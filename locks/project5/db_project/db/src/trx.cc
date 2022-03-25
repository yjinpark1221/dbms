#include "trx.h"

pthread_mutex_t trx_latch;
pthread_mutex_t lock_latch;
std::unordered_map<int, trx_entry_t*> trx_table;
std::unordered_map<std::pair<table_t, pagenum_t>, lock_entry_t*, hash_t> lock_table;
int transaction_id;

int trx_begin() {
    pthread_mutex_lock(&trx_latch);
    trx_entry_t* trx = new trx_entry_t(transaction_id);
    printf("trx_begin trx_id %d\n", transaction_id);
    assert(trx);
    trx_table[transaction_id] = trx;
    int ret = transaction_id;
    ++transaction_id;
    pthread_mutex_unlock(&trx_latch);
    return ret;
}

int trx_commit(int trx_id) {
    printf("commit trx_id %d\n", trx_id);
    pthread_mutex_lock(&trx_latch);
    trx_release_locks(trx_id);
    trx_entry_t* trx = trx_table[trx_id];
    trx_table.erase(trx_table.find(trx_id));
    delete trx;
    pthread_mutex_unlock(&trx_latch);
    return 0;
}

int trx_abort(int trx_id) {
    printf("abort trx_id %d\n", trx_id);
    trx_undo(trx_id);
    trx_release_locks(trx_id);
    trx_entry_t* trx = trx_table[trx_id];
    trx_table.erase(trx_table.find(trx_id));
    delete trx;
    return 0;
}

void trx_release_locks(int trx_id) {
    pthread_mutex_lock(&lock_latch);
    assert(trx_table[trx_id]);
    lock_t* lock = trx_table[trx_id]->head;
    lock_t* next;
    for (; lock; lock = next) {
        next = lock->trx_next;
        lock_release(lock);
    }
    pthread_mutex_unlock(&lock_latch);
}

// remove lock from the lock list, signal successors, free lock
void lock_release(lock_t* lock) {

    lock_entry_t* lentry = lock->sentinel;
    
    // remove lock from lock list
    lock_t* prev = lock->prev;
    lock_t* next = lock->next;
    if (lentry->head == lock) {
        lentry->head = lock->next;
    }
    if (lentry->tail == lock) {
        lentry->tail = lock->prev;
    }
    if (prev) {
        prev->next = next;
    }
    if (next) {
        next->prev = prev;
    }

    // signal successors
    int slock_cnt = 0, slock_trx_id = 0;
    for (lock_t* l = lentry->head; l; l = l->next) {
        if (l->key != lock->key) continue;
        if (l->lock_mode == SHARED) {
            pthread_cond_signal(&l->condition);
            ++slock_cnt;
            slock_trx_id = l->trx_id;
        }
        else {
            if (slock_cnt == 0) {
                pthread_cond_signal(&l->condition);
                break;
            }
            else if (slock_cnt == 1 && slock_trx_id == l->trx_id) {
                pthread_cond_signal(&l->condition);
                break;
            }
            else {
                break;
            }
        }
    }

    // free lock
    delete lock;

}

// roll back
int trx_undo(int trx_id) {
    auto& log = trx_table[trx_id]->logs;
    for (int i = (int)log.size() - 1; i >= 0; --i) {
        auto& entry = log[i];
        table_t table_id = entry.table_id;
        pagenum_t pagenum = entry.pagenum;
        mslot_t slot = entry.slot;
        std::string value = entry.value;

        ctrl_t* ctrl = buf_read_page(table_id, pagenum);
        page_t page = *(ctrl->frame);
        memcpy(page.a + slot.offset, value.c_str(),  slot.size);
        buf_write_page(&page, ctrl);
        pthread_mutex_unlock(&ctrl->mutex);
    }
    return 0;
}

// check if the transaction has the lock
// otherwise, add new lock to the lock list
lock_t* lock_acquire(int trx_id, table_t table_id, pagenum_t pagenum, key__t key, int lock_mode, int* ret_slock, int* ret_xlock) {
    pthread_mutex_lock(&lock_latch);
    lock_entry_t* entry = lock_table[{table_id, pagenum}];
    if (entry == NULL) {
        entry = new lock_entry_t(table_id, pagenum);
        lock_table[{table_id, pagenum}] = entry;
    }
    lock_t* slock_tmp = NULL;
    int this_trx_slock = 0, this_trx_xlock = 0, other_trx_slock = 0, other_trx_xlock = 0;
    for (lock_t* lock = entry->tail; lock; lock = lock->prev) {
        if (lock->key != key) {
            continue;
        }
        if (lock->trx_id == trx_id) {
            if (lock->lock_mode == SHARED) {
                ++this_trx_slock;
                slock_tmp = lock;
            }
            else {
                ++this_trx_xlock;
            }
        }
        else {
            if (lock->lock_mode == SHARED) {
                ++other_trx_slock;
            }
            else {
                ++other_trx_xlock;
            }
        }
    }
    assert(this_trx_slock <= 1);
    assert(this_trx_xlock <= 1);
    assert(other_trx_slock >= 0);
    assert(other_trx_xlock >= 0);
    if  (lock_mode == SHARED) {
        if (this_trx_slock || this_trx_xlock) {
            pthread_mutex_unlock(&lock_latch);
            return NULL;
        }
    }
    else {
        if (this_trx_xlock) {
            pthread_mutex_unlock(&lock_latch);
            return NULL;
        }
        else if (this_trx_slock) {
            if (other_trx_slock == 0 && other_trx_xlock == 0) {
                assert(slock_tmp);
                slock_tmp->lock_mode = EXCLUSIVE;
                pthread_mutex_unlock(&lock_latch);
                return NULL;
            }
        }
    }
    
    lock_t* lock = new lock_t(table_id, pagenum, key, trx_id, lock_mode);
    lock->sentinel = entry;
    lock->prev = entry->tail;
    lock->next = NULL;
    
    // add to lock list
    if (entry->tail) {
        entry->tail->next = lock;
    }
    entry->tail = lock;
    if (entry->head == NULL) {
        entry->head = lock;
    }
    
    *ret_slock = other_trx_slock;
    *ret_xlock = other_trx_xlock;

    pthread_mutex_unlock(&lock_latch);
    return lock;
}

// lock acquiring API
int trx_acquire(int trx_id, table_t table_id, pagenum_t pagenum, key__t key, int lock_mode) {
    int has_slock = 0, has_xlock = 0;
    lock_t* lock = lock_acquire(trx_id, table_id, pagenum, key, lock_mode, &has_slock, &has_xlock);
    // case : the transaction already has the lock
    if (lock == NULL) {
        return 0;
    }

    pthread_mutex_lock(&trx_latch);

    // add to trx list
    trx_entry_t* trx = trx_table[trx_id];
    printf("trx_acquire trx_id %d\n", trx_id);
    assert(trx);

    if (trx->tail) {
        trx->tail->trx_next = lock;
    }
    trx->tail = lock;
    if (trx->head == NULL) {
        trx->head = lock;
    }
    lock->trx_next = NULL;

    // add wait-for edge
    pthread_mutex_lock(&lock_latch);
    if (lock_mode == SHARED) {
        for (lock_t* l = lock->prev; l; l = l->prev) {
            if (l->key == key && l->lock_mode == EXCLUSIVE) {
                trx->edge.insert(l->trx_id);
                break;
            }
        }
    }
    else {
        for (lock_t* l = lock->prev; l; l = l->prev) {
            if (l->key == key && l->lock_mode == SHARED) {
                trx->edge.insert(l->trx_id);
            }
            else if (l->key == key && l->lock_mode == EXCLUSIVE) {
                trx->edge.insert(l->trx_id);
                break;
            }
        }
    }
    pthread_mutex_unlock(&lock_latch);
    
    // case : deadlock -> transaction abort
    if (dfs(trx_id)) {
        trx_abort(trx_id);
        pthread_mutex_unlock(&trx_latch);
        return 1;
    }
    
    // wait for other trx to release lock
    if (lock_mode == SHARED) {
        if (has_xlock) {
            pthread_cond_wait(&lock->condition, &trx_latch);
        }
    }
    else {
        if (has_slock || has_xlock) {
            pthread_cond_wait(&lock->condition, &trx_latch);
        }
    }

    // done waiting -> acquire
    pthread_mutex_unlock(&trx_latch);
    return 0;
}

// deadlock detection
// int dfs(int trx_id) {
//     std::map<int, bool> visited;
//     std::stack<std::pair<int, int> > st;
//     for (int ed : trx_table[trx_id]->edge) {
//         st.push({trx_id, ed});
//         assert(ed != trx_id);
//     }
//     visited[trx_id] = 1;
//     for (; !st.empty(); ) {
//         int a = st.top().first;
//         int b = st.top().second;
//         st.pop();
//         if (b == trx_id) {
//             return 1;
//         }
//         auto it = trx_table.find(b);
//         if (it == trx_table.end() || it->second == NULL){
//             trx_table[a]->edge.erase(b);
//         }
//         if (visited[b]) {
//             continue;
//         }
//         visited[b] = 1;
//         for (int ed : trx_table[b]->edge) {
//             st.push({b, ed});
//         }
//     }
//     return 0;
// }

int dfs(int trx_id) {
    std::stack<std::pair<int, int> > q;
    std::set<int> visited;
    visited.insert(trx_id);

    for (auto edge: trx_table[trx_id]->edge) {
        q.push({trx_id, edge});
    }

    for (; !q.empty();) {
        // printf("%d q.size() %d\n", trx_id, q.size());
        auto fr = q.top();
        q.pop();
        // a is waiting for **b**
        int a = fr.first;
        int b = fr.second;
        if (b == trx_id) {
            printf("cycle 1\n");
            return 1;
        }
        // trx end -> edge remove!
        // when b is aborted or committed
        if (trx_table.find(b) == trx_table.end()) {
            trx_table[a]->edge.erase(b);
            continue;
        }
        if (visited.find(b) != visited.end()) continue;
        visited.insert(b);
        for (auto edge : trx_table[b]->edge) {
            q.push({b, edge});
        }
    }
    printf("cycle 0\n");
    return 0;
}
