#include "recovery.h"

int logfd;
std::map<lsn_t, mlog_t> logs;
pthread_mutex_t log_latch;
std::map<table_page_t, lsn_t> dirty_pages;
lsn_t cur_lsn;

log_t::log_t(mlog_t log) {
        *((logsize_t*)a) = log.size;
        *((lsn_t*)(a + 4)) = log.lsn;
        *((lsn_t*)(a + 12)) = log.prevlsn;
        *((u32_t*)(a + 20)) = log.trx_id;
        *((type_t*)(a + 24)) = log.type;
        *((table_t*)(a + 28)) = log.table_id;
        *((pagenum_t*)(a + 36)) = log.pagenum;
        *((u16_t*)(a + 44)) = log.offset;
        *((u16_t*)(a + 46)) = log.size;
        memcpy(a + 48, log.old_image.c_str(), log.size);
        memcpy(a + 48 + log.size, log.new_image.c_str(), log.size);
        *((lsn_t*)(a + 48 + 2 * log.size)) = log.nextundolsn;
}

log_t::log_t() : log_t(mlog_t()) {}

// no latch
logsize_t get_log_size(type_t type, u16_t size) {
    if (type == BEGIN || type == COMMIT || type == ROLLBACK) {
        return 28;
    }
    else if (type == UPDATE) {
        return 48 + size;
    }
    else if (type == COMPENSATE) {
        return 56 + 2 * size;
    }
    else {
        return 0;
    }
}

// needs latch
void flush_logs() {
    for (auto& log: logs) {
        log_t l(log.second);
        pwrite(logfd, &log, log.second.log_size, log.second.lsn);
        log.second.dirty = 0;
    }
}

// needs latch
void add_log(mlog_t& log) {
    if (logs.size() == 100000) {
        flush_logs();
        logs.clear();
    }
    if (trx_table[log.trx_id] == NULL) trx_table[log.trx_id] = new trx_entry_t(log.trx_id); 
    trx_table[log.trx_id]->lastlsn = log.lsn;
    logs[log.lsn] = log;
    cur_lsn += log.size;
}

// needs latch
mlog_t get_log(lsn_t lsn) {
    auto iter = logs.find(lsn);
    if (iter != logs.end()) {
        return iter->second;
    }
    log_t log;
    pread(logfd, &log, sizeof(log_t), lsn);
    mlog_t mlog(log);
    if (mlog.type != 5) add_log(mlog);
    return mlog;
}

// no latch, do alone
void force() {
    pthread_mutex_lock(&buf_latch);
    flush_buf();
    pthread_mutex_unlock(&buf_latch);
    
    pthread_mutex_lock(&log_latch);
    flush_logs();
    pthread_mutex_unlock(&log_latch);
}

// no latch
std::set<int> analyze(FILE* logmsgfp) {
    pthread_mutex_init(&log_latch, NULL);
    pthread_mutex_lock(&log_latch);
    fprintf(logmsgfp, "[ANALYSIS] Analysis pass start\n");

    std::set<int> winners, losers;
    int offset = 0;
    for (mlog_t log = get_log(0); log.type != 5; log = get_log(offset)) {
        offset += log.log_size;

        if (log.type == BEGIN) {
            losers.insert(log.trx_id);
        }
        else if (log.type == COMMIT || log.type == ROLLBACK) {
            assert(losers.find(log.trx_id) != losers.end());
            losers.erase(log.trx_id);
            winners.insert(log.trx_id);
        }
    }

    fprintf(logmsgfp, "[ANALYSIS] Analysis success. Winner :");
    for (auto win : winners) {
        fprintf(logmsgfp, " %d", win);
    }
    fprintf(logmsgfp, ", Loser:");
    for (auto lose : losers) {
        fprintf(logmsgfp, " %d", lose);
    }
    fprintf(logmsgfp, "\n");
    pthread_mutex_unlock(&log_latch);
    return losers;
}

void apply_redo(mlog_t log, FILE* logmsgfp) {
    if (log.type == BEGIN) {
        fprintf(logmsgfp, "LSN %lu [BEGIN] Transaction id %d\n", log.lsn, log.trx_id);
        if (trx_table[log.trx_id] == NULL) trx_table[log.trx_id] = new trx_entry_t(log.trx_id);
        trx_table[log.trx_id]->lastlsn = log.lsn;
        trx_table[log.trx_id]->status = RUNNING;
    }
    
    else if (log.type == UPDATE) {
        ctrl_t* ctrl = buf_read_page(log.table_id, log.pagenum);
        page_t page = *ctrl->frame;
        lsn_t pagelsn = *(lsn_t*)(page.a + 24);
        trx_table[log.trx_id]->lastlsn = log.lsn;
        if (pagelsn >= log.lsn) {
            fprintf(logmsgfp, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", log.lsn, log.trx_id);
            pthread_mutex_unlock(&(ctrl->mutex));
        }
        else {
            fprintf(logmsgfp, "LSN %lu [UPDATE] Transaction id %d redo apply\n", log.lsn, log.trx_id);
            *(lsn_t*)(page.a + 24) = log.lsn;
            memcpy(page.a + log.offset, log.new_image.c_str(), log.size);
            buf_write_page(&page, ctrl);
            pthread_mutex_unlock(&(ctrl->mutex));
        }
    }
    else if (log.type == COMMIT) {
        trx_table[log.trx_id]->status = COMMITTED;
        trx_table[log.trx_id]->lastlsn = log.lsn;
        fprintf(logmsgfp, "LSN %lu [COMMIT] Transaction id %d\n", log.lsn, log.trx_id);
    }
    else if (log.type == ROLLBACK) {
        trx_table[log.trx_id]->status = ABORTED;
        trx_table[log.trx_id]->lastlsn = log.lsn;
        fprintf(logmsgfp, "LSN %lu [ROLLBACK] Transaction id %d\n", log.lsn, log.trx_id);
    }
    else if (log.type == COMPENSATE) {
        ctrl_t* ctrl = buf_read_page(log.table_id, log.pagenum);
        page_t page = *ctrl->frame;
        lsn_t pagelsn = *(lsn_t*)(page.a + 24);

        trx_table[log.trx_id]->lastlsn = log.lsn;
        if (pagelsn >= log.lsn) {
            fprintf(logmsgfp, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", log.lsn, log.trx_id);
            pthread_mutex_unlock(&(ctrl->mutex));
        }
        else {
            fprintf(logmsgfp, "LSN %lu [CLR] next undo lsn %lu\n", log.lsn, log.nextundolsn);
            *(lsn_t*)(page.a + 24) = log.lsn;
            memcpy(page.a + log.offset, log.new_image.c_str(), log.size);
            buf_write_page(&page, ctrl);
            pthread_mutex_unlock(&(ctrl->mutex));
        }
    }
}

// no latch
void redo(int flag, int log_num, FILE* logmsgfp) {
    pthread_mutex_lock(&log_latch);
    fprintf(logmsgfp, "[REDO] Redo pass start\n");

    int offset = 0, cnt = 0;
    for (mlog_t log = get_log(0); log.type != 5; log = get_log(offset)) {
        if (flag == 1 && cnt == log_num) return;
        ++cnt;
        offset += log.log_size;
        apply_redo(log, logmsgfp);
    }

    fprintf(logmsgfp, "[REDO] Redo pass end\n");
    pthread_mutex_unlock(&log_latch);
}

// needs latch
void apply_undo(mlog_t log, FILE* logmsgfp, std::priority_queue<lsn_t>& pq) {
    if (log.type == BEGIN) {
        if (logmsgfp) fprintf(logmsgfp, "LSN %lu [ROLLBACK] Transaction id %d\n", log.lsn, log.trx_id);
        trx_table[log.trx_id]->lastlsn = cur_lsn;
        mlog_t newlog(get_log_size(ROLLBACK), cur_lsn, cur_lsn, log.trx_id, ROLLBACK);
        add_log(newlog);
        trx_table[log.trx_id]->status = ABORTED;
        return;
    }
    else if (log.type == COMMIT || log.type == ROLLBACK) {
        perror("in apply_undo, invalid log type");
        exit(0);
    }
    else if (log.type == UPDATE) {
        int clrlsn = cur_lsn;
        mlog_t newlog(get_log_size(COMPENSATE, log.size), clrlsn, trx_table[log.trx_id]->lastlsn, log.trx_id, COMPENSATE, log.table_id, log.pagenum, log.offset, log.size, log.new_image, log.old_image);
        newlog.nextundolsn = log.prevlsn;
        add_log(newlog);

        ctrl_t* ctrl = buf_read_page(log.table_id, log.pagenum);
        page_t page = *ctrl->frame;
        lsn_t pagelsn = *(lsn_t*)(page.a + 24); 
        if (logmsgfp) fprintf(logmsgfp, "LSN %lu [UPDATE] Transaction id %d undo apply\n", log.lsn, log.trx_id);
        *(lsn_t*)(page.a + 24) = clrlsn;
        memcpy(page.a + log.offset, log.old_image.c_str(), log.size);
        buf_write_page(&page, ctrl);
        pthread_mutex_unlock(&(ctrl->mutex));
        trx_table[log.trx_id]->lastlsn = clrlsn;

        if (log.prevlsn != log.lsn) pq.push(log.prevlsn);
    }
    else if (log.type == COMPENSATE) {
        if (log.nextundolsn != log.lsn) pq.push(log.nextundolsn);
    }
}

// no latch
void undo(int flag, int log_num, FILE* logmsgfp, std::set<int> losers) {
    pthread_mutex_lock(&log_latch);
    fprintf(logmsgfp, "[UNDO] Undo pass start\n");
    int cnt = 0;
    std::priority_queue<lsn_t> pq;
    for (auto lose : losers) {
        pq.push(trx_table[lose]->lastlsn);
    }
    for (; !pq.empty(); ++cnt) {
        if (flag == 2 && log_num == cnt) return;

        auto lsn = pq.top();
        pq.pop();
        apply_undo(get_log(lsn), logmsgfp, pq);
    }
    for (auto& trx : trx_table) {
        assert(trx.second->status != RUNNING);
    }
    fprintf(logmsgfp, "[UNDO] Undo pass end\n");
    pthread_mutex_unlock(&log_latch);
}
