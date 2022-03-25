#include <pthread.h>
#include <ctime>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include "yjinbpt.h"
#include "txn.h"
#include <iostream>
#include <string>
using namespace std;
// /*
int64_t tid;

void* trx_read_only(void* arg)
{
    // pthread_mutex_t print_latch = PTHREAD_MUTEX_INITIALIZER;
    int trx_id = trx_begin();

    int64_t key;
    char ret_val[150] = {};
    uint16_t val_size;
    int debug;

    // growing phase
    for (int key = 1; key <= 6; key++)
    {
        key = 1;
        memset(ret_val, 0, sizeof(ret_val));
        debug = db_find(tid, key, ret_val, &val_size, trx_id);
        cout << "read + | id = " << trx_id << ", key = " << key << ", value = " << ret_val << ", debug = " << debug << endl;
    }

    // shrinking phase
    int res = trx_commit(trx_id);
    cout << "read - | id = " << trx_id << ", res = " << res << endl;
    return nullptr;
}


string value = "::my3ngd's-database-value=";

void* trx_write_only(void* arg)
{
    // pthread_mutex_t print_latch = PTHREAD_MUTEX_INITIALIZER;
    int trx_id = trx_begin();

    int64_t key;
    uint16_t old_val_size;
    string val = value;
    uint16_t val_size;
    int debug;

    // growing phase
    for (int key = 1; key <= 6; key++)
    {
        val = value;
        while (val.size() < 50) val += std::to_string(key);
        debug = db_update(tid, key, const_cast<char*>(val.c_str()), val_size, &old_val_size, trx_id);
        cout << "write + | id = " << trx_id << ", key = " << key;
    }

    // shrinking phase
    int res = trx_commit(trx_id);
    cout << "write - | id = " << trx_id << ", res = " << res << endl;
    return nullptr;
}







int main(int argc, char const *argv[])
{
    init_db(100000);
    tid = open_table(const_cast<char*>("my3ngd.db"));

    for (int i = 0; i < 10; i++)
    {
        string str = "::my3ngd_database::";
        for (int j = 0; j < 50; j++)
            str += std::to_string(i+1);

        db_insert(tid, i+1, const_cast<char*>(str.c_str()), str.length());
    }


    // test start

    for (int i = 0; i < 10; i++)
    {
        char ret_val[150] = {};
        uint16_t val_size;
        cout << "find key: " << i+1 << "\t: " << db_find(tid, i+1, ret_val, &val_size) << endl;
    }

    for (int i = 0; i < 10; i++)
    {
        cout << i+1 << endl;
        pthread_t trx1, trx2, trx3;
        pthread_create(&trx1, NULL, trx_read_only, NULL);
        // pthread_create(&trx2, NULL, trx_read_only, NULL);
        // pthread_create(&trx3, NULL, trx_read_only, NULL);
        break;

        pthread_join(trx1, NULL);
        // pthread_join(trx2, NULL);
        // pthread_join(trx3, NULL);
    }

    shutdown_db();
    return 0;
}
// */

