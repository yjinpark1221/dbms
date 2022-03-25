#define MAINTEST
// #include "../db/include/file.h"
// #include "../db/include/page.h"
// #include "../db/include/buffer.h"
#include "../db/include/yjinbpt.h"
#include "../db/include/trx.h"
#include "../db/include/recovery.h"
#include "../db/src/file.cc"
#include "../db/src/page.cc"
#include "../db/src/buffer.cc"
#include "../db/src/yjinbpt.cc"
#include "../db/src/trx.cc"
#include "../db/src/recovery.cc"

#include <string>
#include <cassert>
#include <string>
#include <stdio.h>
#include <vector>
#include <set>
int main() {
    init_db(20000, 0, 100, "log.data", "logmsg.txt");
    int fd = open_table("100000.db");
    for (int i = -50000; i < 50000; ++i) {
        char val[] = "123456789012345678901234567890123456789012345678901234567890";
        printf("inserting %d\n", i);
        int ret = db_insert(fd, i, val, 50);

        // char f[10000];
        // u16_t size;
        // if (db_find(fd, i, f, &size) == 1) printf("?");
    }
    shutdown_db();
    return 0;
}