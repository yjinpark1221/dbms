#include <gtest/gtest.h>
#include "yjinbpt.h"
#include "recovery.h"
#include "trx.h"
#include <string>

TEST(InsertTest, ascEven50_10000) {
#include <cassert>
#include <string>
#include <stdio.h>
#include <vector>

    puts("START");
    int n = 1000;
    init_db(20000, 0, 100, "log.data", "logmsg.txt");
    table_t fd = open_table("DATA1");
    ASSERT_TRUE(fd > 0);
    for (int i = 1; i <= n; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Inserting key = " << i << std::endl;
        #endif
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
        int res = db_insert(fd, i, const_cast<char*>(data.c_str()), data.length());
        EXPECT_EQ(res, 0);
    }
    puts(""); puts(""); puts("");
    EXPECT_EQ(shutdown_db(), 0);


    init_db(20000, 0, 100, "log.data", "logmsg.txt");
    fd = open_table("DATA1");
    int trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);
    std::cout << "[INFO] Trx successfully begun, trx_id = " << trx_id << std::endl;

    int err = 0;
    uint16_t val_size;
    for (int i = 1; i <= n; i++) {
        char ret_val[112];
        int res = db_find(fd, i, ret_val, &val_size, trx_id);
        EXPECT_EQ(res, 0);
    }

    std::cout << "[INFO] Successfully found all " << n << " records." << std::endl;

    trx_commit(trx_id);

    std::cout << "[INFO] Trx successfully committed." << std::endl;

    trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    std::cout << "[INFO] Trx successfully begun, trx_id = " << trx_id << std::endl;

    for (int i = 1; i <= n; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Updating key = " << i << std::endl;
        #endif
        std::string data = "12345678901234567890123456789012345678901234567890" + std::to_string(i);
        uint16_t old_val_size = 0;
        int res = db_update(fd, i, const_cast<char*>(data.c_str()), data.length(), &old_val_size, trx_id);
        EXPECT_EQ(res, 0);
        char buffer[120];
        res = db_find(fd, i, buffer, &old_val_size, trx_id);
        EXPECT_EQ(res, 0);
        for (int j = 0; j < old_val_size; j++) {
            EXPECT_EQ(buffer[j], data[j]);
        }
    }
    trx_abort(trx_id);


}
