#include <gtest/gtest.h>
#include "yjinbpt.h"
#include <string>

TEST(InsertTest, ascEven50_10000) {
#include <cassert>
#include <string>
#include <stdio.h>
#include <vector>

    puts("START");
    int n = 100000;
    init_db(20000);
    table_t fd = open_table("ascEven50_10000");
    ASSERT_TRUE(fd > 0);
    for (key__t key = -n; key <= n; key += 2) {
        char val[] = "01234567890123456789012345678901234567890123456789";
        int ret = db_insert(fd, key, val, 50);
        ASSERT_FALSE(ret) << "[insert " << key << " / ret = " << ret << "]\n";
        if (key + n / 2 == n / 5) printf("20%\n");
        if (key + n / 2 == n * 2 / 5) printf("40%\n");
        if (key + n / 2 == n * 3 / 5) printf("60%\n");
        if (key + n / 2 == n * 4 / 5) printf("80%\n");
    }
    puts(""); puts(""); puts("");
    std::vector <key__t> v;
    for (key__t i = -n; i <= n; ++i) {
        char ret_val[8000] = { 0, };
        u16_t val_size;

        int ret = db_find(fd, i, ret_val, &val_size);
        if (i >= 0 && i <= n && i % 2 == 0) {
            ASSERT_FALSE(ret) << "[find " << i << " / ret = " << ret << " / val = " << ret_val << " / size = " << val_size << "\n";
            ASSERT_EQ(val_size, 50);
        }
        if (i >= 0 && i <= n && i % 2 == 1) ASSERT_TRUE(ret) << "[find " << i << " / ret = " << ret << " / val = " << ret_val << " / size = " << val_size << "\n";
        if (ret && i % 2 == 0) v.push_back(i);
    }
    puts(""); puts(""); puts(""); puts(""); puts("");
    ASSERT_EQ((int)v.size(), 0);
    printf("[NOT FOUND] -> [%lu]\n", v.size());
    for (auto& it : v) printf("%d ", it); puts("");
    shutdown_db();
    printf("shutted down");
    remove("ascEven50_10000");
}

TEST(InsertTest, descEvenLen112_10000) {
#include <cassert>
#include <string>
#include <stdio.h>
#include <vector>

    puts("START");
    int n = 100000;
    init_db(20000);
    table_t fd = open_table("descEvenLen112_10000");
    ASSERT_TRUE(fd > 0);
    for (key__t key = n; key >= -n; key -= 2) {
        char val[] = "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
        int ret = db_insert(fd, key, val, 112);
        ASSERT_FALSE(ret) << "[insert " << key << " / ret = " << ret << "]\n";
        if (key + n / 2 == 2 * n / 5) printf("20%\n");
        if (key + n / 2 == 2 * n * 2 / 5) printf("40%\n");
        if (key + n / 2 == 2 * n * 3 / 5) printf("60%\n");
        if (key + n / 2 == 2 * n * 4 / 5) printf("80%\n");
    }
    puts(""); puts(""); puts("");
    std::vector <key__t> v;
    for (key__t i = -n; i <= n; ++i) {
        char ret_val[8000] = { 0, };
        u16_t val_size;

        int ret = db_find(fd, i, ret_val, &val_size);
        if (i >= -n && i <= n && i % 2 == 0) {
            ASSERT_FALSE(ret) << "[find " << i << " / ret = " << ret << " / val = " << ret_val << " / size = " << val_size << "\n";
            ASSERT_EQ(val_size, 112);
        }
        if (i >= -n && i <= n && i % 2 == 1) ASSERT_TRUE(ret) << "[find " << i << " / ret = " << ret << " / val = " << ret_val << " / size = " << val_size << "\n";
        if (ret && i % 2 == 0) v.push_back(i);
    }
    puts(""); puts(""); puts(""); puts(""); puts("");
    ASSERT_EQ((int)v.size(), 0);
    printf("[NOT FOUND] -> [%lu]\n", v.size());
    for (auto& it : v) printf("%d ", it); puts("");
    shutdown_db();
    printf("shutted down");
    remove("descEvenLen112_10000");
}


TEST(InsertTest, randomVariableSize10000) {
#include <cassert>
#include <string>
#include <stdio.h>
#include <vector>

    puts("START");
    key__t n = 100000;
    init_db(20000);
    table_t fd = open_table("insertFindTest10000");
    ASSERT_TRUE(fd > 0);
    std::vector<key__t> randomVec;
    for (key__t i = 0; i <= n; ++i) {
        randomVec.push_back(i);
    }
    random_shuffle(randomVec.begin(), randomVec.end());
    for (key__t i : randomVec) {
        std::string s = "____________________________________________________________________________________________________________________________________";
        std::string val = std::to_string(i) + s.substr(50 +(i % 40)) + std::to_string(i);
        int ret = db_insert(fd, i, const_cast<char*>(val.c_str()), val.size());
        ASSERT_FALSE(ret) << "[insert " << i << " / ret = " << ret << "]\n";
        if (i == randomVec[n / 5]) printf("20%\n");
        if (i == randomVec[n * 2 / 5]) printf("40%\n");
        if (i == randomVec[n * 3 / 5]) printf("60%\n");
        if (i == randomVec[n * 4 / 5]) printf("80%\n");
    }
    puts(""); puts(""); puts("");
    std::vector <key__t> v; 
    for (key__t i = -5; i <= n + 5; ++i) {
        char ret_val[8000] = { 0, };
        u16_t val_size;

        int ret = db_find(fd, i, ret_val, &val_size);
        if (i >= 0 && i <= n) ASSERT_FALSE(ret) << "[find " << i << " / ret = " << ret << " / val = " << ret_val << " / size = " << val_size << "\n";
        if (ret) v.push_back(i);
    }
    puts(""); puts(""); puts(""); puts(""); puts("");
    ASSERT_EQ((int)v.size(), 10);
    printf("[NOT FOUND] -> [%lu]\n", v.size());
    for (auto& it : v) printf("%d ", it); puts("");
    shutdown_db();
    printf("shutted down");
    remove("insertFindTest10000");
}