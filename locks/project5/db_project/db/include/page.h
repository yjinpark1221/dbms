// page.h
#ifndef __PAGE_H__
#define __PAGE_H__

#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <vector>
#include <stdio.h>
#include <stdint.h>
#include <vector>
#include <string>
#include <cstring>
#define INITIAL_DB_FILE_SIZE (10 * 1024 * 1024)  // 10 MiB
#define PAGE_SIZE (4 * 1024)                     // 4 KiB

typedef uint64_t pagenum_t;
typedef int64_t table_t;
typedef int64_t key__t;
typedef uint16_t u16_t;
typedef uint32_t u32_t;

struct page_t;
struct slot_t;
struct mslot_t;
struct mnode_t;
struct mleaf_t;
struct minternal_t;

struct page_t {
// in-memory page structure
    char a[PAGE_SIZE];

    page_t(mleaf_t& leaf);
    page_t(minternal_t& internal);
    page_t() {};
};// These definitions are not requirements.


// Add any structures you need

struct slot_t {
    char a[12];
    slot_t(mslot_t* mslot);
};

/// below are processed structures ///

struct mslot_t {
    key__t key;
    u16_t size;
    u16_t offset;
    mslot_t(key__t k, u16_t s, u16_t o);
    mslot_t(slot_t* p);
    bool operator <(mslot_t& s) const;
    bool operator <(key__t k) const;
    mslot_t(key__t k);
    mslot_t();
};
struct mnode_t {
    pagenum_t parent;
    u32_t is_leaf;
    u32_t num_keys;
    mnode_t();
    mnode_t(pagenum_t p, u32_t i, u32_t n);
    mnode_t(page_t& page);
};

struct mleaf_t : public mnode_t {
    // page header
    pagenum_t free_space; // initial = 3968
    pagenum_t right_sibling;

    // page body
    std::vector<mslot_t> slots;
    std::vector<std::string> values;
    
    mleaf_t();
    mleaf_t(page_t& page);
    mleaf_t(pagenum_t p, u32_t i, u32_t n);
    mleaf_t(key__t key, std::string value);
    void push_back(key__t key, std::string value);
    void pop_back();
};

struct minternal_t : public mnode_t {
    // page header 
    pagenum_t first_child;

    // page body
    std::vector<key__t> keys;
    std::vector<pagenum_t> children;
    void push_back(key__t, pagenum_t);
    void pop_back();

    minternal_t(page_t& page);
    minternal_t() {};
};


#endif      // __PAGE_H__