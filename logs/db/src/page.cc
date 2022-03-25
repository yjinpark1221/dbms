#include "page.h"

#define VERBOSE 1
page_t::page_t(mleaf_t& leaf) {
    if (leaf.num_keys != leaf.slots.size() || leaf.num_keys != leaf.values.size()) {
        perror("leaf to page_t num_key different");
        exit(0);
    }
    memset(a, 0, 4096);
    ((pagenum_t*)a)[0 / 8] = leaf.parent;
    ((u32_t*)a)[8 / 4] = 1;
    ((u32_t*)a)[12 / 4] = leaf.num_keys;
    ((pagenum_t*)a)[112 / 8] = leaf.free_space;
    ((pagenum_t*)a)[120 / 8] = leaf.right_sibling;
    for (u32_t i = 0; i < leaf.num_keys; ++i) {
        slot_t slot = &(leaf.slots[i]);
        ((slot_t*)(a + 128))[i] = slot;
        for (int j = 0; j < leaf.slots[i].size; ++j) {
            *(a + leaf.slots[i].offset + j) = leaf.values[i][j];
        }
    }
}
page_t::page_t(minternal_t& node) {
    if (node.num_keys != node.keys.size() || node.num_keys != node.children.size()) {
        perror("internal to page_t num_key different");
        exit(0);
    }
    memset(a, 0, 4096);
    ((pagenum_t*)a)[0 / 8] = node.parent;
    ((u32_t*)a)[8 / 4] = 0;
    ((u32_t*)a)[12 / 4] = node.num_keys;
    ((pagenum_t*)a)[120 / 8] = node.first_child;
    for (int i = 0; i < node.num_keys; ++i) {
        ((key__t*)a)[128 / 8 + i * 2] = node.keys[i];
        ((pagenum_t*)a)[128 / 8 + i * 2 + 1] = node.children[i];
    }
}
// Add any structures you need

slot_t::slot_t(mslot_t* mslot) {
    ((key__t*)a)[0] = mslot->key;
    ((u16_t*)a)[8 / 2] = mslot->size;
    ((u16_t*)a)[10 / 2] = mslot->offset;
}

/// below are processed structures ///

mslot_t::mslot_t(key__t k, u16_t s, u16_t o) {
    key = k;
    size = s;
    offset = o;
}
mslot_t::mslot_t(slot_t* p) {
    key = ((key__t*)p)[0];
    size = ((u16_t*)p)[8 / 2];
    offset = ((u16_t*)p)[10 / 2];
}
bool mslot_t::operator < (mslot_t& s) const {
    return this->key < s.key;
}
bool mslot_t::operator < (key__t k) const {
    return this->key < k;
}
mslot_t::mslot_t() {}
mslot_t::mslot_t(key__t k) {
    key = k;
}

mnode_t::mnode_t() {
    parent = 0;
    is_leaf = 0;
    num_keys = 0;
    pagelsn = 0;
}
mnode_t::mnode_t(pagenum_t p, u32_t i, u32_t n) {
    parent = p;
    is_leaf = i;
    num_keys = n;
    pagelsn = 0;
}
mnode_t::mnode_t(page_t& page) {
    parent = ((pagenum_t*)page.a)[0 / 8];
    is_leaf = ((u32_t*)page.a)[8 / 4];
    num_keys = ((u32_t*)page.a)[12 / 4];
    pagelsn = ((lsn_t*)page.a)[24 / 8];
}

mleaf_t::mleaf_t() :mnode_t(0, 1, 0) {
    free_space = 3968;
    right_sibling = 0;
}
mleaf_t::mleaf_t(page_t& page) : mnode_t(page) {
    is_leaf = 1;
    free_space = ((pagenum_t*)page.a)[112 / 8];
    right_sibling = ((pagenum_t*)page.a)[120 / 8];
    for (u32_t i = 0; i < num_keys; ++i) {
        mslot_t slot((slot_t*)(page.a + 128) + i);
        std::string val;
        for (int j = 0; j < slot.size; ++j) {
            val.push_back(*(page.a + slot.offset + j)); 
        }
        slots.push_back(slot);
        values.push_back(val);
    }
}
mleaf_t::mleaf_t(pagenum_t p, u32_t i, u32_t n) : mnode_t(p, i, n) {
    is_leaf = 1;
    free_space = 3968;
    right_sibling = 0;
}
mleaf_t::mleaf_t(key__t key, std::string value) : mnode_t(0, 1, 1) {
    free_space = 3968 - value.size() - 12;
    slots.push_back({key, (u16_t)value.size(), (u16_t)(4096 - (int)value.size())});
    values.push_back(value);
    right_sibling = 0;
}
void mleaf_t::push_back(key__t key, std::string value) {
    free_space -= value.size() + 12;
    slots.push_back({key, (u16_t)value.size(), (u16_t)((int)slots[num_keys - 1].offset - (int)value.size())});
    values.push_back(value);
    ++num_keys;
    return;
}
void mleaf_t::pop_back() {
    free_space += slots[num_keys - 1].size + 12;
    slots.pop_back();
    values.pop_back();
    --num_keys;
}
minternal_t::minternal_t(page_t& page) : mnode_t(page) {
    is_leaf = 0;
    first_child = ((pagenum_t*)page.a)[120 / 8];
    for (int i = 0; i < num_keys; ++i) {
        key__t key = ((key__t*)page.a)[128 / 8 + 2 * i];
        pagenum_t pagenum = ((pagenum_t*)page.a)[128 / 8 + 1 + 2 * i];
        keys.push_back(key);
        children.push_back(pagenum);
    }
}
void minternal_t::push_back(key__t key, pagenum_t page) {
    keys.push_back(key);
    children.push_back(page);
    ++num_keys;
}
void minternal_t::pop_back() {
    keys.pop_back();
    children.pop_back();
    --num_keys;
}