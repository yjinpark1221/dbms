#include "yjinbpt.h"

#include <cassert>
#include <iostream>
#define VERBOSE 0


// FUNCTION DEFINITIONS.

// 1. int64_t open_table (char *pathname);
// • Open existing data file using ‘pathname’ or create one if not existed.
// • If success, return the unique table id, which represents the own table in this database. Otherwise,
// return negative value.
table_t open_table(char* pathname) {
    return buf_open_table_file(pathname);
}

//  2. int db_insert (int64_t table_id, int64_t key, char * value, uint16_t val_size);
// • Insert input ‘key/value’ (record) with its size to data file at the right place
// • If success, return 0. Otherwise, return non-zero value.
int db_insert(table_t table_id, key__t key, char * value, u16_t val_size) {
    // // // print("%s\n", __func__);

    char tmpv[123];
    u16_t tmps;
    mleaf_t leaf;
    // TODO : check find
    if (db_find(table_id, key, tmpv, &tmps) == 0) { // find success -> db unchanged
        return 1;                           // insert fail
    }
    // // // print("db_find done\n");
    /* Create a new record for the
     * value.>
     */
    std::string svalue = "";
    svalue.append((const char*) value, val_size);

    pagenum_t pn = get_root_page(table_id);
    // // // print("get_root_page done");
    /* Case: the tree does not exist yet.
     * Start a new tree.
     */

    if (pn == 0) {
        return start_new_tree(table_id, key, svalue);
    }

    /* Case: the tree already exists.
     * (Rest of function body.)
     */

    pagenum_t leaf_pn = find_leaf_page(table_id, key);
    // // // print("find_leaf_page done\n");
    ctrl_t* ctrl_leaf = buf_read_page(table_id, leaf_pn);
    leaf = *(ctrl_leaf->frame);
    pthread_mutex_unlock(&(ctrl_leaf->mutex));

    if (leaf.free_space >= val_size + 12) {
        return insert_into_leaf(table_id, leaf_pn, key, svalue);
    }
    /* Case: leaf has room for key and pointer.
     */
    return insert_into_leaf_after_splitting(table_id, leaf_pn, key, svalue);
}

// 3. int db_find (int64_t table_id, int64_t key, char * ret_val, uint16_t * val_size);
// • Find the record containing input ‘key’.
// • If found matching ‘key’, store matched ‘value’ string in ret_val and matched ‘size’ in val_size.
// If success, return 0. Otherwise, return non-zero value.
// • The “caller” should allocate memory for a record structure (ret_val).

// • Read a value in the table with a matching key for the transaction having trx_id.
// • return 0 (SUCCESS): operation is successfully done, and the transaction can continue the next operation.
// • return non-zero (FAILED): operation is failed (e.g., deadlock detected), and the transaction should be
// aborted. Note that all tasks that need to be handled (e.g., releasing the locks that are held by this
// transaction, rollback of previous operations, etc. ) should be completed in db_find().
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t * val_size, int trx_id) {
    // printf("%s\n", __func__);
    int i;
    page_t page;
    mleaf_t leaf;
    pagenum_t pn = find_leaf_page(table_id, key);
    if (pn == 0) { // fail
        ret_val[0] = 0;
        *val_size = 0;
        return 1;
    }

    ctrl_t* ctrl = buf_read_page(table_id, pn, trx_id);
    leaf = *(ctrl->frame);
    pthread_mutex_unlock(&(ctrl->mutex));

    auto iter = std::lower_bound(leaf.slots.begin(), leaf.slots.end(), key);
    if (iter != leaf.slots.end() && iter->key == key) { // success
        if (trx_id) {
            int has_slock = 0, has_xlock = 0;
            printf("[THREAD %d] acquiring %d lock_mode %d\n", trx_id, key, 0);
            if (trx_acquire(trx_id, table_id, pn, key, SHARED)) {
                printf("[THREAD %d] aborted\n", trx_id);
                memset(ret_val, 0, 112);
                *val_size = 0;
                return -1;
            }
        }
        printf("[THREAD %d] acquired %d lock_mode %d\n", trx_id, key, 0);
        i = iter - leaf.slots.begin();
        for (int j = 0; j < leaf.slots[i].size; ++j) {
            ret_val[j] = leaf.values[i][j];
        }
        *val_size = leaf.slots[i].size;
        return 0;
    }
    else { // fail
        ret_val[0] = 0;
        *val_size = 0;
        return 1;
    }
}

// • Find the matching key and modify the values.
// • If found matching ‘key’, update the value of the record to ‘values’ string with its ‘new_val_size’ and store its size
// in ‘old_val_size’.
// • return 0 (SUCCESS): operation is successfully done, and the transaction can continue the next operation.
// • return non-zero (FAILED): operation is failed (e.g., deadlock detected), and the transaction should be aborted.
// Note that all tasks that need to be handled (e.g., releasing the locks that are held on this transaction, rollback
// of previous operations, etc. ) should be completed in db_update().
int db_update(int64_t table_id, int64_t key, char* values, uint16_t new_val_size, uint16_t* old_val_size, int trx_id) {
    // printf("%d %s\n", trx_id, __func__);
    pagenum_t pn = find_leaf_page(table_id, key);
    if (pn == 0) { // fail
        *old_val_size = 0;
        return 1;
    }
    // printf("leaf page number %d\n", pn);
    page_t page;
    ctrl_t* ctrl = buf_read_page(table_id, pn, trx_id);
    mleaf_t leaf = *(ctrl->frame);
    // printf("printing leaf\nnum_keys %d\t parent %d\t is_leaf \n", leaf.num_keys, leaf.parent, leaf.is_leaf);
    pthread_mutex_unlock(&ctrl->mutex);
    auto iter = std::lower_bound(leaf.slots.begin(), leaf.slots.end(), key);
    if (iter != leaf.slots.end() && iter->key == key) { // key found
        int has_slock = 0, has_xlock = 0;
        printf("[THREAD %d] acquiring %d lock_mode 1\n", trx_id, key);
        if (trx_acquire(trx_id, table_id, pn, key, EXCLUSIVE)) {
            printf("[THREAD %d] aborted\n", trx_id);
            *old_val_size = 0;
            return -1;
        }
        printf("[THREAD %d] acquired %d lock_mode 1\n", trx_id, key);

        /* case : trx already has the lock
        */
        ctrl = buf_read_page(table_id, pn, trx_id);
        // printf("[THREAD %d] key %d buf_read_page done\n", trx_id, key);
        leaf = *(ctrl->frame);
        iter = std::lower_bound(leaf.slots.begin(), leaf.slots.end(), key);
        
        int idx = iter - leaf.slots.begin();
        mslot_t log_slot = leaf.slots[idx];
        std::string log_value = leaf.values[idx];
        auto& old_value = leaf.values[idx];
        for (int i = 0; i < iter->size; ++i) {
            old_value[i] = values[i];
        }
        *old_val_size = iter->size;
        // assert(*old_val_size == new_val_size);

        page = leaf;
        buf_write_page(&page, ctrl);
        pthread_mutex_unlock(&(ctrl->mutex));

        pthread_mutex_lock(&trx_latch);
        printf("bpt log, trx_id %d\n", trx_id);
        assert(trx_table[trx_id]);
        trx_table[trx_id]->logs.emplace_back(table_id, pn, log_slot, log_value);
        pthread_mutex_unlock(&trx_latch);
        return 0; 
    }
    else {
        pthread_mutex_unlock(&(ctrl->mutex));
        *old_val_size = 0;
        return 1; // key not found
    }
}

// 4. int db_delete (int64_t table_id, int64_t key);
// • Find the matching record and delete it if found.
// • If success, return 0. Otherwise, return non-zero value.
int db_delete(table_t table_id, key__t key) {
    // // print("%s\n", __func__);
    char tmpv[123];
    u16_t tmps;
    pagenum_t leaf_pn = find_leaf_page(table_id, key);
    if (leaf_pn == 0 || db_find(table_id, key, tmpv, &tmps)) { // if the tree is empty or the key is nonexistent
        return 1; // deletion failure
    }
    return delete_entry(table_id, leaf_pn, key); // should be 0 (successful deletion)
}

// 5. int init_db ();
// • Initialize your database management system.
// • Initialize and allocate anything you need.
// • The total number of tables is less than 20.
// • If success, return 0. Otherwise, return non-zero value.
int init_db(int num_buf) {
    transaction_id = 1;
    trx_table.clear();
    lock_table.clear();
    // //// // print("%s\n", __func__);
    return buf_init(num_buf) || pthread_mutex_init(&trx_latch, NULL) || pthread_mutex_init(&trx_latch, NULL);
}

// 6. int shutdown_db();
// • Shutdown your database management system.
// • Clean up everything.
// • If success, return 0. Otherwise, return non-zero value.
int shutdown_db() {
    // printf("%s\n", __func__);
    buf_close_table_file();
    trx_table.clear();
    lock_table.clear();
    pthread_mutex_destroy(&trx_latch);
    pthread_mutex_destroy(&lock_latch);
    return 0;
}

pagenum_t get_root_page(table_t fd) {
    // // print("%s\n", __func__);
    ctrl_t* ctrl = buf_read_page(fd, 0);
    // header page

    pagenum_t pn = ((pagenum_t*)(ctrl->frame))[16 / 8];
        // printf("page latch unlocked\n");
    pthread_mutex_unlock(&(ctrl->mutex));
    return pn;
}

/* Traces the path from the root to a leaf, searching
 * by key.  Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 */
pagenum_t find_leaf_page(table_t fd, key__t key) {
    // printf("%s\n", __func__);
    pagenum_t pn = get_root_page(fd);
    if (pn == 0) {
        return 0;
    }
    // printf("root found %d\n", pn);
    mnode_t c;
    minternal_t internal;
    mleaf_t leaf;

    ctrl_t* ctrl = buf_read_page(fd, pn);
    // printf("read done\n");
    c = *(ctrl->frame);
    while (!c.is_leaf) {
        internal = *(ctrl->frame);
        // printf("page latch unlocked\n");
        pthread_mutex_unlock(&(ctrl->mutex));

        auto iter = std::upper_bound(internal.keys.begin(), internal.keys.end(), key);
        int i = iter - internal.keys.begin();
        // printf("i %d\n", i);
        // printf("printing internal\nis_leaf %d, num_keys %d, parent %d\n", internal.is_leaf, internal.num_keys, internal.parent);
        // printf("first child %d\n", internal.first_child);
        // for (int i = 0; i < internal.num_keys; ++i) {
        //     // printf("%d ", internal.keys[i]);
        // }
        if (i == 0) pn = internal.first_child;
        else pn = internal.children[i - 1];
        // // print("pn %d\n", pn);
        ctrl = buf_read_page(fd, pn);
        // // print("printing ctrl\n");
        // // print("tp.pagenumber %d, \n",ctrl->tp.second);
        c = *(ctrl->frame);
    }
    pthread_mutex_unlock(&(ctrl->mutex));
    // // print("leaf mutex unlocked\n");
    return pn;
}

bool cmp_slot(mslot_t a, mslot_t b) {
    // //// // print("%s\n", __func__);
    return a.key < b.key;
}

int cut_leaf(mleaf_t* leaf) {
    // //// // print("%s\n", __func__);
    int i = 0;
    int sum = 0;
    for (; i < leaf->num_keys; ++i) {
        sum += leaf->slots[i].size + 12;
        if (sum >= 1984) break;
    }
    //// // print("cut leaf i = %d, sum = %d, num_keys = %d\n", i, sum, leaf->num_keys);
    assert(i < leaf->num_keys);
    return i;
}

// INSERTION

/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to 
 * the node to the left of the key to be inserted.
 */
int get_left_index(minternal_t& internal /* parent */, pagenum_t left) {
    //// // print("%s\n", __func__);
    for (int i = 0; i < internal.num_keys; ++i) {
        if (internal.children[i] == left) return i;
    }
    assert(internal.first_child == left);
    return -1;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf page number.
 */
pagenum_t insert_into_leaf(table_t fd, pagenum_t pn, key__t key, std::string value) {
    // // print("%s\n", __func__);
    ctrl_t* ctrl_leaf = buf_read_page(fd, pn);

    mleaf_t leaf = *(ctrl_leaf->frame);

    int insertion_point;
    mslot_t slot;
    slot.size = value.size();
    slot.key = key;
    auto iter = std::upper_bound(leaf.slots.begin(), leaf.slots.end(), slot);
    insertion_point = iter - leaf.slots.begin(); // first key index to move
    leaf.slots.insert(leaf.slots.begin() + insertion_point, slot);
    leaf.values.insert(leaf.values.begin() + insertion_point, value);
    leaf.num_keys++;
    adjust(leaf);
    page_t leaf_page = leaf;
    buf_write_page(&leaf_page, ctrl_leaf);
    pthread_mutex_unlock(&(ctrl_leaf->mutex));
    return 0;
}

int adjust(mleaf_t& leaf) {
    //// // print("%s\n", __func__);
    int offset = 4096;
    int used_space = 0;
    for (int i = 0; i < leaf.num_keys; ++i) {
        offset -= leaf.slots[i].size;
        used_space +=  leaf.slots[i].size + 12;
        leaf.slots[i].offset = offset;
    }
    leaf.free_space = 3968 - used_space;
    return used_space;
}

/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
pagenum_t insert_into_leaf_after_splitting(table_t fd, pagenum_t pn, key__t key, std::string value) {
    // // print("%s\n", __func__);
    ctrl_t* ctrl_new = buf_alloc_page(fd);
    pagenum_t new_pn = ctrl_new->tp.second;
    mleaf_t leaf, new_leaf;
    page_t page, new_page;
    ctrl_t* ctrl_pn = buf_read_page(fd, pn);

    leaf = *(ctrl_pn->frame);

    new_leaf.right_sibling = leaf.right_sibling;
    leaf.right_sibling = new_pn;

    new_leaf.parent = leaf.parent;
    //// // print("insert into leaf after splitting leaf key = %d pn = %d\n", key, pn);
    //// // print("ctrl_pn table_id %d pn %d\n", ctrl_pn->tp.first, ctrl_pn->tp.second);
    //// // print("leaf num_key %d is_leaf %d parent %d\n", leaf.num_keys, leaf.is_leaf, leaf.parent);
    int split_point = cut_leaf(&leaf);
    key__t cmp_key = leaf.slots[split_point].key;
    new_leaf.is_leaf = 1;
    //split_point ~ num_keys - 1 -> num_keys  - split_point
    new_leaf.slots.reserve(leaf.num_keys - split_point);
    new_leaf.values.reserve(leaf.num_keys - split_point);
    new_leaf.slots.insert(new_leaf.slots.end(), leaf.slots.begin() + split_point, leaf.slots.end());
    new_leaf.values.insert(new_leaf.values.end(), leaf.values.begin() + split_point, leaf.values.end());
    new_leaf.num_keys = leaf.num_keys - split_point;

    leaf.num_keys = split_point;

    leaf.slots.resize(split_point);
    leaf.values.resize(split_point);

    adjust(leaf);
    adjust(new_leaf);

    page = leaf;
    buf_write_page(&page, ctrl_pn);
    pthread_mutex_unlock(&(ctrl_pn->mutex));

    new_page = new_leaf;
    buf_write_page(&new_page, ctrl_new);
    pthread_mutex_unlock(&(ctrl_new->mutex));
   
    /* Case : insert into original leaf
    */
    if (key < cmp_key) {
        insert_into_leaf(fd, pn, key, value);

        ctrl_pn = buf_read_page(fd, pn);
        leaf = *(ctrl_pn->frame);
        pthread_mutex_unlock(&(ctrl_pn->mutex));
    }

    /* Case : insert into new leaf
     */
    else {
        insert_into_leaf(fd, new_pn, key, value);

        ctrl_new = buf_read_page(fd, new_pn);
        new_leaf = *(ctrl_new->frame);
        pthread_mutex_unlock(&(ctrl_new->mutex));
    }

    return insert_into_parent(fd, pn, new_pn, new_leaf.slots[0].key, leaf.parent);
}

/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
pagenum_t insert_into_node(table_t fd, pagenum_t pn, pagenum_t new_pn, 
        key__t key, pagenum_t parent_pn, int left_index) {
    // // print("%s\n", __func__);
    page_t page;
    ctrl_t* ctrl = buf_read_page(fd, parent_pn);
    minternal_t parent = *(ctrl->frame);

    parent.keys.push_back(-1);
    parent.children.push_back(-1);
    for (int i = parent.num_keys - 1; i > left_index; --i) {
        parent.keys[i + 1] = parent.keys[i];
        parent.children[i + 1] = parent.children[i];
    }
    parent.keys[left_index + 1] = key;
    parent.children[left_index + 1] = new_pn;
    ++parent.num_keys;

    page = parent;
    buf_write_page(&page, ctrl);
    pthread_mutex_unlock(&(ctrl->mutex));

    return 0;
}

/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
pagenum_t insert_into_node_after_splitting(table_t fd, pagenum_t pn, pagenum_t new_pn, 
        key__t key, pagenum_t parent_pn, int left_index) {
    // // print("%s\n", __func__);
    page_t page;
    ctrl_t* ctrl_parent = buf_read_page(fd, parent_pn);
    minternal_t internal = *(ctrl_parent->frame);
    
    /* First create a temporary set of keys and pointers
     * to hold everything in order, including
     * the new key and pointer, inserted in their
     * correct places. 
     * Then create a new node and copy half of the 
     * keys and pointers to the old node and
     * the other half to the new.
     */
    std::vector<key__t> temp_keys = internal.keys;
    std::vector<pagenum_t> temp_children = internal.children;
    temp_keys.push_back(-1);
    temp_children.push_back(-1);
    for (int i = internal.num_keys - 1; i > left_index; --i) {
        temp_keys[i + 1] = temp_keys[i];
        temp_children[i + 1] = temp_children[i];
    }
    temp_keys[left_index + 1] = key;
    temp_children[left_index + 1] = new_pn;

    /* Create the new node and copy
     * half the keys and pointers to the
     * old and half to the new.
     */
    ctrl_t* ctrl_new = buf_alloc_page(fd);
    pagenum_t new_internal_pn = ctrl_new->tp.second;

    key__t new_key = temp_keys[124];
    minternal_t new_internal, original_internal;
    new_internal.parent = internal.parent;
    new_internal.first_child = temp_children[124];
    new_internal.num_keys = 0;
    new_internal.is_leaf = 0;
    internal.num_keys = 0;
    internal.keys.clear();
    internal.children.clear();
    for (int i = 0; i < 124; ++i) {
        internal.push_back(temp_keys[i], temp_children[i]);
        new_internal.push_back(temp_keys[i + 125], temp_children[i + 125]);
    }

    page = internal;
    buf_write_page(&page, ctrl_parent);
    pthread_mutex_unlock(&(ctrl_parent->mutex));
    
    page = new_internal;
    buf_write_page(&page, ctrl_new);
    pthread_mutex_unlock(&(ctrl_new->mutex));

    /* Change the parent page number of children of new page
     */
    pagenum_t child_pn = new_internal.first_child;
    ctrl_t* ctrl_child = buf_read_page(fd, child_pn);
    page = *(ctrl_child->frame);
    ((pagenum_t*)(page.a))[0] = new_internal_pn;
    buf_write_page(&page, ctrl_child);
    pthread_mutex_unlock(&(ctrl_child->mutex));

    for (int i = 0; i < new_internal.num_keys; ++i) {
        child_pn = new_internal.children[i];
        ctrl_t* ctrl_child = buf_read_page(fd, child_pn);
        page = *(ctrl_child->frame);
        ((pagenum_t*)(page.a))[0] = new_internal_pn;
        buf_write_page(&page, ctrl_child);
        pthread_mutex_unlock(&(ctrl_child->mutex));
    }

    /* Insert a new key into the parent of the two
     * nodes resulting from the split, with
     * the old node to the left and the new to the right.
     */
    return insert_into_parent(fd, parent_pn, new_internal_pn, new_key, internal.parent);
}

/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
pagenum_t insert_into_parent(table_t fd, pagenum_t pn, pagenum_t new_pn, key__t new_key, pagenum_t parent) {
    // // print("%s\n", __func__);
    if (parent == 0) {
        return insert_into_new_root(fd, pn, new_pn, new_key);
    }
    /* Case: leaf or node. (Remainder of
     * function body.)  
     */
    ctrl_t* ctrl = buf_read_page(fd, parent);
    minternal_t internal = *(ctrl->frame);
    pthread_mutex_unlock(&(ctrl->mutex));

    /* Find the parent's pointer to the left 
     * node.
     */
    int left_index = get_left_index(internal, pn);

    /* Simple case: the new key fits into the node. 
     */
    if (internal.num_keys < INTERNAL_MAX_KEYS) {
        return insert_into_node(fd, pn, new_pn, new_key, parent, left_index);
    }

    /* Harder case: split a node in order 
     * to preserve the B+ tree properties.
     */
    return insert_into_node_after_splitting(fd, pn, new_pn, new_key, parent, left_index);
}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
pagenum_t insert_into_new_root(table_t fd, pagenum_t pn, pagenum_t new_pn, key__t key) {
    // // print("%s\n", __func__);
    page_t page, new_page;
    ctrl_t* ctrl_pn = buf_read_page(fd, pn);
    ctrl_t* ctrl_new = buf_read_page(fd, new_pn);
    ctrl_t* ctrl_root = buf_alloc_page(fd);
    pagenum_t root_pn = ctrl_root->tp.second;
    // // print("new root pn %d\n", root_pn);
    page_t root_page;
    minternal_t root;
    root.num_keys = 1;
    root.parent = 0;
    root.is_leaf = 0;

    root.first_child = pn;
    root.children.push_back(new_pn);
    mnode_t node = *(ctrl_pn->frame);
    if (node.is_leaf) {
        mleaf_t leaf = *(ctrl_pn->frame), new_leaf = *(ctrl_new->frame);
        root.keys.push_back(new_leaf.slots[0].key);
        leaf.parent = root_pn;
        new_leaf.parent = root_pn;
        new_leaf.right_sibling = leaf.right_sibling;
        leaf.right_sibling = new_pn;
        page = leaf;
        new_page = new_leaf;
    }
    else {
        minternal_t internal = *(ctrl_pn->frame), new_internal = *(ctrl_new->frame);
        root.keys.push_back(key);
        internal.parent = root_pn;
        new_internal.parent = root_pn;
        page = internal;
        new_page = new_internal;
    }

    root_page = root;
    buf_write_page(&page, ctrl_pn);
    pthread_mutex_unlock(&(ctrl_pn->mutex));

    buf_write_page(&new_page, ctrl_new);
    pthread_mutex_unlock(&(ctrl_new->mutex));

    buf_write_page(&root_page, ctrl_root);
    pthread_mutex_unlock(&(ctrl_root->mutex));

    ctrl_t* ctrl_header = buf_read_page(fd, 0);
    page_t header_page = *(ctrl_header->frame);
    ((pagenum_t*)header_page.a)[2] = root_pn;
    buf_write_page(&header_page, ctrl_header);
    pthread_mutex_unlock(&(ctrl_header->mutex));
    return 0;
}

/* First insertion:
 * start a new tree.
 */
pagenum_t start_new_tree(table_t fd, key__t key, std::string value) {
    //// // print("%s\n", __func__);
    ctrl_t* ctrl = buf_alloc_page(fd);
    pagenum_t pn = ctrl->tp.second;
    mleaf_t leaf(key, value);
    page_t page = leaf;
    buf_write_page(&page, ctrl);     // root
    pthread_mutex_unlock(&(ctrl->mutex));

    ctrl_t* ctrl_header = buf_read_page(fd, 0);
    page = *(ctrl_header->frame);
    ((pagenum_t*)page.a)[2] = pn;
    buf_write_page(&page, ctrl_header);      // header
    pthread_mutex_unlock(&(ctrl_header->mutex));
    return 0;
}

// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */

// if pn is leftmost(first_child) -> -1
// else index
int get_index(table_t fd, pagenum_t pn) {
    // //// // print("%s\n", __func__);
    /* Return the index of the key to the left
     * of the pointer in the parent pointing
     * to n.  
     * If n is the leftmost child, this means
     * return -1.
     */
    page_t page, parent_page;
    ctrl_t* ctrl_pn = buf_read_page(fd, pn);
    mnode_t node;
    node = *(ctrl_pn->frame);
    pthread_mutex_unlock(&(ctrl_pn->mutex));

    ctrl_t* ctrl_parent = buf_read_page(fd, node.parent);
    minternal_t internal = parent_page;
    pthread_mutex_unlock(&(ctrl_parent->mutex));

    for (int i = 0; i <= internal.num_keys; ++i) {
        if (internal.children[i] == pn) {
            return i;
        }
    }
    if (internal.first_child == pn) {
        return -1;
    }
    perror("get_index failure");
    exit(0);
}

void remove_entry_from_node(table_t fd, pagenum_t pn, key__t key) {
    // // // print("%s\n", __func__);
    page_t page;
    ctrl_t* ctrl = buf_read_page(fd, pn);
    mnode_t node = *(ctrl->frame);
    if (node.is_leaf) {
        mleaf_t leaf = *(ctrl->frame);
        mslot_t slot = key;
        auto iter = std::lower_bound(leaf.slots.begin(), leaf.slots.end(), key); 
        int index = iter - leaf.slots.begin();
        for (int i = index; i < leaf.num_keys - 1; ++i) {
            leaf.slots[i] = leaf.slots[i + 1];
            leaf.values[i] = leaf.values[i + 1];
        }
        leaf.slots.resize(leaf.num_keys - 1);
        leaf.values.resize(leaf.num_keys - 1);
        --leaf.num_keys;
        adjust(leaf); // adjust offset and free_space
        page = leaf;
        buf_write_page(&page, ctrl);
        pthread_mutex_unlock(&(ctrl->mutex));
    }
    else {
        minternal_t internal = *(ctrl->frame);
        auto iter = std::lower_bound(internal.keys.begin(), internal.keys.end(), key);
        int index = iter - internal.keys.begin();
        if (index < -1 || index > internal.num_keys) {
            perror("in remove_entry_from_node wrong internal index");
            exit(0);
        }
        if (index == -1) {
            internal.first_child = internal.children[0];
            ++index;
        }
        internal.keys.erase(internal.keys.begin() + index, internal.keys.begin() + index + 1);
        internal.children.erase(internal.children.begin() + index, internal.children.begin() + index + 1);

        --internal.num_keys;
        page = internal;
        buf_write_page(&page, ctrl);
        pthread_mutex_unlock(&(ctrl->mutex));
    }
}

int adjust_root(table_t fd, pagenum_t pn) {
    // // // print("%s\n", __func__);
    page_t page;
    ctrl_t* ctrl = buf_read_page(fd, pn);
    mnode_t node = *(ctrl->frame);
    
    /* Case: nonempty root.
     * Key and pointer have already been deleted,
     * so nothing to be done.
     */
    if (node.num_keys > 0) {
        pthread_mutex_unlock(&(ctrl->mutex));
        return 0;
    }

    /* Case: empty root. 
     */

    buf_free_page(fd, pn);
    pthread_mutex_unlock(&(ctrl->mutex));
    
    pagenum_t new_root_pn = 0;

    // If it has a child, promote 
    // the first (only) child
    // as the new root.
    if (!node.is_leaf) {
        minternal_t internal = page;
        new_root_pn = internal.first_child;
        
        ctrl_t* ctrl_new = buf_read_page(fd, new_root_pn);
        page = *(ctrl_new->frame);
        ((pagenum_t*)page.a)[0] = 0;
        buf_write_page(&page, ctrl_new);
        pthread_mutex_unlock(&(ctrl_new->mutex));
    }

    // If it is a leaf (has no children),
    // then the whole tree is empty.

    ctrl_t* ctrl_header = buf_read_page(fd, 0);
    page = *(ctrl_header->frame);
    ((pagenum_t*)page.a)[2] = new_root_pn;
    buf_write_page(&page, ctrl_header);
    pthread_mutex_unlock(&(ctrl_header->mutex));

    return 0;
}

/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
int coalesce_nodes(table_t fd, pagenum_t pn, pagenum_t neighbor_pn, int index/*index of pn*/, int k_prime) {
    // //// // // print("%s\n", __func__);
    /* Swap neighbor with node if node is on the
     * extreme left and neighbor is to its right.
     * after this, neighbor is on the left of node
     */
    if (index == -1) {
        pagenum_t tmp = pn;
        pn = neighbor_pn;
        neighbor_pn = tmp;
    }

    page_t page, neighbor_page;
    ctrl_t* ctrl_pn = buf_read_page(fd, pn);
    ctrl_t* ctrl_neighbor = buf_read_page(fd, neighbor_pn);
    mnode_t node = *(ctrl_pn->frame), neighbor = *(ctrl_neighbor->frame);

    /* Starting point in the neighbor for copying
     * keys and pointers from n.
     * Recall that n and neighbor have swapped places
     * in the special case of n being a leftmost child.
     */
    int insertion_point = neighbor.num_keys;

    /* Case: nonleaf node.
     * Append k_prime and the following pointer.
     * Append all pointers and keys from the neighbor.
     */
    if (!node.is_leaf) {
        /* Append k_prime.
         */
        minternal_t internal = *(ctrl_pn->frame), neighbor_internal = *(ctrl_neighbor->frame);
        neighbor_internal.keys.push_back(k_prime);
        neighbor_internal.children.push_back(internal.first_child);
        neighbor_internal.keys.insert(neighbor_internal.keys.end(), internal.keys.begin(), internal.keys.end());
        neighbor_internal.children.insert(neighbor_internal.children.end(), internal.children.begin(), internal.children.end());
        int update_index = neighbor_internal.num_keys;
        neighbor_internal.num_keys += internal.num_keys + 1;
        neighbor_page = neighbor_internal;

        buf_write_page(&neighbor_page, ctrl_neighbor);
        pthread_mutex_unlock(&(ctrl_neighbor->mutex));

        /* All children must now point up to the same parent.
         */
        for (int i = update_index; i < neighbor_internal.num_keys; ++i) {
            pagenum_t child_pn = neighbor_internal.children[i];
            ctrl_t* ctrl_child = buf_read_page(fd, child_pn);
            page_t child_page = *(ctrl_child->frame);
            ((pagenum_t*)child_page.a)[0] = neighbor_pn;
            buf_write_page(&child_page, ctrl_child);
            pthread_mutex_unlock(&(ctrl_child->mutex));
        }
    }

    /* In a leaf, append the keys and pointers of
     * n to the neighbor.
     * Set the neighbor's last pointer to point to
     * what had been n's right neighbor.
     */
    else {
        mleaf_t leaf = *(ctrl_pn->frame), neighbor_leaf = *(ctrl_neighbor->frame);
        neighbor_leaf.right_sibling = leaf.right_sibling;
        for (int i = 0; i < leaf.num_keys; ++i) {
            neighbor_leaf.slots.push_back(leaf.slots[i]);
            neighbor_leaf.values.push_back(leaf.values[i]);
        }
        neighbor_leaf.num_keys += leaf.num_keys;
        adjust(neighbor_leaf); // adjust offset and free_space
        neighbor_page = neighbor_leaf;

        buf_write_page(&neighbor_page, ctrl_neighbor);
        pthread_mutex_unlock(&(ctrl_neighbor->mutex));
    }
    buf_free_page(fd, pn);
    pthread_mutex_unlock(&(ctrl_pn->mutex));

    return delete_entry(fd, neighbor.parent, k_prime);
}

/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
// all of them needs to be pinned and unpinned
int redistribute_nodes(table_t fd, pagenum_t pn, pagenum_t neighbor_pn, int index/*pn's index*/, int k_prime_index, int k_prime) {
    // //// // // print("%s\n", __func__);
    page_t page, neighbor_page;
    ctrl_t* ctrl_pn = buf_read_page(fd, pn);
    ctrl_t* ctrl_neighbor = buf_read_page(fd, neighbor_pn);
    mnode_t node = *(ctrl_pn->frame);

    /* Case: n is the leftmost child.
     * Take a key-pointer pair from the neighbor to the right.
     * Move the neighbor's leftmost key-pointer pair
     * to n's rightmost position.
     */
    if (index == -1) {
        if (node.is_leaf) {
            mleaf_t leaf = *(ctrl_pn->frame), neighbor_leaf = *(ctrl_neighbor->frame);
            leaf.slots.push_back(neighbor_leaf.slots[0]);
            leaf.values.push_back(neighbor_leaf.values[0]);
            neighbor_leaf.slots.erase(neighbor_leaf.slots.begin(), neighbor_leaf.slots.begin() + 1);
            neighbor_leaf.values.erase(neighbor_leaf.values.begin(), neighbor_leaf.values.begin() + 1);
            ++leaf.num_keys;
            --neighbor_leaf.num_keys;
            adjust(leaf);
            adjust(neighbor_leaf);
            
            page_t parent_page;
            ctrl_t* ctrl_parent = buf_read_page(fd, leaf.parent);
            minternal_t parent = *(ctrl_parent->frame);
            parent.keys[k_prime_index] = neighbor_leaf.slots[0].key;
            parent_page = parent;
            buf_write_page(&parent_page, ctrl_parent);
            pthread_mutex_unlock(&(ctrl_parent->mutex));

            page = leaf;
            neighbor_page = neighbor_leaf;
        }
        else {
            minternal_t internal = *(ctrl_pn->frame), neighbor_internal = *(ctrl_neighbor->frame);
            internal.keys.push_back(k_prime);
            internal.children.push_back(neighbor_internal.first_child);

            page_t parent_page;
            ctrl_t* ctrl_parent = buf_read_page(fd, internal.parent);
            minternal_t parent = *(ctrl_parent->frame);
            parent.keys[k_prime_index] = neighbor_internal.keys[0];
            parent_page = parent;
            buf_write_page(&parent_page, ctrl_parent);
            pthread_mutex_unlock(&(ctrl_parent->mutex));

            neighbor_internal.first_child = neighbor_internal.children[0];
            neighbor_internal.keys.erase(neighbor_internal.keys.begin(), neighbor_internal.keys.begin() + 1);
            neighbor_internal.children.erase(neighbor_internal.children.begin(), neighbor_internal.children.begin() + 1);
            --neighbor_internal.num_keys;
            ++internal.num_keys;

            pagenum_t child = internal.children[internal.num_keys - 1];

            ctrl_t* ctrl_child = buf_read_page(fd, child);
            page_t child_page = *(ctrl_child->frame);
            ((pagenum_t*)child_page.a)[0] = pn;
            buf_write_page(&child_page, ctrl_child);
            pthread_mutex_unlock(&(ctrl_child->mutex));

            page = internal;
            neighbor_page = neighbor_internal;
        }
    }

    /* Case: n has a neighbor to the left. 
     * Pull the neighbor's last key-pointer pair over
     * from the neighbor's right end to n's left end.
     */
    else {
        if (node.is_leaf) {
            mleaf_t leaf = *(ctrl_pn->frame), neighbor_leaf = *(ctrl_neighbor->frame);
            leaf.slots.insert(leaf.slots.begin(), neighbor_leaf.slots[neighbor_leaf.num_keys - 1]);
            leaf.values.insert(leaf.values.begin(), neighbor_leaf.values[neighbor_leaf.num_keys - 1]);
            neighbor_leaf.slots.pop_back();
            neighbor_leaf.values.pop_back();
            ++leaf.num_keys;
            --neighbor_leaf.num_keys;
            adjust(leaf);
            adjust(neighbor_leaf);
            
            page_t parent_page;
            ctrl_t* ctrl_parent = buf_read_page(fd, leaf.parent);
            minternal_t parent = *(ctrl_parent->frame);
            parent.keys[k_prime_index] = leaf.slots[0].key;
            parent_page = parent;
            buf_write_page(&parent_page, ctrl_parent);
            pthread_mutex_unlock(&(ctrl_parent->mutex));

            page = leaf;
            neighbor_page = neighbor_leaf;
        }
        else {
            minternal_t internal = *(ctrl_pn->frame), neighbor_internal = *(ctrl_neighbor->frame);
            internal.keys.insert(internal.keys.begin(), k_prime);
            internal.children.insert(internal.children.begin(), internal.first_child);
            internal.first_child = neighbor_internal.children[neighbor_internal.num_keys - 1];

            page_t parent_page;
            ctrl_t* ctrl_parent = buf_read_page(fd, internal.parent);
            minternal_t parent = parent_page;
            parent.keys[k_prime_index] = neighbor_internal.keys[neighbor_internal.num_keys - 1];
            parent_page = parent;
            buf_write_page(&parent_page, ctrl_parent);
            pthread_mutex_unlock(&(ctrl_parent->mutex));

            neighbor_internal.keys.pop_back();
            neighbor_internal.children.pop_back();
            --neighbor_internal.num_keys;
            ++internal.num_keys;

            pagenum_t child = internal.first_child;
            ctrl_t* ctrl_child = buf_read_page(fd, child);
            page_t child_page = *(ctrl_child->frame);
            ((pagenum_t*)child_page.a)[0] = pn;
            buf_write_page(&child_page, ctrl_child);
            pthread_mutex_unlock(&(ctrl_child->mutex));

            page = internal;
            neighbor_page = neighbor_internal;
        }
    }
    buf_write_page(&page, ctrl_pn);
    pthread_mutex_unlock(&(ctrl_pn->mutex));

    buf_write_page(&neighbor_page, ctrl_neighbor);
    pthread_mutex_unlock(&(ctrl_neighbor->mutex));
    return 0;
}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */

int delete_entry(table_t fd, pagenum_t pn, key__t key) {
    // // // print("%s\n", __func__);
    // Remove key and pointer from node.
    remove_entry_from_node(fd, pn, key);
    
    /* Case:  deletion from the root. 
     */
    if (pn == get_root_page(fd)) {
        return adjust_root(fd, pn);
    }

    /* Case:  deletion from a node below the root.
     * (Rest of function body.)
     */

    /* Determine minimum allowable size of node,
     * to be preserved after deletion.
     */
     
    page_t page;
    ctrl_t* ctrl = buf_read_page(fd, pn);
    mnode_t node = *(ctrl->frame);

    if (node.is_leaf) {
        mleaf_t leaf = *(ctrl->frame);

        /* Case:  node stays at or above minimum.
        * (The simple case.)
        */
        if (leaf.free_space < LEAF_THRESHOLD) {
            pthread_mutex_unlock(&(ctrl->mutex));
            return 0;
        }
    }
    else {
        // if (VERBOSE) //// // // print("case inter\n");
        minternal_t internal = *(ctrl->frame);

        /* Case: node stays at or above minimum.
        * (The simple case.)
        */
        if (internal.num_keys >= 124) {
            pthread_mutex_unlock(&(ctrl->mutex));
            return 0;
        }
    }

    /* Case:  node falls below minimum.
    * Either coalescence or redistribution
    * is needed.
    */

    /* Find the appropriate neighbor node with which
    * to coalesce.
    * Also find the key (k_prime) in the parent
    * between the pointer to node n and the pointer
    * to the neighbor.
    */
    int index = get_index(fd, pn), neighbor_index, k_prime_index;
    if (index == -1) { // leftmost -> right neighbor
        neighbor_index = 0;
        k_prime_index = 0;
    }
    else { // left neighbor
        neighbor_index = index - 1;
        k_prime_index = index;
    }

    ctrl_t* ctrl_parent = buf_read_page(fd, node.parent);
    minternal_t parent = *(ctrl_parent->frame);
    pthread_mutex_unlock(&(ctrl_parent->mutex));

    pagenum_t neighbor_pn;
    key__t k_prime = parent.keys[k_prime_index];

    if (neighbor_index == -1) {
        neighbor_pn = parent.first_child;
    }
    else {
        neighbor_pn = parent.children[neighbor_index];
    }

    page_t neighbor_page;
    ctrl_t* ctrl_neighbor = buf_read_page(fd, neighbor_pn);
    mnode_t neighbor = *(ctrl_neighbor->frame);

    // leaf
    if (node.is_leaf) {
        mleaf_t neighbor_leaf = *(ctrl_neighbor->frame);
        mleaf_t leaf = *(ctrl->frame);
        pthread_mutex_unlock(&(ctrl->mutex));
        pthread_mutex_unlock(&(ctrl_neighbor->mutex));

        /* Coalescence. */
        if (neighbor_leaf.free_space >= 3968 - leaf.free_space) {
            return coalesce_nodes(fd, pn, neighbor_pn, index, k_prime);
        }

        /* Redistribution. */
        else {
            return redistribute_nodes(fd, pn, neighbor_pn, index, k_prime_index, k_prime);
        }
    }

    // internal
    pthread_mutex_unlock(&(ctrl->mutex));
    pthread_mutex_unlock(&(ctrl_neighbor->mutex));

    /* Coalescence. */
    if (neighbor.num_keys + node.num_keys + 2 <= 249) {
        return coalesce_nodes(fd, pn, neighbor_pn, index, k_prime);
    }

    /* Redistribution. */
    else {
        return redistribute_nodes(fd, pn, neighbor_pn, index, k_prime_index, k_prime);
    }
}