#ifndef __YJINBPT_H__
#define __YJINBPT_H__

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <algorithm>
#include <queue>
#include "buffer.h"
#include "trx.h"
#include "recovery.h"

#define LEAF_THRESHOLD 2500
#define INTERNAL_MAX_KEYS 248
#define INTERNAL_MIN_KEYS 124

#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif

// TYPES.

/* Type representing the record
 * to which a given key refers.
 * In a real B+ tree system, the
 * record would hold data (in a database)
 * or a file (in an operating system)
 * or some other information.
 * Users can rewrite this part of the code
 * to change the type and content
 * of the value field.
 */

/* Type representing a node in the B+ tree.
 * This type is general enough to serve for both
 * the leaf and the internal node.
 * The heart of the node is the array
 * of keys and the array of corresponding
 * pointers.  The relation between keys
 * and pointers differs between leaves and
 * internal nodes.  In a leaf, the index
 * of each key equals the index of its corresponding
 * pointer, with a maximum of order - 1 key-pointer
 * pairs.  The last pointer points to the
 * leaf to the right (or NULL in the case
 * of the rightmost leaf).
 * In an internal node, the first pointer
 * refers to lower nodes with keys less than
 * the smallest key in the keys array.  Then,
 * with indices i starting at 0, the pointer
 * at i + 1 points to the subtree with keys
 * greater than or equal to the key in this
 * node at index i.
 * The num_keys field is used to keep
 * track of the number of valid keys.
 * In an internal node, the number of valid
 * pointers is always num_keys + 1.
 * In a leaf, the number of valid pointers
 * to data is always num_keys.  The
 * last leaf pointer points to the next leaf.
 */

table_t open_table (char *pathname);
int db_insert(table_t table_id, key__t key, char * value, u16_t val_size);
int db_delete (table_t table_id, key__t key);
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t * val_size, int trx_id = 0);
int db_update(int64_t table_id, int64_t key, char* values, uint16_t new_val_size, uint16_t* old_val_size, int trx_id);
int init_db(int num_buf, int flag, int log_num, char* log_path, char* logmsg_path);
int shutdown_db();

void print_leaves(table_t fd, mnode_t* root );
int height(table_t fd, mnode_t* root);
int path_to_root(table_t fd, mnode_t& child);
pagenum_t get_root_page(table_t fd);
void print_tree(table_t fd);
pagenum_t find_leaf_page(table_t fd, key__t key);
bool cmp_slot(mslot_t a, mslot_t b);
void print(mleaf_t&);
void print(minternal_t&);

int cut(int length);
int cut_leaf(mleaf_t* leaf);
int get_left_index(minternal_t& internal /* parent */, pagenum_t left);
int adjust(mleaf_t& leaf);
pagenum_t insert_into_leaf(table_t fd, pagenum_t pn, key__t key, std::string value);
pagenum_t insert_into_leaf_after_splitting(table_t fd, pagenum_t pn, key__t key, std::string value);
pagenum_t insert_into_node(table_t fd, pagenum_t pn, pagenum_t new_pn, 
        key__t key, pagenum_t parent_pn, int left_index);
pagenum_t insert_into_node_after_splitting(table_t fd, pagenum_t pn, pagenum_t new_pn, 
        key__t key, pagenum_t parent_pn, int left_index);
pagenum_t insert_into_parent(table_t fd, pagenum_t pn, pagenum_t new_pn, key__t new_key, pagenum_t parent);
pagenum_t insert_into_new_root(table_t fd, pagenum_t pn, pagenum_t new_pn, key__t key);
pagenum_t start_new_tree(table_t fd, key__t key, std::string value);

int get_index(table_t fd, pagenum_t pn);
void remove_entry_from_node(table_t fd, pagenum_t pn, int index);
int adjust_root(table_t fd, pagenum_t pn);
int coalesce_nodes(table_t fd, pagenum_t pn, pagenum_t neighbor_pn, int index/* index of pn */, int k_prime);
int redistribute_nodes(table_t fd, pagenum_t pn, pagenum_t neighbor_pn, int index, int k_prime_index, int k_prime);
int delete_entry(table_t fd, pagenum_t pn, key__t key);


#endif /* __YJINBPT_H__*/
