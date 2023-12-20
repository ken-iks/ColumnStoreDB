/**
 * Contains function definitions for all
 * b+ tree functionality.
 **/

#include "cs165_api.h"


/***********************************/
/* Functions for searching b+ tree */
int find_pos(BPTreeNode* root, int val, int min);
BPTreeNode* find_leaf_node(BPTreeNode* root, int val);
void find_pos_range(BPTreeNode* root, int* num_results, int** ret_indices, int* min_val, int* max_val);
/***********************************/

/************************************************/
/* Functions for updating/deleting from b+ tree */
void bplus_remove(BPTreeNode* root, int val, int pos);
/************************************************/

/****************************************/
/* Functions for inserting into b+ tree */

BPTreeNode* create_node();
BPTreeNode* create_new_root_node();

int find_insertion_index(BPTreeNode* node, int val);

BPTreeNode* bplus_insert(BPTreeNode* root, int val, int pos, int update_vals);
void insert_into_leaf(BPTreeNode* leaf_node, int val, int pos, int insertion_index);
BPTreeNode* split_leaf_and_insert(BPTreeNode* root, BPTreeNode* leaf_node, int val, int pos);

BPTreeNode* insert_into_parent(BPTreeNode* root, BPTreeNode* parent, BPTreeNode* left_node, BPTreeNode* right_node, int val);
BPTreeNode* insert_into_node(BPTreeNode* node, BPTreeNode* right_node, int val);
BPTreeNode* split_node_and_insert(BPTreeNode* root, BPTreeNode* node, BPTreeNode* right_node, int val);
BPTreeNode* insert_into_new_root(BPTreeNode* left_node, BPTreeNode* right_node, int val);
void print_tree();