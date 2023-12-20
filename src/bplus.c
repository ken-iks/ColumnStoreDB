/**
 * Contains all functionality
 * for b+ tree;
 **/

#include "bplus.h"
#include "parse.h"
#include <signal.h>


void print_leaf(BPTreeNode* curr);
void print_tree(BPTreeNode* curr);


/**
 * Clear pointers in node, used in loading from file.
 **/
void clear_node_ptrs(BPTreeNode* node) {
    node->type.leaf_node.next = NULL;
    node->type.leaf_node.prev = NULL;
    node->parent = NULL;
}


/**
 * Recursively dump a bp tree node to the given fd.
 **/
void dump_bptree_node(FILE* fd, BPTreeNode* node, int* base_data) {
    // if leaf need to dump pos ptrs
    if (node->is_leaf) {
        // dump node's vals
        fwrite(&node->type.leaf_node.vals, sizeof(int), node->num_vals, fd);

        // dump all children
        for (int num_child = 0; num_child < node->num_vals; num_child++) {
            int pos = node->type.leaf_node.positions[num_child];
            fwrite(&pos, sizeof(int), 1, fd);
        }
    } else {
        // dump node's vals
        fwrite(&node->type.internal_node.vals, sizeof(int), node->num_vals, fd);

        // dump all children
        for (int num_child = 0; num_child <= node->num_vals; num_child++) {
            // write child
            BPTreeNode* child = (BPTreeNode*) node->type.internal_node.pointers[num_child];
            fwrite(child, sizeof(BPTreeNode), 1, fd);
            
            // recursively dump
            dump_bptree_node(fd, child, base_data);
        }
    }
}


void dump_bptree(FILE* fd, BPTreeNode* root, int* base_data) {    
    // dump root metadata
    fwrite(root, sizeof(BPTreeNode), 1, fd);

    // recursively dump nodes
    dump_bptree_node(fd, root, base_data);    

    // free btree
    free_node(root);
}

typedef struct ConnectNodes {
    BPTreeNode** connect_children;
    int num_children;
} ConnectNodes;


/**
 * Recursively load a bp tree node to the given fd.
 **/
void load_bptree_node(FILE* fd, BPTreeNode* node, ConnectNodes* t, int* base_data) {
    // recursively load children
    BPTreeNode* prev_child = NULL;
    for (int num_child = 0; num_child <= node->num_vals; num_child++) {
        // read child metadata
        BPTreeNode* child = create_node();
        fread(child, sizeof(BPTreeNode), 1, fd);
        clear_node_ptrs(child);

        // set child's parent
        child->parent = node;

        // if child leaf just load pos ptrs
        if (child->is_leaf) {
            // read vals
            fread(child->type.leaf_node.vals, sizeof(int), child->num_vals, fd);

            // read pos ptrs
            for (int num_pos = 0; num_pos < child->num_vals; num_pos++) {
                int* pos_ptr = malloc(sizeof(int));
                fread(pos_ptr, sizeof(int), 1, fd);
                // TODO: get address via ptr arithmetic: need address of base data
                child->type.leaf_node.positions[num_pos] = *pos_ptr;
            }

            // set prev/next
            if (prev_child != NULL) {
                prev_child->type.leaf_node.next = child;
                child->type.leaf_node.prev = prev_child;
            // if on first child, add to connect children
            } else {
                t->connect_children = realloc(t->connect_children, ++t->num_children * sizeof(BPTreeNode*));
                t->connect_children[t->num_children - 1] = child;
            }

            prev_child = child;
        // else recursively load children
        } else {
            // read vals
            fread(child->type.internal_node.vals, sizeof(int), child->num_vals, fd);

            load_bptree_node(fd, child, t, base_data);
        }

        node->type.internal_node.pointers[num_child] = child;
    }

    if (prev_child != NULL) {
        t->connect_children = realloc(t->connect_children, ++t->num_children * sizeof(BPTreeNode*));
        t->connect_children[t->num_children - 1] = prev_child;
    }
}


void* load_bptree(FILE* fd, int* base_data) {
    // load root
    BPTreeNode* root = create_node();
    fread(root, sizeof(BPTreeNode), 1, fd);
    if (root->is_leaf) {
        fread(root->type.leaf_node.vals, sizeof(int), root->num_vals, fd);
    } else {
        fread(root->type.internal_node.vals, sizeof(int), root->num_vals, fd);
    }
    clear_node_ptrs(root);

    ConnectNodes* t = malloc(sizeof(ConnectNodes));
    t->connect_children = NULL;
    t->num_children = 0;

    // recursively load children
    load_bptree_node(fd, root, t, base_data);

    // connect leaves
    BPTreeNode* last = NULL;
    BPTreeNode* first = NULL;
    for (int i = 1; i < t->num_children - 1; i += 2) {
        last = t->connect_children[i];
        first = t->connect_children[i + 1];

        last->type.leaf_node.next = first;
        first->type.leaf_node.prev = last;
    }

    free(t);

    return (void*) root;
}


/**
 * Recursively free BPTreeNode memory
 **/
void free_node(BPTreeNode* node) {
    // if leaf just need to free
    if (node->is_leaf) {
        free(node);
    // else call free_node for all children
    } else {
        for (int i = 0; i <= node->num_vals; i++) {
            BPTreeNode* child = node->type.internal_node.pointers[i];
            free_node(child);
        }
        
        // free node
        free(node);
    }
}


/** 
 * Find index where first instance of val should be.
 **/
LeafIndexRes* find_leaf_val_index(BPTreeNode* node, int val) {
    // simply binary search for val
    int index = binary_search(node->type.leaf_node.vals, node->num_vals, val);

    // if index is 0 need to check previous leafs
    if (index == 0) {
        BPTreeNode* prev_node = node->type.leaf_node.prev;
        int prev_index;
        while (prev_node != NULL) {
            // get index of val in previous leaf node
            prev_index = binary_search(prev_node->type.leaf_node.vals, prev_node->num_vals, val);

            // if index isn't very last, then val is in leaf node
            if (prev_index < prev_node->num_vals) {
                // set leaf node to prev_node
                node = prev_node;

                // keep moving left till found position where node val != val
                while (prev_index > 0 && prev_node->type.leaf_node.vals[prev_index - 1] == val) {
                    prev_index--;
                }
                index = prev_index;

                // if at prev_index 0, need to continue to previous leaf node
                if (prev_index == 0) {
                    prev_node = prev_node->type.leaf_node.prev;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    LeafIndexRes* res = malloc(sizeof(LeafIndexRes));
    res->leaf_node = node;
    res->index = index;

    return res;
}


void find_pos_range(BPTreeNode* root, int* num_results, int** ret_indices, int* min_val, int* max_val) {
    int num_indices = 0;
    int* return_indices = *ret_indices;

    // find leaf node
    if (min_val != NULL && max_val != NULL) {
        BPTreeNode* leaf_node = find_leaf_node(root, *min_val);

        LeafIndexRes* min_leaf_res = find_leaf_val_index(leaf_node, *min_val);
        leaf_node = min_leaf_res->leaf_node;

        if (max_val == NULL) {
            int start_index = min_leaf_res->index;
            while (leaf_node != NULL) {
                for (int index = start_index; index < leaf_node->num_vals; index++) {
                    return_indices[num_indices++] = leaf_node->type.leaf_node.positions[index];
                }
                start_index = 0;
                leaf_node = leaf_node->type.leaf_node.next;
            }
        } else {

            // find max_leaf_node 
            BPTreeNode* max_leaf_node = find_leaf_node(root, *max_val);
            LeafIndexRes* max_leaf_res = find_leaf_val_index(max_leaf_node, *max_val);
            max_leaf_node = max_leaf_res->leaf_node;

            int start_index = min_leaf_res->index;
            int end_index;
            do {
                if (leaf_node == max_leaf_node) {
                    end_index = max_leaf_res->index;
                } else {
                    end_index = leaf_node->num_vals;
                }

                for (int index = start_index; index < end_index; index++) {
                    return_indices[num_indices++] = leaf_node->type.leaf_node.positions[index];
                }
                start_index = 0;
                leaf_node = leaf_node->type.leaf_node.next;
            } while (leaf_node != NULL && leaf_node->type.leaf_node.prev != max_leaf_node);
        }
    }
    *num_results = num_indices;
}


/**
 * Given a val and tree, returns pos for given val.
 **/
int find_pos(BPTreeNode* root, int val, int min) {
    // find leaf node
    BPTreeNode* leaf_node = find_leaf_node(root, val);

    if (leaf_node == NULL) {
        return 0;
    }

    // get index of val
    LeafIndexRes* res = find_leaf_val_index(leaf_node, val);

    leaf_node = res->leaf_node;
    int index = res->index;

    int adjust = 0;
    // if getting max and node val is greater than val, go to previous index and add one
    if (leaf_node->type.leaf_node.vals[index] > val && !min) {
        adjust = 1;
        index -= 1;
    }

    if (index < leaf_node->num_vals) {
        // get pos from index
        return leaf_node->type.leaf_node.positions[index] + adjust;
    } else {
        if (leaf_node->type.leaf_node.next) {
            return leaf_node->type.leaf_node.next->type.leaf_node.positions[0] + adjust;
        } else {
            return leaf_node->type.leaf_node.positions[index - 1] + adjust;
        }
    }
}


/**
 * Given a root node and a val, searches through
 * tree and finds leaf node where val should
 * be contained.
 **/
BPTreeNode* find_leaf_node(BPTreeNode* root, int val) {
    if (root == NULL) {
        return NULL;
    }

    // iterate through tree till at leaf
    BPTreeNode* curr = root;
    while (curr != NULL && !curr->is_leaf) {
        // binary search for val
        int index = binary_search(curr->type.internal_node.vals, curr->num_vals, val);

        // get next node from pointer
        curr = curr->type.internal_node.pointers[index];
    }

    // return leaf node
    return curr;
}


/**
 * Creates a new empty BPlusNode.
 **/
BPTreeNode* create_node() {
    // allocate space for new node
    BPTreeNode* new_node = calloc(1, sizeof(BPTreeNode));

    // allocate space for pointers and vals
    // new_node->pointers = calloc(FANOUT, sizeof(BPTreeNode*));
    // new_node->vals = calloc(FANOUT - 1, sizeof(int));

    // set everything else as null
    // new_node->parent = NULL;
    // new_node->type.leaf_node.next = NULL;
    // new_node->type.leaf_node.prev = NULL;
    new_node->num_vals = 0;
    new_node->is_leaf = 0;

    return new_node;
}


/**
 * Creates a new leaf BPlusNode.
 **/
BPTreeNode* create_leaf_node() {
    BPTreeNode* leaf = create_node();
    leaf->is_leaf = 1;
    return leaf;
}


/**
 * Initializes new root with val and pos.
 **/
BPTreeNode* init_tree(int val, int pos) {
    // create leaf node as root
    BPTreeNode* root = create_leaf_node();
    
    // insert val and pos ptr
    root->type.leaf_node.vals[0] = val;
    root->type.leaf_node.positions[0] = pos;
    root->num_vals = 1;

    return root;
}


void update_leaf_positions(BPTreeNode* leaf_node, int pos, int subtract) {
    if (subtract) {
        for (int i = 0; i < leaf_node->num_vals; i++) {
            if (leaf_node->type.leaf_node.positions[i] >= pos) {
                leaf_node->type.leaf_node.positions[i] -= 1;
            }
        }
    } else {
        for (int i = 0; i < leaf_node->num_vals; i++) {
            if (leaf_node->type.leaf_node.positions[i] >= pos) {
                leaf_node->type.leaf_node.positions[i] += 1;
            }
        }
    }
}


void update_all_positions(BPTreeNode* leaf_node, int pos, int subtract) {
    // update leaf node's positions
    update_leaf_positions(leaf_node, pos, subtract);

    // update all leafs to the right
    BPTreeNode* curr = leaf_node->type.leaf_node.next;
    while (curr) {
        update_leaf_positions(curr, pos, subtract);
        curr = curr->type.leaf_node.next;
    }

    // update all leafs to the left
    curr = leaf_node->type.leaf_node.prev;
    while (curr) {
        update_leaf_positions(curr, pos, subtract);
        curr = curr->type.leaf_node.prev;
    }
}

/**
 * Remove position from bplus tree and 
 * updates all other positions.
 **/
void bplus_remove(BPTreeNode* root, int val, int pos) {
    // get leaf from val
    BPTreeNode* leaf_node = find_leaf_node(root, val);

    // get index of val
    LeafIndexRes* res = find_leaf_val_index(leaf_node, val);
    leaf_node = res->leaf_node;
    int index = res->index;

    BPTreeNode* curr = leaf_node;
    while (curr != NULL) {
        // if at position, remove and shift over
        if (curr->type.leaf_node.positions[index] == pos) {
            for (int i = index; i < curr->num_vals - 1; i++) {
                curr->type.leaf_node.positions[i] = curr->type.leaf_node.positions[i + 1];
                curr->type.leaf_node.vals[i] = curr->type.leaf_node.vals[i + 1];    
            }
            curr->num_vals -= 1;
            break;
        }

        // add one to index, check if at end of leaf
        index += 1;
        if (index >= curr->num_vals) {
            curr = curr->type.leaf_node.next;
            index = 0;
        }
    }

    // now update all positions greater than position
    update_all_positions(leaf_node, pos, 1);
}

/**
 * Inserts a given val and pos into 
 * a B+ Tree, given as root.
 **/
BPTreeNode* bplus_insert(BPTreeNode* root, int val, int pos, int update_vals) {
    // if root is null start new tree and return
    if (root == NULL) {
        return init_tree(val, pos);
    }

    // find leaf node
    BPTreeNode* leaf_node = find_leaf_node(root, val);

    // check if leaf_node has room or if val already in leaf
    int insertion_index = find_insertion_index(leaf_node, val);
    if (leaf_node->num_vals < (LEAF_SIZE - 1)) {
        // if clustered and middle insert update all positions greater or equal to pos
        if (update_vals) {
            update_all_positions(leaf_node, pos, 0);
        }

        // simply insert into leaf
        insert_into_leaf(leaf_node, val, pos, insertion_index);

        return root;
    // else need to split leaf then insert
    } else {
        return split_leaf_and_insert(root, leaf_node, val, pos);
    }
}


/**
 * Creates a new root BPlusNode,
 * and inserts pointers and val.
 **/
BPTreeNode* insert_into_new_root(BPTreeNode* left_node, BPTreeNode* right_node, int val) {
    BPTreeNode* root = create_node();
    
    // set val
    root->type.internal_node.vals[0] = val;
    root->num_vals = 1;

    // set pointers
    root->type.internal_node.pointers[0] = left_node;
    root->type.internal_node.pointers[1] = right_node;

    // set children parent
    left_node->parent = root;
    right_node->parent = root;

    return root;
}


/** 
 * Find index where val should be inserted.
 **/
int find_insertion_index(BPTreeNode* node, int val) {
    int index;
    if (node->is_leaf) {
        // simply binary search for val
        index = binary_search(node->type.leaf_node.vals, node->num_vals, val);

        // go to last index where val is located
        while (index < node->num_vals && node->type.leaf_node.vals[index] == val) {
            index++;
        }
    } else {
        // simply binary search for val
        index = binary_search(node->type.internal_node.vals, node->num_vals, val);

        // go to last index where val is located
        while (index < node->num_vals && node->type.internal_node.vals[index] == val) {
            index++;
        }
    }


    return index;
}


/**
 * Given a leaf node, a val and a posue, this
 * inserts the new val-posue pair into the leaf node,
 * also executing any balancing that needs to be done.
 **/
void insert_into_leaf(BPTreeNode* leaf_node, int val, int pos, int insertion_index) {
    // shift over all past insertion_index
    for (int i = leaf_node->num_vals; i > insertion_index; i--) {
        leaf_node->type.leaf_node.vals[i] = leaf_node->type.leaf_node.vals[i - 1];
        leaf_node->type.leaf_node.positions[i] = leaf_node->type.leaf_node.positions[i - 1];
    }

    // insert val and pos ptr
    leaf_node->type.leaf_node.vals[insertion_index] = val;
    leaf_node->type.leaf_node.positions[insertion_index] = pos;
    leaf_node->num_vals++;
}

/**
 * Given a full leaf_node and a val-posue pair, this
 * splits the leaf node into two, balances the two nodes,
 * and passes the new necessary vals to the parent.
 **/
BPTreeNode* split_leaf_and_insert(BPTreeNode* root, BPTreeNode* leaf_node, int val, int pos) {
    // create temporary arrays to hold all vals and posues
    int* all_vals = calloc(LEAF_SIZE, sizeof(int));
    int* all_positions = calloc(LEAF_SIZE, sizeof(int));

    // find index to insert new val-pos
    int index = find_insertion_index(leaf_node, val);

    // insert into all vals and pointers
    all_vals[index] = val;
    all_positions[index] = pos;

    // fill in rest of vals and pointers
    int all_idx, leaf_idx;
    for (all_idx = 0, leaf_idx = 0; leaf_idx < LEAF_SIZE - 1; all_idx++, leaf_idx++) {
        // if at index of new val-pos, add one
        if (all_idx == index) {
            all_idx++;
        }

        // insert leaf val-pos to all vals pointers
        all_vals[all_idx] = leaf_node->type.leaf_node.vals[leaf_idx];
        all_positions[all_idx] = leaf_node->type.leaf_node.positions[leaf_idx];
    }

    // create new leaf
    BPTreeNode* right_leaf = create_leaf_node();
    BPTreeNode* left_leaf = leaf_node;

    // clear left_leaf
    left_leaf->num_vals = 0;

    // now split all vals/pointers between left/right leaves
    // get middle index to split at
    int split_index = LEAF_SIZE / 2;

    // set left leaf's vals/pointers
    for (int i = 0; i < split_index; i++) {
        left_leaf->type.leaf_node.vals[i] = all_vals[i];
        left_leaf->type.leaf_node.positions[i] = all_positions[i];
        left_leaf->num_vals++;    
    }

    // set right leaf's vals/pointers
    for (all_idx = split_index, leaf_idx = 0; all_idx < LEAF_SIZE; all_idx++, leaf_idx++) {
        right_leaf->type.leaf_node.vals[leaf_idx] = all_vals[all_idx];
        right_leaf->type.leaf_node.positions[leaf_idx] = all_positions[all_idx];
        right_leaf->num_vals++;    
    }

    // set right leaf's parent
    right_leaf->parent = left_leaf->parent;

    right_leaf->type.leaf_node.next = left_leaf->type.leaf_node.next;
    right_leaf->type.leaf_node.prev = left_leaf;
    left_leaf->type.leaf_node.next = right_leaf;

    // free all vals/pointers
    free(all_vals);
    free(all_positions);

    // now need to insert new val into parent
    int new_val = right_leaf->type.leaf_node.vals[0];
    return insert_into_parent(root, right_leaf->parent, left_leaf, right_leaf, new_val);
}


/**
 * Inserts a new val into a parent node, given
 * a child left and right node and the new val.
 **/
BPTreeNode* insert_into_parent(BPTreeNode* root, BPTreeNode* parent, BPTreeNode* left_node, BPTreeNode* right_node, int val) {
    // if no parent need to create new root with val
    if (parent == NULL) {
        // create new root and return
        return insert_into_new_root(left_node, right_node, val);
    }

    // if parent has room just insert into node
    if (parent->num_vals < (FANOUT - 1)) {
        insert_into_node(parent, right_node, val);
        return root;
    // else need to split node and insert
    } else {
        return split_node_and_insert(root, parent, right_node, val);
    }
}


/**
 * Given a node with more space, simply inserts
 * a new val and pointer (either left node or right node) into the node.
 **/
BPTreeNode* insert_into_node(BPTreeNode* node, BPTreeNode* right_node, int val) {
    int index = find_insertion_index(node, val);

    // shift over all past index
    for (int i = node->num_vals; i > index; i--) {
        node->type.internal_node.vals[i] = node->type.internal_node.vals[i - 1];
        node->type.internal_node.pointers[i + 1] = node->type.internal_node.pointers[i];
    }

    // insert val and right node
    node->type.internal_node.vals[index] = val;
    node->type.internal_node.pointers[index + 1] = right_node;
    node->num_vals++;

    return node;
}


/**
 * Given a full node, this splits the node into two,
 * balances the two nodes, then passes the necessary
 * pointers and val to the parent for rebalancing.
 **/
BPTreeNode* split_node_and_insert(BPTreeNode* root, BPTreeNode* node, BPTreeNode* right_node, int val) {
    // create temporary arrays to hold all vals and posues
    int* all_vals = calloc(FANOUT, sizeof(int));
    void** all_pointers = calloc(FANOUT + 1, sizeof(void*));

    // find index to insert new val-pos
    int index = find_insertion_index(node, val);

    // insert into all vals and pointers
    all_vals[index] = val;
    all_pointers[index + 1] = right_node;

    // fill in rest of vals and pointers
    int node_idx;
    for (int all_val_idx = 0, node_idx = 0; node_idx < FANOUT - 1; node_idx++, all_val_idx++) {
        // if at index of new val, skip one
        if (all_val_idx == index) {
            all_val_idx++;
        }

        all_vals[all_val_idx] = node->type.internal_node.vals[node_idx];    
    }

    for (int all_ptr_idx = 0, node_idx = 0; node_idx < FANOUT; node_idx++, all_ptr_idx++) {
        // if at index of new ptr, skip one
        if (all_ptr_idx == index + 1) {
            all_ptr_idx++;
        }

        all_pointers[all_ptr_idx] = node->type.internal_node.pointers[node_idx];
    }

    // create new node
    BPTreeNode* parent_right_node = create_node();
    BPTreeNode* parent_left_node = node;

    // clear parent left node
    parent_left_node->num_vals = 0;

    // now split all vals/pointers between parent left/right nodes
    // get middle index to split at
    int split_index = FANOUT / 2;

    // get new val to insert into parent
    int new_val = all_vals[split_index];

    // set left node's vals/pointers
    for (node_idx = 0; node_idx < split_index; node_idx++) {
        parent_left_node->type.internal_node.vals[node_idx] = all_vals[node_idx];
        parent_left_node->type.internal_node.pointers[node_idx] = all_pointers[node_idx];
        parent_left_node->num_vals++;    
    }
    // set last pointer
    parent_left_node->type.internal_node.pointers[node_idx] = all_pointers[node_idx];

    // set right node's vals/pointers
    int all_idx;
    BPTreeNode* temp = NULL;
    for (all_idx = split_index + 1, node_idx = 0; all_idx < FANOUT; all_idx++, node_idx++) {
        parent_right_node->type.internal_node.vals[node_idx] = all_vals[all_idx];
        parent_right_node->type.internal_node.pointers[node_idx] = all_pointers[all_idx];
        parent_right_node->num_vals++;

        // set new parent
        temp = (BPTreeNode*) parent_right_node->type.internal_node.pointers[node_idx];
        temp->parent = parent_right_node;
    }
    // need to copy last pointer
    parent_right_node->type.internal_node.pointers[node_idx] = all_pointers[all_idx];
    temp = (BPTreeNode*) parent_right_node->type.internal_node.pointers[node_idx];
    temp->parent = parent_right_node;

    // set right node's parent
    parent_right_node->parent = parent_left_node->parent;

    // set next 
    // parent_right_node->type.leaf_node.next = parent_left_node->type.leaf_node.next;
    // parent_left_node->type.leaf_node.next = parent_right_node;

    // free all vals/pointers
    free(all_vals);
    free(all_pointers);

    return insert_into_parent(root, parent_right_node->parent, parent_left_node, parent_right_node, new_val);
}


void print_leaf(BPTreeNode* curr) {
    if (curr != NULL) {
        for (int i = 0; i < curr->num_vals; i++) {
            printf("%d: %d\n", curr->type.leaf_node.vals[i], curr->type.leaf_node.positions[i]);
        }
    }
}


void print_tree(BPTreeNode* root) {
    BPTreeNode* curr = root;
    // BPTreeNode* prev = NULL;

    // printf("ROOT: ");
    // print_leaf(root);
    // printf("\n\n");

    while (!curr->is_leaf) {        
        if (!curr->num_vals) 
            return;

        curr = (BPTreeNode*) curr->type.internal_node.pointers[0];
    }

    while (curr != NULL) {
        for (int i = 0; i < 10; i++) {
            printf("%d: %d\n", curr->type.leaf_node.vals[i], curr->type.leaf_node.positions[i]);
        }
        curr = NULL;
        // curr = curr->type.leaf_node.next;
    }

    // while (prev != NULL) {
    //     printf("%p: |", (void*) prev);
    //     for (int i = 0; i < prev->num_vals; i++) {
    //         printf("%d,", prev->vals[i]);

    //     }
    //     printf("|\n");
    //     for (int i = 0; i < prev->num_vals + 1; i++) {
    //         print_leaf((BPTreeNode*) prev->pointers[i]);
    //     }
    //     printf("\n\n");
    //     prev = prev->type.leaf_node.next;
    // }
}