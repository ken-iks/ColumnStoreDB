

/* BREAK APART THIS API (TODO MYSELF) */
/* PLEASE UPPERCASE ALL THE STUCTS */

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>

// My additions
#include <pthread.h>

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64

// define fanout of btree so that each node fits on one page (4096 bytes)
#define FANOUT 340
#define LEAF_SIZE 508



typedef enum IndexType {
    NONE,
    BTREE_CLUSTERED,
    BTREE_UNCLUSTERED,
    SORTED_CLUSTERED,
    SORTED_UNCLUSTERED
} IndexType;


typedef struct Column {
    char name[MAX_SIZE_NAME];
    char path[MAX_SIZE_NAME]; 
    int* data;
    // You will implement column indexes later. 
    IndexType index;
    int data_size; //keep track of how much space you used to allocate data incase you need to reallocate
    int count; //dunno how i will even keep track of entry count but maybe
} Column;

/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - columns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/

typedef struct Table {
    char name [MAX_SIZE_NAME];
    char path[MAX_SIZE_NAME]; 
    //CatalogEntry** columns;
    size_t col_count;
    size_t table_length;
    char sort_col_name [MAX_SIZE_NAME];
    size_t sort_col_index;
} Table;


/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/

typedef struct Db {
    char name[MAX_SIZE_NAME];
    char path[MAX_SIZE_NAME]; 
    Table *tables;
    size_t tables_size;
    size_t tables_capacity;
} Db;



/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call. */
  ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status {
    StatusCode code;
    char* error_message;
} Status;


/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */

// Structure for shared scan context.
typedef struct SelectObject{
    char handle[MAX_SIZE_NAME];
    int minval; // Starting index of the scan.
    int maxval; // Ending index of the scan.
    int* results; // to store results
    int results_capacity;
    pthread_mutex_t* mutex; // Mutex for shared resources, like writing to the results array.
} SelectObject;



/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType {
    CREATE,
    INSERT,
    LOAD,
} OperatorType;


typedef enum CreateType {
    _DB,
    _TABLE,
    _COLUMN,
} CreateType;

/*
 * necessary fields for creation
 * "create_type" indicates what kind of object you are creating. 
 * For example, if create_type == _DB, the operator should create a db named <<name>> 
 * if create_type = _TABLE, the operator should create a table named <<name>> with <<col_count>> columns within db <<db>>
 * if create_type = = _COLUMN, the operator should create a column named <<name>> within table <<table>>
 */
typedef struct CreateOperator {
    CreateType create_type; 
    char name[MAX_SIZE_NAME]; 
    Db* db;
    Table* table;
    int col_count;
} CreateOperator;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;
/*
 * necessary fields for insertion
 */
typedef struct LoadOperator {
    char* file_name;
} LoadOperator;
/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    CreateOperator create_operator;
    InsertOperator insert_operator;
    LoadOperator load_operator;
} OperatorFields;
/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */


typedef struct {
    int startLine;
    int endLine;
    int startOffset;
    int endOffset;
    int ihigh;
    int ilow;
    char handle[MAX_SIZE_NAME];
    char filepath[MAX_SIZE_NAME];
    bool is_column;
    int pvector[10240];
    int vvector[10240];

    int* bitvector;
    SelectObject** selects;
    int numSelects;
    
    // Other necessary fields
} ThreadArgs;

typedef struct Index {
    char filepath[MAX_SIZE_NAME];
    IndexType type;
    int* data;
    int* positions;
    int num_items;
} Index;

typedef struct BPTreeNode BPTreeNode;

typedef struct BPTreeInternalNode {
    struct BPTreeNode* pointers[FANOUT];    // array of pointers to other nodes
    int vals[FANOUT - 1];                   // array of keys 
} BPTreeInternalNode;


typedef struct BPTreeLeafNode {
    int vals[LEAF_SIZE];         // array of values 
    int positions[LEAF_SIZE];    // array of corresponding positions in base data
    
    struct BPTreeNode* next;     // pointer to next leaf
    struct BPTreeNode* prev;     // pointer to previous leaf
} BPTreeLeafNode;


typedef union BPTreeNodeType {
    BPTreeInternalNode internal_node;
    BPTreeLeafNode leaf_node;
} BPTreeNodeType;


struct BPTreeNode {
    int is_leaf;                  // bool for leaf
    int num_vals;                 // number of vals stored
    BPTreeNodeType type;          // leaf or internal
    struct BPTreeNode* parent;    // pointer to parent node
};

typedef struct LeafIndexRes {
    BPTreeNode* leaf_node;
    int index;
} LeafIndexRes;



// CODE FOR HASHTABLE IMPLEMENTATION OF CATALOG -> also used for variable pool

typedef struct CatalogEntry {
    char name[MAX_SIZE_NAME];
    char filepath[MAX_SIZE_NAME];
    CreateType t;
    int size; // size of bit vector (and TODO: size of table)
    int line; // which line this entry is on the catalog
    struct CatalogEntry *next;  // In case of collisions, we use chaining
    bool in_vpool;
    int* bitvector; // For variable pool
    int bitv_capacity;
    bool is_column;
    Index** indexes;
    bool has_index;
    bool in_cluster;
    int index_count;
    int index_capacity;
    char* data; // This will point to the memory-mapped file or a string
    int* data2;
    size_t data_size; // Size of the data
    size_t data2_size;
    int num_lines;
    int num_entries; // for int* implementation
    int offset;
    bool has_value;
    float value; // For arithmetic operators
} CatalogEntry;

typedef struct CatalogHashtable {
    CatalogEntry* table[5003]; // An array of pointers to entries
} CatalogHashtable;

typedef struct Tb {
    char name [MAX_SIZE_NAME];
    char path[MAX_SIZE_NAME]; 
    CatalogEntry** columns;
    size_t col_count;
    size_t col_capacity;
    bool indexed;
    bool clustered;
    char sort_col_path[MAX_SIZE_NAME];
    int sort_col_index;
} Tb;

typedef struct ClientContext {
    Tb* tables[100];
    int num_tables;
    // So we can know whether or not we are within a batch query
    bool is_batch;
    char batch_identifier[MAX_SIZE_NAME];
    SelectObject* selects[1000];
    int num_selects;
    // For loading -> to be persisted upon shutdown
    //char* cols_in_vpool[2040];
    bool multithread;
    
} ClientContext;

typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;

extern Db *current_db;


// QUEUE IS FOR MULTIPLE CORE USE

// A node in the queue.
typedef struct node {
    SelectObject query;
    struct node* next;
} node_t;

// A thread-safe queue with mutex.
typedef struct Queue {
    node_t* head;
    node_t* tail;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} Queue;



/* 
 * Use this command to see if databases that were persisted start up properly. If files
 * don't load as expected, this can return an error. 
 */

Status db_startup();

Status create_db(const char* db_name);

Table* create_table(Db* db, const char* name, size_t num_columns, Status *status);

Column* create_column(Table *table, char *name, bool sorted, Status *ret_status);

Status shutdown_server();

char** execute_db_operator(DbOperator* query);
void db_operator_free(DbOperator* query);


#endif /* CS165_H */

