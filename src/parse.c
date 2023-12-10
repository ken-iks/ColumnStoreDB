/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"


#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <dirent.h>
#include <limits.h>
#include <pthread.h>
#define MAX_LINE_SIZE 1024
#define DELIMITER ","

#define MAX_THREADS 4
#define TASK_QUEUE_SIZE 10

char* makePath(char* name, CreateType t);


/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

/**
 * This method takes a char* filepath and maps it to a bucket in our Catalogue Hashtable
 * We use a division remainder method - table size currently 5003 (subject to change)
*/

unsigned long hash(const char *str) {
    unsigned long hash = 0;
    int c;
    while ((c = *str++))
        hash = c + (hash << 6) + (hash << 16) - hash;
    return hash % 5003;
}


// THIS METHOD ALLOCATES AN EMPTY CATALOGUE HASHTABLE
int allocate(CatalogHashtable** ht, int size) {
    *ht = (CatalogHashtable *)malloc(sizeof(CatalogHashtable));
    if (*ht != NULL) {
        CatalogHashtable t;
        for (int i = 0; i < 5003; i++) {
            t.table[i] = (CatalogEntry*) NULL;
        }
        **ht = t;
        // At this point ht should be a block of memory for a hashtable that has all values at null node pointers
        return 0;
    }
    (void) size;
    return -1;
}

// THIS METHOD RETRIEVES THE CATALOG ENTRY FOR A GIVEN OBJECT (CAN BE DB TB OR CL)
// CAN BE IDENTIFIED BY NAME OR BY FILEPATH (filepath.txt = name if column)
CatalogEntry* get(CatalogHashtable* ht, char* name) {
    if (ht != NULL) {
        int key = hash(name);
        bool found = false;
        CatalogEntry* target_node_ptr = ht->table[key];
        while (target_node_ptr != NULL && !found) {
            CatalogEntry target = *target_node_ptr;
            //perror(target.name);
            if (strcmp(target.name, name) == 0) { // || (target.filepath != NULL && (strcmp(target.filepath, name) == 0)) maybe
                found = true;
            }
            else {
                target_node_ptr = target.next;
            } 
        }
        return (found) ? target_node_ptr : (CatalogEntry*) NULL;
    }
    perror("Hashtable doesnt exist");
    return (CatalogEntry*) NULL;
}

// THIS METHOD ADDS A NODE TO THE HASHTABLE IN THE COLLISION CASE
int add_node(CatalogEntry* object_node, CatalogEntry* target_node) {
    if (object_node != (CatalogEntry*) NULL) {
        CatalogEntry this_node = *object_node;
        if (this_node.next == (CatalogEntry*) NULL) {
            object_node->next = target_node;
            return 0;
        }
        else {
            return add_node(object_node->next, target_node);
        }
    }
    return -1;
}

// THIS METHOD PUTS A KEY VALUE PAIRING INTO THE HASHTABLE
int put(CatalogHashtable* ht, CatalogEntry value) {
    // Initialize node
    CatalogEntry* new_node = (CatalogEntry *)malloc(sizeof(CatalogEntry));
    *new_node = value;

    if (ht != NULL) {
        int buck = hash(value.name);
        
        CatalogHashtable temp = *ht;
        if (temp.table[buck] == NULL) {
            ht->table[buck] = new_node;
            return 0;
        }
        else {
            // COLLISION CASE: TODO - FIX. MAYBE OVERCOMMITING? DONT KEEP ADDING TO CATALOGUE?
            // if need to add a column to the hashtable
            if (new_node->in_vpool != true) {
                return add_node(ht->table[buck], new_node);
            }
            // else if need to replace a repeated variable name 
            // (TODO: this is actually comparing hashed names. Hopefully wont cause problems)
            else {
                ht->table[buck] = new_node;
                return 0;
            }
        }
            }
    return -1;
}
// HELPER FUNCTION FOR ERASE
int changeprev(CatalogEntry** ptr2pres, CatalogEntry* newptr) {
    *ptr2pres = newptr;
    return 0;
}
// HELPER FUNCTION FOR ERASE (may come in handy on its own)
int removenode(CatalogEntry** current, CatalogEntry** temp, CatalogEntry** previous) {
    CatalogEntry* current_loc = *temp;
    if (*previous != NULL) {
        (**previous).next = (**temp).next;
        *temp = (**temp).next;
    }
    else {
        *current = (**current).next;
        *temp = *current;
    }
    free(current_loc);
    return 0;
}

int sync_col(CatalogEntry* col) {
    if (col->is_column != NULL && col->is_column == true) {
		int rflag = msync(col->data, col->data_size, MS_SYNC);
		if(rflag == -1) {
            perror("Unable to msync.\n");
            return -1;
		}
		rflag = munmap(col->data, col->data_size);
        if(rflag == -1) {
            perror("Unable to munmap.\n");
            return -1;
        }
        return 0;
    }
    return -1; 
}

// HELPER FUNCTION TO AVOID MEMORY LEAKS
int deallocate_bucket(CatalogEntry* head) {
    CatalogEntry *temp;
    while (head != NULL) {
        temp = head;
        head = head->next;
        sync_col(temp);
        free(temp);
    }
    return 0;
}
// THIS ERASES A GIVEN NAME FROM THE HASHTABLE (may or may not come in handy)
int erase(CatalogHashtable* ht, char* name) {
    if (ht != NULL) {
        int key = hash(name);
        CatalogEntry** curr_node = &ht->table[key];
        CatalogEntry *temp = *curr_node;
        CatalogEntry* prev_node = NULL;
        while (temp != NULL) {
            if (strcmp((*temp).name, name) == 0) {
                removenode(curr_node, &temp, &prev_node);
            }
            else {
                changeprev(&prev_node, temp);
                temp = (*temp).next;
            }
        }
        return 0;
    }
    return -1;
}

// THIS CLEARS THE HASHTABLE FROM MEMORY TO AVOID MEMORY LEAKS
int deallocate(CatalogHashtable* ht) {
    if (ht != NULL) {
        for (int i=0; i<5003; i++) {
            deallocate_bucket((*ht).table[i]);
        }
        free(ht);
        return 0;
    }
    return -1;
}


CreateType str_to_type(char * t) {
    if (strcmp(t, "db") == 0) {
        return _DB;
    } else if (strcmp(t, "tb") == 0) {
        return _TABLE;
    } else if (strcmp(t, "cl") == 0) {
        return _COLUMN;
    } else {
        return (CreateType) NULL;
    }
}

// THIS TAKES A LINE IN THE FILE AND CONVERTS IT TO A CATALOG ENTRY, TO BE APPROPRIATLEY PLACED IN THE HASHTABLE
CatalogEntry* line_to_entry(char* line, int line_num){
    CatalogEntry* cat = (CatalogEntry *) malloc(sizeof(CatalogEntry));
    if (!cat) {
        perror("Failed to allocate memory for CatalogEntry");
        return NULL;
    }

    char* tmp = line; // Use a tmp pointer to keep track of current position in the string.
    
    char* name = strsep(&tmp, ",");
    char* path = strsep(&tmp, ",");
    char* typestring = strsep(&tmp, ",");

    
    if (!name || !path || !typestring) {
        free(cat);
        fprintf(stderr, "Error: Malformed line: %s in line_to_entry function\n", line);
        return NULL;
    }

    //fprintf(stderr, "Valid line: %s in line_to_entry function\n", line);

    CreateType type = str_to_type(typestring);

    strcpy(cat->filepath, path);
    strcpy(cat->name, name);
    cat->t = type;
    cat->next = (CatalogEntry*)NULL;
    cat->line = line_num;
    return cat;
}

// Adds an element with filename = db.tbl.cl to correct file granted that cl exists in catalog
// Primarily used in 'load'
int add_element_for_load(char* filename, char* val, CatalogHashtable* variable_pool) {
    CatalogEntry* this_table = get(variable_pool, filename);
    if (!this_table) {
        //perror("Error retriving column");
        fprintf(stderr, "Error retriving column: %s", filename);
        return -1;
    }

    // Check for collisions
    while (this_table != NULL && this_table->filepath == NULL || strcmp(this_table->filepath, filename) != 0) {
        //fprintf(stdout, "%s\n", (this_table)->filepath);
        this_table = this_table->next;
    }

    if (this_table == NULL || !(this_table->is_column)) {
        perror("Error retrieving correct column");
        return -1;
    }

    // NOW ADD THE CHAR* TO THE END OF THE MEMORY MAPPED FILE

    char* line = malloc(1024);
    strcpy(line, val);
    strcat(line, "\n");

    //fprintf(stdout, "LINE: %s\n", line);

    strcat(this_table->data, line);

    //fprintf(stdout, "DATA: %s\n", this_table->data);
    
    /*

    fprintf(file, "%s\n", val);

    */

   free(line);

    return 0;
}


/**
 * This method transforms the .txt catalog into a CatalogHashtable, which represents an array of CatalogEntry's
 * It takes in an opened .txt catalog object and returns its Hashtable representation
 * Used for scan based queries
 */

CatalogHashtable* populate_catalog(FILE* file) {
    if (file == NULL) {
        perror("Error opening file");
        return NULL;
    }

    // Get the file descriptor
    int fd = fileno(file);

    CatalogHashtable* ht = NULL;
    int size = 10;
    allocate(&ht, size);

    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        perror("fstat");
        deallocate(ht);
        close(fd);
        return NULL;
    }

    char* data = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    char *buffer = malloc(sb.st_size);  // +1 for null-terminator

    if (data == MAP_FAILED) {
        perror("mmap");
        close(fd);
        free(buffer); // Don't forget to free the buffer if mmap fails.
        deallocate(ht);
        return NULL;
    }

    if (!buffer) {
        perror("malloc");
        close(fd);
        free(buffer);
        deallocate(buffer);
        return NULL;
    }

    memcpy(buffer, data, sb.st_size);
    //buffer[sb.st_size] = '\0';  // Null-terminate the buffer

    char* line = NULL;
    char* tmp = buffer;  
    int count = 1;

 
    while ((line = strsep(&tmp, "\n")) != NULL) {
        if (strlen(line) == 0) {
            continue;
        }
        // Create entry for the line
        CatalogEntry* ct = line_to_entry(line, count);

        if (!ct->name)  {
            perror("Couldnt add line");
            munmap(data, sb.st_size);
            close(fd);
            return ht;
        }
        // Add entry to hashtable
        put(ht, *ct);

        free(ct);
        
        // Increase count
        count++;
    }
    munmap(data, sb.st_size);
    free(buffer);
    close(fd);
    return ht;
}

/**
 * This method takes in a string representing the arguments to create a column.
 * It parses those arguments, checks that they are valid, and creates a column - both in the catalog, and a physical file.
 */

DbOperator* parse_create_col(char* create_arguments, CatalogEntry* variable_pool) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* column_name = next_token(create_arguments_index, &status);
    char* table_name = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }
    // Get the table name free of quotation marks
    column_name = trim_quotes(column_name);

    int last_char = strlen(table_name) - 1;
    if (last_char < 0 || table_name[last_char] != ')') {
        return NULL;
    }
    // replace final ')' with null-termination character.
    table_name[last_char] = '\0';

    // join tablename with column name for path
    char* startpath = (char*)malloc(strlen(table_name) + strlen(column_name) + 3);
    strcpy(startpath, table_name);
    strcat(startpath, ".");
    strcat(startpath, column_name);


    // Make pathname, then update catalog
    char* path = makePath(startpath, _COLUMN);

    if (!path) {
        return NULL;
    }

    FILE* file = fopen("catalogue.txt", "a");
    if (!file) {
        perror("Error opening file");
        return NULL;
        }


    fprintf(file, "%s,%s,%s\n", column_name, path, "cl");
    fclose(file);

    // Create actual files. And then create a memory mapped copy to add to the variable pool.
    // If vpool has a full column object then the name/filepath will have a .txt

    strcat(path,".txt");

    int capacity = 10240; // No. of elements that the column can hold
    int line_size = 1024; // Size of line
    size_t full_size = capacity * line_size; // Total size for memory mapping

    // Open the file with read and write permissions, create if it doesn't exist
    int fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        perror("Error opening file");
        return NULL;
    }

    // Extend the file to the desired size
    if (ftruncate(fd, full_size) == -1) {
        perror("Error extending file size");
        close(fd);
        return NULL;
    }

    // Memory map the file
    char* data = mmap(NULL, full_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        perror("Error mapping file");
        close(fd);
        return NULL;
    }

    // Use data[] as an array here, e.g., writing column name at the start
    snprintf(data, line_size, "COL NAME: %s\n", column_name);

    // ... Additional operations on data[] ...

    close(fd);


    // add memory mapped column to catalogue

    CatalogEntry* cat = (CatalogEntry*) malloc(sizeof(CatalogEntry)); //FREE

    strcpy(cat->name, path);
    strcpy(cat->filepath, path); //maybe redundant
    //cat->data = malloc(full_size * sizeof(char));
    //strcpy(cat->data, data);
    cat->data = data;
    cat->data_size = full_size;
    cat->is_column = true;

    put(variable_pool, *cat);

    //fclose(file2);
    free(path);

    // make create dbo for table
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _COLUMN;
    strcpy(dbo->operator_fields.create_operator.name, column_name);
    dbo->operator_fields.create_operator.db = current_db;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/


DbOperator* parse_create_tbl(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }
    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return NULL;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';

    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return NULL;
    }

    char* startpath = (char*)malloc(strlen(db_name) + strlen(table_name) + 2);
    strcpy(startpath, db_name);
    strcat(startpath, ".");
    strcat(startpath, table_name);


    char* path = makePath(startpath, _TABLE);

    if (!path) {
        return NULL;
    }

    // Directory permissions (read, write, execute for owner; read and execute for others)
    mode_t mode = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;

    if (mkdir(path, mode) == -1) {
        if (errno != EEXIST) {
            perror("Error creating directory");
            return NULL;
        }
    }

    FILE* file = fopen("catalogue.txt", "a");
    if (!file) {
        perror("Error opening file");
        return NULL;
        }

    fprintf(file, "%s,%s,%s\n", table_name, path, "tb");
    fclose(file);

    free(path);

    // make create dbo for table
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _TABLE;
    strcpy(dbo->operator_fields.create_operator.name, table_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.col_count = column_cnt;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/


DbOperator* parse_create_db(char* create_arguments) {
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return NULL;
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return NULL;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';
        char* db_name_copy = db_name;
        char* path = makePath(db_name_copy, _DB);
        if (!path) {
            return NULL;
        }

        // Directory permissions (read, write, execute for owner; read and execute for others)
        mode_t mode = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;

        if (mkdir(path, mode) == -1) {
            if (errno != EEXIST) {
                perror("Error creating directory");
                return NULL;
            }
        }

        FILE* file = fopen("catalogue.txt", "a");
        if (!file) {
            perror("Error opening file");
            return NULL;
            }


        fprintf(file, "%s,%s,%s\n", db_name, path, "db");
        fclose(file);


        // make create operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
        dbo->operator_fields.create_operator.create_type = _DB;
        strcpy(dbo->operator_fields.create_operator.name, db_name);

        free(path);
        return dbo;
    }
}
// Function to get name from a '.' seperated string (name will be final value)
char* getName(char* input) {
    if (!input) {
        return NULL;
    }

    // Find the last occurrence of '.'
    const char* last_dot = strrchr(input, '.');
    if (last_dot) {
        return (char*)(last_dot + 1);  // Return the value after the '.'
    } else {
        return input;  // If no '.' is found, return the whole string
    }
}



/* Function for making directory */
char* makePath(char* name, CreateType t) {
    // dbname = name. tablename = dbname.name (split by '.') columnname = dbname.tbname.name
    char *path = malloc(256);  // allocate more memory, or dynamically calculate size
    if (!path) {
        return NULL;
    }

    char *nameCopy = strdup(name);  // work on a copy of the name to avoid modifying original
    char *temp = nameCopy;
    char *token;

    strcpy(path, "./");

    switch (t) {
        case _DB:
            strcat(path, nameCopy);
            break;
            
        case _TABLE:
            token = strsep(&temp, ".");
            if (!token) { free(nameCopy); return NULL; }
            strcat(path, token);
            strcat(path, "/");
            
            token = strsep(&temp, ".");
            if (!token) { free(nameCopy); return NULL; }
            strcat(path, token);
            break;
            
        case _COLUMN:
            token = strsep(&temp, ".");
            if (!token) { free(nameCopy); return NULL; }
            strcat(path, token);
            strcat(path, "/");
            
            token = strsep(&temp, ".");
            if (!token) { free(nameCopy); return NULL; }
            strcat(path, token);
            strcat(path, "/");
            
            token = strsep(&temp, ".");
            if (!token) { free(nameCopy); return NULL; }
            strcat(path, token);
            break;

        default:
            free(nameCopy);
            free(path);
            return NULL;
    }
    
    free(nameCopy);
    return path;
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
DbOperator* parse_create(char* create_arguments, CatalogEntry* variable_pool) {
    message_status mes_status;
    DbOperator* dbo = NULL;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
  
        // pass off to next parse function. 
        if (strcmp(token, "db") == 0) {
            dbo = parse_create_db(tokenizer_copy);
        } else if (strcmp(token, "tbl") == 0) {
            dbo = parse_create_tbl(tokenizer_copy);
        } else if (strcmp(token, "col") == 0){
            dbo = parse_create_col(tokenizer_copy, variable_pool);
        } else {
            mes_status = UNKNOWN_COMMAND;
        }

    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return dbo;
}
// For 'insert'
int add_element_to_file(char* fname, char* abspath, char* val, CatalogHashtable* variable_pool) {
    char *fullpath = malloc(100);
    strcpy(fullpath, abspath);
    strcat(fullpath, "/");
    strcat(fullpath, fname);

    add_element_for_load(fullpath, val, variable_pool);

    /*
    FILE* file = fopen(fullpath, "a");
    if (!file) {
        perror("Error opening file");
        return -1;
        }
    fprintf(file, "%s\n", val);
    fclose(file);
    */
    free(fullpath);
    return 0;
}



/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message, CatalogHashtable* variable_pool) {
    //unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* table_name = next_token(command_index, &send_message->status);
        //char* catname = getName(table_name);
        char* catpath = makePath(table_name, _TABLE);

        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        /*

        // find entry for this table in catalog
        FILE* file = fopen("catalogue.txt", "r");
        if (!file) {
            // handle error, free catname and catpath
            free(catpath);
            return NULL;
        }
        CatalogHashtable* ht = populate_catalog(file);
        fclose(file);

        CatalogEntry* this_table = get(ht, catname);

        if (!this_table) {
            free(catpath);
            return NULL;
        }

        // Check for collisions
        while (strcmp((*this_table).filepath, catpath) != 0) {
            this_table = this_table->next;
        }

        char* path = (*this_table).filepath;
        */

        char* path = catpath;

        // find paths of columns within this table
        struct dirent *entry;
        DIR *dir = opendir(path);

        if (dir == NULL) {
            perror("Unable to open directory");
            return NULL;
        }

        while ((entry = readdir(dir)) != NULL) {
            // Check if the entry is a regular file -> need to account for final ')' at some point
            if (entry->d_type == DT_REG) {
                token = strsep(command_index, ",");
                if (strchr(token, ')')) {
                    *strchr(token, ')') = '\0';
                }
                add_element_to_file(entry->d_name, path, token, variable_pool);
            }
        }
        free(catpath);
        closedir(dir);

        DbOperator* dbo = malloc(sizeof(DbOperator));
 
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

// char* get_array(char* filepath, )

DbOperator* parse_load_insert(char* query_command, message* send_message, CatalogHashtable* variable_pool, ClientContext* context) {

    // IF WE ARE ALREADY IN A LOAD, just run the add function with colname (txt will be added) and value

    // IF WE ARE NOT IN A LOAD, MEMORY MAP THE RELAVENT FILES? Then do regular thing?

    // FOR NOW, WE ASSUME THAT A LOAD WILL FOLLOW A CREATE COLUMN AND SO OUR COLUMN WILl BE IN VPOOL

    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* table_name = next_token(command_index, &send_message->status);
        char* val = next_token(command_index, &send_message->status);
        val = trim_parenthesis(val);
        char* catpath = makePath(table_name, _TABLE);
        strcat(catpath, ".txt");
        

        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        // find entry for this table in catalog
        FILE* file = fopen("catalogue.txt", "r");
        if (!file) {
            // handle error, free catname and catpath
            free(catpath);
            return NULL;
        }

        fclose(file);

        CatalogEntry* this_table = get(variable_pool, catpath);

        if (!this_table) {
            free(catpath);
            fprintf(stdout, "COULDT FIND COLUMN!!\n");
            return NULL;
        }

        // Check for collisions
        while (strcmp((*this_table).filepath, catpath) != 0) {
            this_table = this_table->next;
        }

        add_element_for_load(catpath, val, variable_pool);

        DbOperator* dbo = malloc(sizeof(DbOperator));
 
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}





// helper functions for parse select - to decide if something is a column or a vector
bool contains_dot(const char *str) {
    return strchr(str, '.') != NULL;
}


DbOperator* parse_select(char* query_command, char* handle, message* send_message, CatalogHashtable* variable_pool) {
    if (strncmp(query_command, "(", 1) != 0) {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command++;
    char** command_index = &query_command;
    
    
    // parse table input
    char* arg1 = next_token(command_index, &send_message->status);
    if (contains_dot(arg1)) {
        // Dealing with column
        char* name = getName(arg1);
        char* catpath = makePath(arg1, _COLUMN);
        /*
        // retrieve filename from catalog
        FILE* file1 = fopen("catalogue.txt", "r");
        CatalogHashtable* ht = populate_catalog(file1);
        CatalogEntry* this_table = get(ht, name);

        // Check for collisions
        while (strcmp((*this_table).filepath, catpath) != 0) {
            this_table = this_table->next;
        }

        if (!this_table) {
            //perror("Error retriving column");
            fprintf(stderr, "Error retriving column: %s", name);
            return NULL;
        }
        char* fullpath = (*this_table).filepath;
        */
        char* fullpath = catpath;
        strcat(fullpath, ".txt");
        // open column file
        FILE* file = fopen(fullpath, "r");
        if (!file) {
            perror("Error opening file");
            return NULL;
            }

        char* low = next_token(command_index, &send_message->status);
        char* high = next_token(command_index, &send_message->status);
        high = trim_parenthesis(high);
        int ilow;
        int ihigh;
        if (strcmp(low, "null") == 0) {
            ilow = INT_MIN;
        } else {
            ilow = atoi(low);
        }
        if (strcmp(high, "null") == 0) {
            ihigh = INT_MAX;
        } else {
            ihigh = atoi(high);
        }

        char line[1024];
        // Skip the first line

        if (!fgets(line, sizeof(line), file)) {
            perror("Error reading file");
            fclose(file);
            return NULL;
        }


        CatalogEntry* cat = (CatalogEntry *) malloc(sizeof(CatalogEntry));
        if (!cat) {
            perror("Failed to allocate memory for CatalogEntry");
            return NULL;
        }
        strcpy(cat->name, handle);
        


        int count = 0;
        // Now loop through each subsequent line
        while (fgets(line, sizeof(line), file)) {
            fprintf(stdout, "SELECT LINE IS: %s", line);
            if (strncmp(line, "\0", 1) == 0) {
                break;
            }
            // Remove the newline character, if present
            size_t len = strlen(line);
            if (len > 0 && line[len - 1] == '\n') {
                line[len - 1] = '\0';
            }
            int lineval = atoi(line);
            // Now 'line' contains the current line from the file without the newline character
            // For our bitvector, false === INT_MIN and true === INT_MAX (this allows us to use the bitvector object for a value vector)
            int val;
            if (lineval < ihigh && lineval >= ilow) {
                fprintf(stdout, "%i IS between %i and %i", lineval, ilow, ihigh);
                val = INT_MAX;
            }
            else {
                fprintf(stdout, "%i IS NOT between %i and %i", lineval, ilow, ihigh);
                val = INT_MIN;
            }
            cat->bitvector[count] = val;
            count++;

        }
        cat->size = count;
        cat->in_vpool = true;
        
        fclose(file);
        put(variable_pool, *cat);
    }
    else {
        // Dealing with pos vector
        CatalogEntry* pvector = get(variable_pool, arg1);
        char* vvector_name = next_token(command_index, &send_message->status);
        CatalogEntry* vvector = get(variable_pool, vvector_name);
        int size = pvector->size;

        char* low = next_token(command_index, &send_message->status);
        char* high = next_token(command_index, &send_message->status);
        high = trim_parenthesis(high);
        int ilow;
        int ihigh;
        if (!low) {
            ilow = INT_MIN;
        } else {
            ilow = atoi(low);
        }
        if (!high) {
            ihigh = INT_MAX;
        } else {
            ihigh = atoi(high);
        }

        CatalogEntry* cat = (CatalogEntry *) malloc(sizeof(CatalogEntry));
        if (!cat) {
            perror("Failed to allocate memory for CatalogEntry");
            return NULL;
        }
        strcpy(cat->name, handle);
        cat->size = size;

        // assume that both vectors are the same size (one is treated as a bit vector and one as val vector)
        for (int i=0; i<size; i++) {
            int val;
            if (pvector->bitvector[i] != INT_MIN && (vvector->bitvector[i] > ilow && vvector->bitvector[i] < ihigh)) {
                val = INT_MAX;
            }
            else {
                val = INT_MIN;
            }
            cat->bitvector[i] = val;
        }
        cat->in_vpool = true;
        put(variable_pool, *cat);
    }

    DbOperator* dbo = malloc(sizeof(DbOperator));
    return dbo;
}


/*
MULTITHREADING THE SELECT
*/

void* threadFunction(void* arg) {
    ThreadArgs* threadArgs = (ThreadArgs*) arg;
    if (threadArgs->is_column == true) {

        // Open the file (consider thread-safe mechanisms or separate file pointers)
        FILE* file = fopen(threadArgs->filepath, "r");
        if (!file) {
            perror("Error opening file");
            return NULL;
        }
        // Process the assigned lines
        char line[1024];
        fseek(file, threadArgs->startOffset, SEEK_SET); // Adjust file position
        if (fseek(file, threadArgs->startOffset, SEEK_SET) != 0) {
            perror("Seek error");
        }

        long currentOffset = threadArgs->startOffset;
        int currentLine = threadArgs->startLine;
        while (currentOffset <= threadArgs->endOffset) {
             if (fgets(line, sizeof(line), file) == NULL) {
                // Check for end-of-file versus an error
                if (feof(file)) {
                    break; // End of file reached
                } else {
                    perror("Error reading file");
                    fclose(file);
                    return NULL;
                }
            }
            else {

                //fprintf(stdout, "SELECT LINE IS: %s", line);
                /*
                if (strncmp(line, "\0", 1) == 0) {
                    break;
                }
                */
                // Remove the newline character, if present
                size_t len = strlen(line);
                if (len > 0 && line[len - 1] == '\n') {
                    line[len - 1] = '\0';
                }
                int lineval = atoi(line);
                // Now 'line' contains the current line from the file without the newline character
                // For our bitvector, false === INT_MIN and true === INT_MAX (this allows us to use the bitvector object for a value vector)
                int val;
                if (lineval < threadArgs->ihigh && lineval >= threadArgs->ilow) {
                    //fprintf(stdout, "%i IS between %i and %i. So line %i is a YES!\n", lineval, threadArgs->ilow, threadArgs->ihigh, currentLine-1);
                    val = INT_MAX;
                }
                else {
                    //fprintf(stdout, "%i IS NOT between %i and %i. So line %i is a NO!\n", lineval, threadArgs->ilow, threadArgs->ihigh, currentLine-1);
                    val = INT_MIN;
                }
                threadArgs->bitvector[currentLine-1] = val;
                currentLine++;
                if (file == NULL) {
                    perror("FILE CORRUPTED");
                    return NULL;
                }
                if (ferror(file)) {
                    perror("FILE CORRUPTED");
                    return NULL;
                }
                currentOffset = ftell(file);
                if (currentOffset == -1L) {
                    perror("ftell failed");
                    fclose(file);
                    return NULL;
                }
            }
        }

        if (fclose(file) != 0) {
            perror("Error closing file");
            return NULL;
        }
    } 
    else {

        // assume that both vectors are the same size (one is treated as a bit vector and one as val vector)
        for (int i=threadArgs->startLine; i<threadArgs->endLine; i++) {
            int val;
            if (threadArgs->pvector[i] != INT_MIN && (threadArgs->vvector[i] > threadArgs->ilow && threadArgs->vvector[i] < threadArgs->ihigh)) {
                fprintf(stdout, "%i IS between %i and %i. So line %i is a YES!\n", threadArgs->vvector[i], threadArgs->ilow, threadArgs->ihigh,i);
                val = INT_MAX;
            }
            else {
                val = INT_MIN;
                fprintf(stdout, "%i IS NOT between %i and %i. So line %i is a NO!\n", threadArgs->vvector[i], threadArgs->ilow, threadArgs->ihigh,i);
            }
            threadArgs->bitvector[i] = val;
        }

    }
    //fprintf(stdout, "DO I FINISH ANY THREAD?\n");
    return NULL;
}

DbOperator* parse_select_multithread(char* query_command, char* handle, message* send_message, CatalogHashtable* variable_pool) {
    if (strncmp(query_command, "(", 1) != 0) {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command++;
    char** command_index = &query_command;

    int threadCount = 4; // Number of threads
    pthread_t threads[threadCount];
    ThreadArgs args[threadCount];
    
    // parse table input
    char* arg1 = next_token(command_index, &send_message->status);
    if (contains_dot(arg1)) {
        // Dealing with column
        char* name = getName(arg1);
        char* catpath = makePath(arg1, _COLUMN);
        char* fullpath = catpath;
        strcat(fullpath, ".txt");
        // open column file
        FILE* file = fopen(fullpath, "r");
        if (!file) {
            perror("Error opening file");
            return NULL;
            }

        char* low = next_token(command_index, &send_message->status);
        char* high = next_token(command_index, &send_message->status);
        high = trim_parenthesis(high);
        int ilow;
        int ihigh;
        if (strcmp(low, "null") == 0) {
            ilow = INT_MIN;
        } else {
            ilow = atoi(low);
        }
        if (strcmp(high, "null") == 0) {
            ihigh = INT_MAX;
        } else {
            ihigh = atoi(high);
        }

        char line[1024];
        // Skip the first line
        /*
        if (!fgets(line, sizeof(line), file)) {
            perror("Error reading file");
            fclose(file);
            return NULL;
        }
        */
        int lineCount = 0;
        long *lineOffsets = malloc(sizeof(long) * 102400);
        long currentOffset = 0;
        // Count lines
        while (fgets(line, sizeof(line), file)) {
            if (strncmp(line, "\0", 1) == 0) {
                break;
            }
            lineOffsets[lineCount] = currentOffset;
            //fprintf(stdout, "LINE OFFSET NUMBER: %i is at offset %i, and has val %s", lineCount, currentOffset, line);
            currentOffset = ftell(file);
            lineCount++;
        }
        
        fclose(file);

        int nums[5];
        nums[0] = 1;
        nums[1] = lineCount / 4;
        nums[2] = (lineCount / 4) * 2;
        nums[3] = (lineCount / 4) * 3;
        nums[4] = lineCount-1;


        // Split the file into portions and initialize thread arguments
        for (int i = 0; i < threadCount; ++i) {
            args[i].startLine = nums[i];
            args[i].endLine = nums[i+1];
            args[i].startOffset = lineOffsets[nums[i]]; // Calculate start line for this thread
            args[i].endOffset = lineOffsets[nums[i+1]]; // Calculate end line for this thread
            //fprintf(stdout, "THREAD %i is %i (offset %i) to %i (offset %i)\n", i, nums[i], lineOffsets[nums[i]], nums[i+1], lineOffsets[nums[i+1]]);
            strcpy(args[i].filepath, fullpath);
            args[i].is_column = true;
            strcpy(args[i].handle, handle);
            args[i].ihigh = ihigh;
            args[i].ilow = ilow;
            args[i].bitvector = malloc((lineOffsets[nums[i+1]] - lineOffsets[nums[i]]) * sizeof(int));
            // Initialize other necessary fields

            // Create the thread
            if (pthread_create(&threads[i], NULL, threadFunction, &args[i])) {
                perror("Failed to create thread");
            }
        }

        CatalogEntry* cat = (CatalogEntry *) malloc(sizeof(CatalogEntry));
        if (!cat) {
            perror("Failed to allocate memory for CatalogEntry");
            return NULL;
        }

        // Wait for threads and aggregate results
        int* finalBitvector = malloc(lineCount * sizeof(int));
        int finalIndex = 0;
        for (int i = 0; i < threadCount; ++i) {
            pthread_join(threads[i], NULL);
            /*
            int segmentSize = nums[i+1] - nums[i];
            memcpy(finalBitvector + finalIndex, args[i].bitvector, segmentSize * sizeof(int));
            finalIndex += segmentSize;
            */
            for (int j=args[i].startLine-1; j<args[i].endLine; ++j) {
                if (args[i].bitvector[j] == INT_MAX) {
                    cat->bitvector[j] = INT_MAX;
                }
                else {
                    cat->bitvector[j] = INT_MIN;
                }
                finalBitvector[j] = args[i].bitvector[j];
            }
            free(args[i].bitvector);  // Free the individual bitvector
        }

        // TODO: GET RESULTS AND JOIN THEM
        


        strcpy(cat->name, handle);
        /*
        fprintf(stdout, "THIS IS THE FINAL BITVECTOR:\n");
        for (int i=0; i<lineCount; i++) {
            if (finalBitvector[i] == INT_MAX) {
                fprintf(stdout, "Line %i is a YES\n", i);
                cat->bitvector[i] = INT_MAX;
            }
            else {
                fprintf(stdout, "Line %i is a NO\n", i);
                cat->bitvector[i] = INT_MIN;
            }
        }
        */

        cat->size = lineCount;
        cat->in_vpool = true;
        put(variable_pool, *cat);
        free(finalBitvector);

    }

    else {
        // Dealing with pos vector
        CatalogEntry* pvector = get(variable_pool, arg1);
        char* vvector_name = next_token(command_index, &send_message->status);
        CatalogEntry* vvector = get(variable_pool, vvector_name);
        int size = pvector->size;

        char* low = next_token(command_index, &send_message->status);
        char* high = next_token(command_index, &send_message->status);
        high = trim_parenthesis(high);
        int ilow;
        int ihigh;
        if (!low) {
            ilow = INT_MIN;
        } else {
            ilow = atoi(low);
        }
        if (!high) {
            ihigh = INT_MAX;
        } else {
            ihigh = atoi(high);
        }

        int count = size;

        int nums[5];
        nums[0] = 0;
        nums[1] = count / 4;
        nums[2] = (count / 4) * 2;
        nums[3] = (count / 4) * 3;
        nums[4] = count;


        // Split the file into portions and initialize thread arguments
        for (int i = 0; i < threadCount; ++i) {
            args[i].startLine = nums[i]; // Calculate start line for this thread
            args[i].endLine = nums[i+1]; // Calculate end line for this thread
            fprintf(stdout, "Thread %i is line %i to %i\n", i, nums[i], nums[i+1]);
            args[i].is_column = false;
            strcpy(args[i].handle, handle);
            args[i].ihigh = ihigh;
            args[i].ilow = ilow;
            //args[i].pvector = pvector->bitvector;
            memcpy(args[i].pvector, pvector->bitvector, sizeof(pvector->bitvector));
            //args[i].vvector = vvector->bitvector;
            memcpy(args[i].vvector, vvector->bitvector, sizeof(vvector->bitvector));
            // Initialize other necessary fields
            args[i].bitvector = malloc((count) * sizeof(int));
            //memcpy(args[i].bitvector, vvector->bitvector, count * sizeof(int));

            // Create the thread
            if (pthread_create(&threads[i], NULL, threadFunction, &args[i])) {
                perror("Failed to create thread");
            }
        }

        CatalogEntry* cat = (CatalogEntry *) malloc(sizeof(CatalogEntry));
        if (!cat) {
            perror("Failed to allocate memory for CatalogEntry");
            return NULL;
        }

        // Wait for threads and aggregate results
        int* finalBitvector = malloc(count * sizeof(int));
        int finalIndex = 0;
        for (int i = 0; i < threadCount; ++i) {
            pthread_join(threads[i], NULL);
            /*
            int segmentSize = nums[i+1] - nums[i];
            memcpy(finalBitvector + finalIndex, args[i].bitvector, segmentSize * sizeof(int));
            finalIndex += segmentSize;
            */
            for (int j=args[i].startLine; j<args[i].endLine; ++j) {
                if (args[i].bitvector[j] == INT_MAX) {
                    cat->bitvector[j] = INT_MAX;
                }
                else {
                    cat->bitvector[j] = INT_MIN;
                }
                finalBitvector[j] = args[i].bitvector[j];
            }
            free(args[i].bitvector);  // Free the individual bitvector
        }

        // TODO: GET RESULTS AND JOIN THEM
        
        

        fprintf(stdout, "AM I HERE ?");
        strcpy(cat->name, handle);
        //memcpy(cat->bitvector, finalBitvector, (count * sizeof(int)));
        cat->size = count;
        cat->in_vpool = true;
        put(variable_pool, *cat);
        free(finalBitvector);

    }

    DbOperator* dbo = malloc(sizeof(DbOperator));
    return dbo;
}



DbOperator* parse_fetch(char* query_command, char* handle, message* send_message, CatalogHashtable* variable_pool) {
    if (strncmp(query_command, "(", 1) != 0) {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command++;
    char** command_index = &query_command;
    
    
    // parse table input
    char* colname = next_token(command_index, &send_message->status);
    char* bitvname = next_token(command_index, &send_message->status);
    bitvname = trim_parenthesis(bitvname);

    char* name = getName(colname);
    char* catpath = makePath(colname, _COLUMN);

    /*
    // retrieve filename from catalog
    FILE* file1 = fopen("catalogue.txt", "r");
    CatalogHashtable* ht = populate_catalog(file1);
    CatalogEntry* this_table = get(ht, name);

    if (!this_table) {
        //perror("Error retriving column");
        fprintf(stderr, "Error retriving column: %s", name);
        return NULL;
    }

    // Check for collisions
    while (strcmp((*this_table).filepath, catpath) != 0) {
        this_table = this_table->next;
    }

    char* fullpath = (*this_table).filepath;
    */
    char* fullpath = catpath;
    strcat(fullpath, ".txt");
    // open column file
    FILE* file = fopen(fullpath, "r");
    if (!file) {
        perror("Error opening file");
        return NULL;
        }
    // retrieve bitvector
    CatalogEntry* pvector = get(variable_pool, bitvname);

 

    char line[1024];
    // Skip the first line
    if (!fgets(line, sizeof(line), file)) {
        perror("Error reading file");
        fclose(file);
        return NULL;
    }

    CatalogEntry* cat = (CatalogEntry *) malloc(sizeof(CatalogEntry));
    if (!cat) {
        perror("Failed to allocate memory for CatalogEntry");
        return NULL;
    }
    strcpy(cat->name, handle);

    int count = 0;
    // Now loop through each subsequent line
    while (fgets(line, sizeof(line), file)) {
        //fprintf(stdout, "LINE IS: %s\n", line);
        //fprintf(stdout, "PVECTOR NAME IS: %s\n", pvector->name);
        if (strncmp(line, "\0", 1) == 0) {
            break;
        }
        // Remove the newline character, if present
        size_t len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') {
            line[len - 1] = '\0';
        }
        int lineval = atoi(line);
        
        // Now 'line' contains the current line from the file without the newline character
        if (pvector->bitvector[count] != INT_MIN) {
            //fprintf(stdout, "THIS IS!!! IN THE SET\n");
            cat->bitvector[count] = lineval;
        }
        else {
            cat->bitvector[count] = INT_MIN;
            //fprintf(stdout, "THIS IS NOT! IN THE SET\n");
        }
        count++;
    }
    cat->size = count;
    cat->in_vpool = true;
    cat->has_value = false;

    put(variable_pool, *cat);

    DbOperator* dbo = malloc(sizeof(DbOperator));
    return dbo;
}

int print_column(char* token) {
    char* name = getName(token);
    // retrieve filename from catalog
    FILE* file1 = fopen("catalogue.txt", "r");
    CatalogHashtable* ht = populate_catalog(file1);
    CatalogEntry* this_table = get(ht, name);

    if (!this_table) {
        //perror("Error retriving column");
        fprintf(stderr, "Error retriving column: %s", name);
        return 0;
    }
    char* fullpath = (*this_table).filepath;
    strcat(fullpath, ".txt");
    // open column file
    FILE* file = fopen(fullpath, "r");
    if (!file) {
        perror("Error opening file");
        return 0;
        }

    FILE *combinedFile = fopen("combined_data.txt", "a");

    char line[1024];
    //skip first line
    if (!fgets(line, sizeof(line), file)) {
        perror("Error reading file");
        fclose(file);
        return 0;
    }

    while (fgets(line, sizeof(line), file)) {
        fputs(line, combinedFile);
        fputc(',', combinedFile);
    }
    fclose(file1);
    fclose(file);
    fclose(combinedFile);
    return 1;
}   

int print_vector(char* token, CatalogHashtable* variable_pool) {
    CatalogEntry* vvector = get(variable_pool, token);
    FILE *combinedFile = fopen("combined_data.txt", "a");
    if (!combinedFile) {
        perror("Error opening combined file");
        return 0;
    }
    // check if it simply a return value and not a full vector
    if (vvector->has_value == true) {
        fprintf(stdout, "THIS HAS A VALUE UH OH!!!");
        char buffer[12];
        if (vvector->value == (int) vvector->value) {
            snprintf(buffer, sizeof(buffer), "%d", (int)vvector->value);
        }
        else {
            snprintf(buffer, sizeof(buffer), "%.2f", vvector->value);
        }
        fputs(buffer, combinedFile);
        //fputc(',', combinedFile);
    }
    else {
        int size = vvector->size;
        fprintf(stdout, "SIZE IS: %i", size);
        for (int i=0; i<size; i++) {
            if (vvector->bitvector[i] != INT_MIN && vvector->bitvector[i] != INT_MAX) {
                char buffer[12];
                snprintf(buffer, sizeof(buffer), "%d", vvector->bitvector[i]);
                /*log_err(buffer);*/
                fputs(buffer, combinedFile);
                fputs("\n", combinedFile);
            }
        }
    }
    //fputc('\n', combinedFile);

    long pos = ftell(combinedFile);
    if (pos > 0) {
        fseek(combinedFile, -1, SEEK_CUR);  // Move back one character
        fputc('\n', combinedFile);          // Replace comma with newline
    }

    fclose(combinedFile);
    return 1;
}

/*
parse_print will print out one or more vectors in tabular format (X,Y,Z...)
takes in a column name or a vector name 
*/
//TODO: HANDLE MULTIPLE FILES PROPERLY
char* parse_print(char* query_command, message* send_message, CatalogHashtable* variable_pool) {
    if (strncmp(query_command, "(", 1) != 0) {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command++;
    char** command_index = &query_command;
    char* token = next_token(command_index, &send_message->status);
    int last_char = strlen(token) - 1;
    // Clear combined data
    FILE *clearCombined = fopen("combined_data.txt", "w");
    if (!clearCombined) {
        perror("Error opening combined file");
        return NULL;
    }
    fclose(clearCombined);
    while (last_char < 0 || token[last_char] != ')') {
        if (contains_dot(token)) {
            fprintf(stdout, "PRINTING A COLUMN!!!");
            print_column(token);
        }
        else {
            fprintf(stdout, "PRINTING A VECTOR!!!");
            print_vector(token, variable_pool);
        }
        token = next_token(command_index, &send_message->status);
        last_char = strlen(token) - 1;

        if (token && token[0] != ')') {
            // Not the last token, add a comma
            FILE *combinedFile = fopen("combined_data.txt", "a");
            fputc(',', combinedFile);
            fclose(combinedFile);
        }

    }
    token = trim_parenthesis(token);
    if (contains_dot(token)) {
        fprintf(stdout, "PRINTING A COLUMN!!!");
        print_column(token);
    }
    else {
        fprintf(stdout, "PRINTING A VECTOR!!!");
        print_vector(token, variable_pool);
    }

    // Open the combined file for reading
    FILE *readCombined = fopen("combined_data.txt", "r");
    if (!readCombined) {
        perror("Error opening combined file");
        return NULL;
    }

    // Seek to the end of the file to determine the file size
    fseek(readCombined, 0, SEEK_END);
    int filelen = ftell(readCombined);
    // Reset the file position indicator to the beginning of the file
    rewind(readCombined);

    // Allocate memory for the entire content
    char* ret_buffer = (char*)malloc((filelen + 1) * sizeof(char));

    // Read the file into the buffer
    size_t readLength = fread(ret_buffer, 1, filelen, readCombined);
    if (readLength != filelen) {
        // Error handling for partial read
        perror("Error reading file");
        free(ret_buffer);
        fclose(readCombined);
        return NULL;
    }

    // Null-terminate the buffer
    ret_buffer[filelen] = '\0';

    // Iterate through the buffer and remove newlines directly before a comma
    for (int i = 0; i < filelen; i++) {
        if (ret_buffer[i] == '\n' && ret_buffer[i + 1] == ',') {
            // Shift the rest of the string one character to the left
            memmove(&ret_buffer[i], &ret_buffer[i + 1], filelen - i);
        }
    }

    // Close the file when done
    fclose(readCombined);
    return ret_buffer;
}


DbOperator* parse_avg(char* query_command, char* handle, message* send_message, CatalogHashtable* variable_pool) {
    if (strncmp(query_command, "(", 1) != 0) {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command++;
    char** command_index = &query_command;
    
    
    // parse table input
    char* arg1 = next_token(command_index, &send_message->status);
    arg1 = trim_parenthesis(arg1);
    CatalogEntry* pvector;

    //TODO: IN THE CASE THAT WE WANT AVERAGE OF A COLUMN - SO LOOK FOR IT IN CATALOG THEN DO SAME THING
    float ret;
    int sum = 0;
    int div = 0;
    int count;
    // If column
    if (contains_dot(arg1)) {
        char* name = getName(arg1);
        
        // retrieve filename from catalog
        FILE* file1 = fopen("catalogue.txt", "r");
        CatalogHashtable* ht = populate_catalog(file1);
        pvector = get(ht, name);
        char* catpath = makePath(arg1, _COLUMN);

        while (strcmp((*pvector).filepath, catpath) != 0) {
            pvector = pvector->next;
        }

        char* fullpath = (*pvector).filepath;
        strcat(fullpath, ".txt");

        // open column file
        FILE* file = fopen(fullpath, "r");
        if (!file) {
            perror("Error opening file");
            return NULL;
            }
        char line[1024];
        //skip first line
        if (!fgets(line, sizeof(line), file)) {
            perror("Error reading file");
            fclose(file);
            return 0;
        }
        //loop through the rest
        while (fgets(line, sizeof(line), file)) {
            size_t len = strlen(line);
            if (len > 0 && line[len - 1] == '\n') {
                line[len - 1] = '\0';
            }
            int lineval = atoi(line);
            if (lineval > INT_MIN && lineval < INT_MAX) {
                sum += lineval;
                div++;
            }
        }
        // close column file
        fclose(file1);
    }
    // If value vector
    else {
        // get value vector
        pvector = get(variable_pool, arg1);
        int count = pvector->size;
        for (int i=0; i< count; i++) {
            if (pvector->bitvector[i] < INT_MAX && pvector->bitvector[i] > INT_MIN) {
                sum += pvector->bitvector[i];
                div += 1;
            }
        }
    }
    

    // ret now has average
    ret = (float) sum / div;

    CatalogEntry* cat = (CatalogEntry *) malloc(sizeof(CatalogEntry));

    if (!cat) {
        perror("Failed to allocate memory for CatalogEntry");
        return NULL;
    }

    strcpy(cat->name, handle);
    cat->has_value = true;
    cat->value = ret;
    cat->in_vpool = true;
    put(variable_pool, *cat);

    DbOperator* dbo = malloc(sizeof(DbOperator));
    return dbo;
}

DbOperator* parse_sum(char* query_command, char* handle, message* send_message, CatalogHashtable* variable_pool) {
    if (strncmp(query_command, "(", 1) != 0) {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command++;
    char** command_index = &query_command;
    
    // parse table input
    char* arg1 = next_token(command_index, &send_message->status);
    arg1 = trim_parenthesis(arg1);
    CatalogEntry* pvector;

    int ret = 0;
    // if column
    if (contains_dot(arg1)) {
        char* name = getName(arg1);
        
        // retrieve filename from catalog
        FILE* file1 = fopen("catalogue.txt", "r");
        CatalogHashtable* ht = populate_catalog(file1);
        pvector = get(ht, name);

        char* catpath = makePath(arg1, _COLUMN);

        while (strcmp((*pvector).filepath, catpath) != 0) {
            pvector = pvector->next;
        }

        char* fullpath = (*pvector).filepath;
        strcat(fullpath, ".txt");

        // open column file
        FILE* file = fopen(fullpath, "r");
        if (!file) {
            perror("Error opening file");
            return NULL;
            }
        char line[1024];
        //skip first line
        if (!fgets(line, sizeof(line), file)) {
            perror("Error reading file");
            fclose(file);
            return 0;
        }
        //loop through the rest
        while (fgets(line, sizeof(line), file)) {
            size_t len = strlen(line);
            if (len > 0 && line[len - 1] == '\n') {
                line[len - 1] = '\0';
            }
            int lineval = atoi(line);
            if (lineval > INT_MIN && lineval < INT_MAX) {
                ret += lineval;
            }
        }
        // close column file
        fclose(file1);
    }
    // if vector
    else {
        pvector = get(variable_pool, arg1);
        int count = pvector->size;

        for (int i=0; i< count; i++) {
            if (pvector->bitvector[i] < INT_MAX && pvector->bitvector[i] > INT_MIN) {
                ret += pvector->bitvector[i];
            }
        }
    }

    // ret now has sum
    CatalogEntry* cat = (CatalogEntry *) malloc(sizeof(CatalogEntry));
    if (!cat) {
        perror("Failed to allocate memory for CatalogEntry");
        return NULL;
    }

    strcpy(cat->name, handle);
    cat->has_value = true;
    cat->value = ret;
    cat->in_vpool = true;
    put(variable_pool, *cat);

    DbOperator* dbo = malloc(sizeof(DbOperator));
    return dbo;
}

DbOperator* parse_max(char* query_command, char* handle, message* send_message, CatalogHashtable* variable_pool) {
    if (strncmp(query_command, "(", 1) != 0) {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command++;
    char** command_index = &query_command;
    
    
    // parse table input
    char* arg1 = next_token(command_index, &send_message->status);
    int length = strlen(arg1);

    // only one parameter
    if (arg1[length-1] == ')') {
        int ret = INT_MIN;

        arg1 = trim_parenthesis(arg1);
        if (!contains_dot(arg1)) {
            // get value vector
            CatalogEntry* vvector = get(variable_pool, arg1);

            int count = vvector->size;

            for (int i=0; i < count; i++) {
                if (vvector->bitvector[i] < INT_MAX && vvector->bitvector[i] > INT_MIN) {
                    int temp = vvector->bitvector[i];
                    if (temp >= ret) {
                        ret = temp;          
                    }
                }
            }
        }
        else {
            char* name = getName(arg1);
            
            // retrieve filename from catalog
            FILE* file1 = fopen("catalogue.txt", "r");
            CatalogHashtable* ht = populate_catalog(file1);
            CatalogEntry* pvector = get(ht, name);

            char* catpath = makePath(arg1, _COLUMN);

            while (strcmp((*pvector).filepath, catpath) != 0) {
                pvector = pvector->next;
            }

            char* fullpath = (*pvector).filepath;
            strcat(fullpath, ".txt");

            // open column file
            FILE* file = fopen(fullpath, "r");
            if (!file) {
                perror("Error opening file");
                return NULL;
                }
            char line[1024];
            //skip first line
            if (!fgets(line, sizeof(line), file)) {
                perror("Error reading file");
                fclose(file);
                return 0;
            }
            //loop through the rest
            while (fgets(line, sizeof(line), file)) {
                size_t len = strlen(line);
                if (len > 0 && line[len - 1] == '\n') {
                    line[len - 1] = '\0';
                }
                int lineval = atoi(line);
                if (lineval > INT_MIN && lineval < INT_MAX) {
                    if (lineval >= ret) {
                        ret = lineval;
                    }
                }
            }
            // close column file
            fclose(file1);   
        }


        // Object for first return val
        CatalogEntry* cat = (CatalogEntry *) malloc(sizeof(CatalogEntry));
        if (!cat) {
            perror("Failed to allocate memory for CatalogEntry");
            return NULL;
        }

        strcpy(cat->name, handle);
        cat->value = ret;
        cat->has_value = true;
        cat->in_vpool = true;
        put(variable_pool, *cat);
    }
    else {
        char** handle_index = &handle;
        char* handle_first = next_token(handle_index, &send_message->status);
        char* handle_second = next_token(handle_index, &send_message->status);

        // Val vector is arg2, pos vector is arg1
        char* arg2 = next_token(command_index, &send_message->status);
        arg2 = trim_parenthesis(arg2);

        CatalogEntry* vvector = get(variable_pool, arg2);

        int ret = INT_MIN;
        int count = vvector->size;
        
        // If we are search through all of the vvector
        if (strncmp(arg1, "null", 4) == 0) {
            for (int i=0; i < count; i++) {
                int temp = vvector->bitvector[i];
                if (temp >= ret && temp < INT_MAX) {
                    ret = temp;          
                }
            }
        }
        else {
            CatalogEntry* pvector = get(variable_pool, arg1);
            for (int i=0; i < count; i++) {
                int temp = vvector->bitvector[i];
                if (temp >= ret && temp < INT_MAX && pvector->bitvector[i] == INT_MAX) {
                    ret = temp;          
                }
            }
        }

        // Object for first return val
        CatalogEntry* positionlist = (CatalogEntry *) malloc(sizeof(CatalogEntry));
        if (!positionlist) {
            perror("Failed to allocate memory for CatalogEntry");
            return NULL;
        }

        // Find pos of all max values -> TODO: Technically this is searching through too many values in the second case
        for (int i=0; i < count; i++) {
            int temp = vvector->bitvector[i];
            if (temp == ret) {
                positionlist->bitvector[i] = INT_MAX;          
            }
            else {
                positionlist->bitvector[i] = INT_MIN;
            }
        }

        // Object for second return val
        CatalogEntry* maxvalues = (CatalogEntry *) malloc(sizeof(CatalogEntry));
        if (!maxvalues) {
            perror("Failed to allocate memory for CatalogEntry");
            return NULL;
        }

        strcpy(positionlist->name, handle_first);
        positionlist->size = count;

        strcpy(maxvalues->name, handle_second);
        maxvalues->value = ret;
        maxvalues->has_value = true;

        positionlist->in_vpool = true;
        maxvalues->in_vpool = true;

        put(variable_pool, *positionlist);
        put(variable_pool, *maxvalues);
    }

    
    DbOperator* dbo = malloc(sizeof(DbOperator));
    return dbo;
}

DbOperator* parse_min(char* query_command, char* handle, message* send_message, CatalogHashtable* variable_pool) {
    if (strncmp(query_command, "(", 1) != 0) {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command++;
    char** command_index = &query_command;
    
    
    // parse table input
    char* arg1 = next_token(command_index, &send_message->status);
    int length = strlen(arg1);

    // only one parameter
    if (arg1[length-1] == ')') {

        int ret = INT_MAX;

        arg1 = trim_parenthesis(arg1);
        if (!contains_dot(arg1)) {
            // get value vector
            CatalogEntry* vvector = get(variable_pool, arg1);
            
            int count = vvector->size;

            for (int i=0; i < count; i++) {
                int temp = vvector->bitvector[i];
                if (temp <= ret && temp > INT_MIN) {
                    ret = temp;          
                }
            }
        }
        else {
            char* name = getName(arg1);
            
            // retrieve filename from catalog
            FILE* file1 = fopen("catalogue.txt", "r");
            CatalogHashtable* ht = populate_catalog(file1);
            CatalogEntry* pvector = get(ht, name);

            char* catpath = makePath(arg1, _COLUMN);

            while (strcmp((*pvector).filepath, catpath) != 0) {
                pvector = pvector->next;
            }

            char* fullpath = (*pvector).filepath;
            strcat(fullpath, ".txt");

            // open column file
            FILE* file = fopen(fullpath, "r");
            if (!file) {
                perror("Error opening file");
                return NULL;
                }
            char line[1024];
            //skip first line
            if (!fgets(line, sizeof(line), file)) {
                perror("Error reading file");
                fclose(file);
                return 0;
            }
            //loop through the rest
            while (fgets(line, sizeof(line), file)) {
                size_t len = strlen(line);
                if (len > 0 && line[len - 1] == '\n') {
                    line[len - 1] = '\0';
                }
                int lineval = atoi(line);
                if (lineval > INT_MIN && lineval < INT_MAX) {
                    if (lineval <= ret) {
                        ret = lineval;
                    }
                }
            }
            // close column file
            fclose(file1);
        }


        // Object for first return val
        CatalogEntry* cat = (CatalogEntry *) malloc(sizeof(CatalogEntry));
        if (!cat) {
            perror("Failed to allocate memory for CatalogEntry");
            return NULL;
        }

        strcpy(cat->name, handle);
        cat->value = ret;
        cat->has_value = true;
        cat->in_vpool = true;

        put(variable_pool, *cat);
    }
    else {
        char** handle_index = &handle;
        char* handle_first = next_token(handle_index, &send_message->status);
        char* handle_second = next_token(handle_index, &send_message->status);

        // Val vector is arg2, pos vector is arg1
        char* arg2 = next_token(command_index, &send_message->status);
        arg2 = trim_parenthesis(arg2);

        CatalogEntry* vvector = get(variable_pool, arg2);

        int ret = INT_MAX;
        int count = vvector->size;
        
        // If we are search through all of the vvector
        if (strcmp(arg1, "null") == 0) {
            for (int i=0; i < count; i++) {
                int temp = vvector->bitvector[i];
                if (temp <= ret && temp > INT_MIN) {
                    ret = temp;          
                }
            }
        }
        else {
            CatalogEntry* pvector = get(variable_pool, arg1);
            for (int i=0; i < count; i++) {
                int temp = vvector->bitvector[i];
                if (temp <= ret && temp > INT_MIN && pvector->bitvector[i] == INT_MAX) {
                    ret = temp;          
                }
            }
        }

        // Object for first return val
        CatalogEntry* positionlist = (CatalogEntry *) malloc(sizeof(CatalogEntry));
        if (!positionlist) {
            perror("Failed to allocate memory for CatalogEntry");
            return NULL;
        }

        // Find pos of all min values -> TODO: Technically this is searching through too many values in the second case
        for (int i=0; i < count; i++) {
            int temp = vvector->bitvector[i];
            if (temp == ret) {
                positionlist->bitvector[i] = INT_MAX;          
            }
            else {
                positionlist->bitvector[i] = INT_MIN;
            }
        }

        // Object for second return val
        CatalogEntry* maxvalues = (CatalogEntry *) malloc(sizeof(CatalogEntry));
        if (!maxvalues) {
            perror("Failed to allocate memory for CatalogEntry");
            return NULL;
        }

        strcpy(positionlist->name, handle_first);
        positionlist->size = count;

        strcpy(maxvalues->name, handle_second);
        maxvalues->value = ret;
        maxvalues->has_value = true;

        positionlist->in_vpool = true;
        maxvalues->in_vpool = true;

        put(variable_pool, *positionlist);
        put(variable_pool, *maxvalues);
    }

    
    DbOperator* dbo = malloc(sizeof(DbOperator));
    return dbo;
}

DbOperator* parse_add(char* query_command, char* handle, message* send_message, CatalogHashtable* variable_pool) {
    if (strncmp(query_command, "(", 1) != 0) {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command++;
    char** command_index = &query_command;
    
    
    // parse parameters
    char* arg1 = next_token(command_index, &send_message->status);
    char* arg2 = next_token(command_index, &send_message->status);
    arg1 = trim_parenthesis(arg1);
    arg2 = trim_parenthesis(arg2);

    CatalogEntry* vvector1 = get(variable_pool, arg1);
    CatalogEntry* vvector2 = get(variable_pool, arg2);

    CatalogEntry* retvector = (CatalogEntry *) malloc(sizeof(CatalogEntry));
    if (!retvector) {
        perror("Failed to allocate memory for CatalogEntry");
        return NULL;
    }

    int count = vvector1->size;

    for (int i=0; i<count; i++) {
        if ((vvector1->bitvector[i]) != INT_MIN && (vvector2->bitvector[i]) != INT_MIN 
        && (vvector1->bitvector[i]) != INT_MAX && (vvector2->bitvector[i]) != INT_MAX) {
            retvector->bitvector[i] = (vvector1->bitvector[i]) + (vvector2->bitvector[i]);
        }
        else {
            retvector->bitvector[i] = INT_MIN;
        }
    }

    strcpy(retvector->name, handle);
    retvector->size = count;
    retvector->in_vpool = true;

    put(variable_pool, *retvector);

    DbOperator* dbo = malloc(sizeof(DbOperator));
    return dbo;
}

DbOperator* parse_sub(char* query_command, char* handle, message* send_message, CatalogHashtable* variable_pool) {
    if (strncmp(query_command, "(", 1) != 0) {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command++;
    char** command_index = &query_command;
    
    
    // parse parameters
    char* arg1 = next_token(command_index, &send_message->status);
    char* arg2 = next_token(command_index, &send_message->status);
    arg1 = trim_parenthesis(arg1);
    arg2 = trim_parenthesis(arg2);

    CatalogEntry* vvector1 = get(variable_pool, arg1);
    CatalogEntry* vvector2 = get(variable_pool, arg2);

    CatalogEntry* retvector = (CatalogEntry *) malloc(sizeof(CatalogEntry));
    if (!retvector) {
        perror("Failed to allocate memory for CatalogEntry");
        return NULL;
    }

    int count = vvector1->size;

    for (int i=0; i<count; i++) {
        if ((vvector1->bitvector[i]) != INT_MIN && (vvector2->bitvector[i]) != INT_MIN 
        && (vvector1->bitvector[i]) != INT_MAX && (vvector2->bitvector[i]) != INT_MAX) {
            retvector->bitvector[i] = (vvector1->bitvector[i]) - (vvector2->bitvector[i]);
        }
        else {
            retvector->bitvector[i] = INT_MIN;
        }
    }

    strcpy(retvector->name, handle);
    retvector->size = count;
    retvector->in_vpool = true;

    put(variable_pool, *retvector);

    DbOperator* dbo = malloc(sizeof(DbOperator));
    return dbo;
}

// TODO: MILESTONE 2 BATCHING QUERIES!!!

// Initializes the queue.

void queue_init(Queue* queue) {
    node_t* dummy = malloc(sizeof(node_t));
    dummy->next = NULL;
    queue->head = queue->tail = dummy;
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->cond, NULL);
}

// Push a new query to the queue.
void queue_push(Queue* queue, SelectObject query) {
    node_t* new_node = malloc(sizeof(node_t));
    if (new_node == NULL) {
        exit(1); // handle memory allocation failure
    }
    new_node->query = query;
    new_node->next = NULL;

    pthread_mutex_lock(&queue->mutex);
    queue->tail->next = new_node;
    queue->tail = new_node;
    pthread_cond_signal(&queue->cond);
    pthread_mutex_unlock(&queue->mutex);
}

// Pop a query from the queue. This call is blocking.
SelectObject queue_pop(Queue* queue) {
    pthread_mutex_lock(&queue->mutex);
    while (queue->head == queue->tail) {
        // Wait until the queue is non-empty.
        pthread_cond_wait(&queue->cond, &queue->mutex);
    }
    node_t* old_head = queue->head;
    node_t* new_head = old_head->next;
    SelectObject query = new_head->query; // this is the actual "data" being popped
    queue->head = new_head;
    pthread_mutex_unlock(&queue->mutex);
    free(old_head);
    return query;
}

// Helper functions for batch processing

int get_select_obj(char* filename, ClientContext* context) {
    for (int i=0; i<20; i++) {
        if (strcmp(filename, context->selects[i])) {
            return i;
        }
    }
    return NULL;
}

DbOperator* batch_select_add(char* query_command, char* handle, message* send_message, CatalogEntry* variable_pool, ClientContext* context) {
    if (strncmp(query_command, "(", 1) != 0) {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
    query_command++;
    char** command_index = &query_command;

    SelectObject* retselect = (SelectObject*) malloc(sizeof(SelectObject));

    // parse table input. Assuming this is a column name
    char* arg1 = next_token(command_index, &send_message->status);

    char* low = next_token(command_index, &send_message->status);
    char* high = next_token(command_index, &send_message->status);
    high = trim_parenthesis(high);
    int ilow;
    int ihigh;
    if (strcmp(low, "null") == 0) {
        ilow = INT_MIN;
    } else {
        ilow = atoi(low);
    }
    if (strcmp(high, "null") == 0) {
        ihigh = INT_MAX;
    } else {
        ihigh = atoi(high);
    }

    strcpy(context->batch_identifier, arg1); //dont need to keep updating?

    retselect->maxval = ihigh;
    retselect->minval = ilow;
    strcpy(retselect->handle, handle);

    context->selects[context->num_selects] = retselect;
    context->num_selects++;

    DbOperator* dbo = malloc(sizeof(DbOperator));
    return dbo;
}

DbOperator* parse_batch_execute(char* query_command, message* send_message, CatalogEntry* variable_pool, ClientContext* context) {
    if (strncmp(query_command, "()", 2) != 0) {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }

    query_command++;
    query_command++;
    char** command_index = &query_command;

    char* arg1 = context->batch_identifier;
    char* name = getName(arg1);
    char* catpath = makePath(arg1, _COLUMN);

    char* fullpath = catpath;
    strcat(fullpath, ".txt");
    // open column file
    FILE* file = fopen(fullpath, "r");
    if (!file) {
        perror("Error opening file");
        return NULL;
    }


    char line[1024];
    // Skip the first line

    if (!fgets(line, sizeof(line), file)) {
        perror("Error reading file");
        fclose(file);
        return NULL;
    }

    int count = 0;
    // Now loop through each subsequent line
    while (fgets(line, sizeof(line), file)) {
        if (strncmp(line, "\0", 1) == 0) {
            break;
        }
        // Remove the newline character, if present
        size_t len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') {
            line[len - 1] = '\0';
        }
        int lineval = atoi(line);
        // Now 'line' contains the current line from the file without the newline character
        // For our bitvector, false === INT_MIN and true === INT_MAX (this allows us to use the bitvector object for a value vector)

        // NOW WE CHECK EACH SELECT OBJECT IN THE CLIENT CONTEXT THIS VALUE
        for (int i=0; i<context->num_selects; i++) {
            SelectObject* obj_in_question = context->selects[i];
            int ihigh = obj_in_question->maxval;
            int ilow = obj_in_question->minval;

            int val;
            if (lineval < ihigh && lineval >= ilow) {
                val = INT_MAX;
            }
            else {
                val = INT_MIN;
            }
            context->selects[i]->results[count] = val;

        }
        count++;
        printf("%i", count);
    }
        

    for (int i=0; i<context->num_selects; i++) {
        SelectObject* obj_in_question = context->selects[i];
        CatalogEntry* cat = (CatalogEntry *) malloc(sizeof(CatalogEntry));
        if (!cat) {
            perror("Failed to allocate memory for CatalogEntry");
            return NULL;
        }
        strcpy(cat->name, obj_in_question->handle);
        cat->size = count;
        for (int j=0; j<count; j++) {
            cat->bitvector[j] = obj_in_question->results[j];
        }
        cat->in_vpool = true;
        put(variable_pool, *cat);
    }

    fclose(file);

    DbOperator* dbo = malloc(sizeof(DbOperator));
    return dbo;

}



/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 * 
 * Getting Started Hint:
 *      What commands are currently supported for parsing in the starter code distribution?
 *      How would you add a new command type to parse? 
 *      What if such command requires multiple arguments?
 **/
char* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context, CatalogHashtable* variable_pool,
Queue* batch_queue) {
    // a second option is to malloc the dbo here (instead of inside the parse commands). Either way, you should track the dbo
    // and free it when the variable is no longer needed. 
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator));

    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.  
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here. 
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    cs165_log(stdout, "QUERY: %s\n", query_command);

    // by default, set the status to acknowledge receipt of command,
    //   indication to client to now wait for the response from the server.
    //   Note, some commands might want to relay a different status back to the client.
    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);
    // check what command is given. 
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        dbo = parse_create(query_command, variable_pool);
        if(dbo == NULL){
            send_message->status = INCORRECT_FORMAT;
        }
        else{
            send_message->status = OK_DONE;
        }
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message, variable_pool);
    } else if (strncmp(query_command, "load_insert", 11) == 0) {
        query_command += 17;
        dbo = parse_load_insert(query_command, send_message, variable_pool, context);
    } else if (handle != NULL && (strncmp(query_command, "select", 6) == 0)) {
        query_command += 6;
        if (context->is_batch) {
            dbo = batch_select_add(query_command, handle, send_message, variable_pool, context);
        }
        else {
            if (context->multithread == false) {
                dbo = parse_select(query_command, handle, send_message, variable_pool);
            }
            else {
                dbo = parse_select_multithread(query_command, handle, send_message, variable_pool);
            }
        }
    } else if (handle != NULL && (strncmp(query_command, "fetch", 5) == 0)) {
        query_command += 5;
        dbo = parse_fetch(query_command, handle, send_message, variable_pool);
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        return parse_print(query_command, send_message, variable_pool);
    } else if (handle != NULL && (strncmp(query_command, "avg", 3) == 0)) {
        query_command += 3;
        dbo = parse_avg(query_command, handle, send_message, variable_pool); 
    } else if (handle != NULL && strncmp(query_command, "sum", 3) == 0) {
        query_command += 3;
        dbo = parse_sum(query_command, handle, send_message, variable_pool); 
    } else if (handle != NULL && strncmp(query_command, "max", 3) == 0) {
        query_command += 3;
        dbo = parse_max(query_command, handle, send_message, variable_pool); 
    } else if (handle != NULL && strncmp(query_command, "min", 3) == 0) {
        query_command += 3;
        dbo = parse_min(query_command, handle, send_message, variable_pool); 
    } else if (handle != NULL && strncmp(query_command, "add", 3) == 0) {
        query_command += 3;
        dbo = parse_add(query_command, handle, send_message, variable_pool); 
    } else if (handle != NULL && strncmp(query_command, "sub", 3) == 0) {
        query_command += 3;
        dbo = parse_sub(query_command, handle, send_message, variable_pool); 
    } /*else if (handle != NULL && strncmp(query_command, "shutdown", 8) == 0) {
        query_command += 3;
        dbo = parse_shutdown(query_command, handle, send_message, variable_pool, context); 
    }*/ else if (strncmp(query_command, "batch_queries()", 15) == 0) {
        query_command += 13;
        context->is_batch = true;
    } else if (strncmp(query_command, "single_core()", 13) == 0) {
        query_command += 13;
        context->multithread = false;   
    } else if (strncmp(query_command, "single_core_execute()", 21) == 0) {
        query_command += 21;
        context->multithread = true;   
    } else if (strncmp(query_command, "batch_execute", 13) == 0) {
        query_command += 13;
        dbo = parse_batch_execute(query_command, send_message, variable_pool, context);
    }
    free(dbo);
    return "";
}
