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
 * We use a division remainder method gotten from chatgpt - table size currently 101 (subject to change)
*/

unsigned long hash(const char *str) {
    unsigned long hash = 0;
    int c;
    while ((c = *str++))
        hash = c + (hash << 6) + (hash << 16) - hash;
    return hash % 101;
}


// THIS METHOD ALLOCATES AN EMPTY CATALOGUE HASHTABLE
int allocate(CatalogHashtable** ht, int size) {
    *ht = (CatalogHashtable *)malloc(sizeof(CatalogHashtable));
    if (*ht != NULL) {
        CatalogHashtable t;
        for (int i = 0; i < 101; i++) {
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
CatalogEntry* get(CatalogHashtable* ht, char* name) {
    if (ht != NULL) {
        int key = hash(name);
        bool found = false;
        CatalogEntry* target_node_ptr = ht->table[key];
        while (target_node_ptr != NULL && !found) {
            CatalogEntry target = *target_node_ptr;
            //perror(target.name);
            if (strcmp(target.name, name) == 0) {
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
            // COLLISION CASE
            return add_node(ht->table[buck], new_node);
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
// HELPER FUNCTION TO AVOID MEMORY LEAKS
int deallocate_bucket(CatalogEntry* head) {
    CatalogEntry *temp;
    while (head != NULL) {
        temp = head;
        head = head->next;
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
        for (int i=0; i<101; i++) {
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
        close(fd);
        return NULL;
    }

    char* data = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    char *buffer = malloc(sb.st_size);  // +1 for null-terminator
    if (!buffer) {
        perror("malloc");
        close(fd);
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
        
        // Increase count
        count++;
    }
    munmap(data, sb.st_size);
    close(fd);
    return ht;
}

/**
 * This method takes in a string representing the arguments to create a column.
 * It parses those arguments, checks that they are valid, and creates a column - both in the catalog, and a physical file.
 */

DbOperator* parse_create_col(char* create_arguments) {
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
    char* startpath = (char*)malloc(strlen(table_name) + strlen(column_name) + 1);
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

    // Create actual files
    strcat(path,".txt");
    FILE* file2 = fopen(path, "w");
    if (!file2) {
        perror("Error opening file");
        return NULL;
        }
    fprintf(file2, "COL NAME: %s\n", column_name);
    fclose(file2);
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

    char* startpath = (char*)malloc(strlen(db_name) + strlen(table_name) + 1);
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
DbOperator* parse_create(char* create_arguments) {
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
        if (mes_status == INCORRECT_FORMAT) {
            return NULL;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                dbo = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                dbo = parse_create_tbl(tokenizer_copy);
            } else if (strcmp(token, "col") == 0){
                dbo = parse_create_col(tokenizer_copy);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return dbo;
}
// For 'insert'
int add_element_to_file(char* fname, char* abspath, char* val) {
    char *fullpath = malloc(100);
    strcpy(fullpath, abspath);
    strcat(fullpath, "/");
    strcat(fullpath, fname);

    FILE* file = fopen(fullpath, "a");
    if (!file) {
        perror("Error opening file");
        return -1;
        }
    fprintf(file, "%s\n", val);
    fclose(file);
    free(fullpath);
    return 0;
}
// Adds an element with filename = db.tbl.cl to correct file granted that cl exists in catalog
// Primarily used in 'load'
int add_element_to_file2(char* filename, char* val) {

    char* name = getName(filename);

    FILE* file1 = fopen("catalogue.txt", "r");
    
    CatalogHashtable* ht = populate_catalog(file1);
    
    CatalogEntry* this_table = get(ht, name);

    if (!this_table) {
        //perror("Error retriving column");
        fprintf(stderr, "Error retriving column: %s", name);
        return -1;
    }
    char* fullpath = (*this_table).filepath;
    strcat(fullpath, ".txt");

    FILE* file = fopen(fullpath, "a");
    if (!file) {
        perror("Error opening file");
        return -1;
        }
    fprintf(file, "%s\n", val);
    fclose(file);

    return 0;
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {
    //unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* table_name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        // find entry for this table in catalog
        FILE* file = fopen("catalogue.txt", "a");
        CatalogHashtable* ht = populate_catalog(file);
        CatalogEntry* this_table = get(ht, table_name);

        if (!this_table) {
            return NULL;
        }
        // very minimal validation
        char* path = (*this_table).filepath;

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
                add_element_to_file(entry->d_name, path, token);
            }
        }

        closedir(dir);

        DbOperator* dbo = malloc(sizeof(DbOperator));
        /*
        // lookup the table and make sure it exists. 
        Table* insert_table = lookup_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // make insert operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);
        // parse inputs until we reach the end. Turn each given string into an integer. 
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
            return NULL;
        }  */
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}



DbOperator* parse_load(char* query_command, message* send_message) {
        if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        
        
        // parse table input
        char* file_name = next_token(command_index, &send_message->status);
        file_name = trim_quotes(file_name);
        int last_char = strlen(file_name) - 1;
        if (last_char < 0 || file_name[last_char] != ')') {
            return NULL;
        }
        file_name[last_char] = '\0';

        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        //TODO: CHECK IF THE THING WE WANT TO LOAD ACTUALLY EXISTS

        FILE* csv = fopen(file_name, "r");
        if (!csv) {
            perror("Error opening file");
            return -1;
            }

        char line[1024];
        char *headerTokens[100];
        int index = 0;

        if (fgets(line, sizeof(line), csv)) {
            size_t len = strlen(line);
            if (len > 0 && line[len - 1] == '\n') {
                line[len - 1] = '\0';
            }

            char *token, *stringp, *tofree;
            tofree = stringp = strdup(line);
            while ((token = strsep(&stringp, ",")) != NULL) {
                headerTokens[index] = strdup(token);
                index++;
            }
            free(tofree);
        }

        while (fgets(line, sizeof(line), csv)) {
            char *lineTokens[100];
            int lineIndex = 0;

            size_t len = strlen(line);
            if (len > 0 && line[len - 1] == '\n') {
                line[len - 1] = '\0';
            }

            char *token, *stringp, *tofree;
            tofree = stringp = strdup(line);
            while ((token = strsep(&stringp, ",")) != NULL) {
                lineTokens[lineIndex] = token;
                lineIndex++;
            }

            for (int i = 0; i < lineIndex; i++) {
                if (headerTokens[i] != NULL && lineTokens[i] != NULL) {
                    add_element_to_file2(headerTokens[i], lineTokens[i]);
                }
                else {
                    break;
                }
            }

            free(tofree);
        }
        fclose(csv);

        // Clean up the allocated memory for header tokens
        for (int i = 0; i < index; i++) {
            free(headerTokens[i]);
        }
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
        
        // retrieve filename from catalog
        FILE* file1 = fopen("catalogue.txt", "r");
        CatalogHashtable* ht = populate_catalog(file1);
        CatalogEntry* this_table = get(ht, name);

        if (!this_table) {
            //perror("Error retriving column");
            fprintf(stderr, "Error retriving column: %s", name);
            return NULL;
        }
        char* fullpath = (*this_table).filepath;
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
                val = INT_MAX;
            }
            else {
                val = INT_MIN;
            }
            cat->bitvector[count] = val;
            count++;

        }
        cat->size = count;
        
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
        
        put(variable_pool, *cat);
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
    
    // retrieve filename from catalog
    FILE* file1 = fopen("catalogue.txt", "r");
    CatalogHashtable* ht = populate_catalog(file1);
    CatalogEntry* this_table = get(ht, name);

    if (!this_table) {
        //perror("Error retriving column");
        fprintf(stderr, "Error retriving column: %s", name);
        return NULL;
    }
    char* fullpath = (*this_table).filepath;
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
        // Remove the newline character, if present
        size_t len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') {
            line[len - 1] = '\0';
        }
        int lineval = atoi(line);
        // Now 'line' contains the current line from the file without the newline character
        if (pvector->bitvector[count] != INT_MIN) {
            cat->bitvector[count] = lineval;
        }
        else {
            cat->bitvector[count] = INT_MIN;
        }
        count++;
    }
    cat->size = count;

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
    FILE *combinedFile = fopen("combined_data.txt", "w");
    if (!combinedFile) {
        perror("Error opening combined file");
        return 0;
    }
    // check if it simply a return value and not a full vector
    if (vvector->has_value == true) {
        char buffer[12];
        snprintf(buffer, sizeof(buffer), "%.2f", vvector->value);
        fputs(buffer, combinedFile);
    }
    int size = vvector->size;
    for (int i=0; i<size; i++) {
        if (vvector->bitvector[i] != INT_MIN && vvector->bitvector[i] != INT_MAX) {
            char buffer[12];
            snprintf(buffer, sizeof(buffer), "%d", vvector->bitvector[i]);
            /*log_err(buffer);*/
            fputs(buffer, combinedFile);
            fputs("\n", combinedFile);
        }
    }
    fputc('\n', combinedFile);
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
    while (last_char < 0 || token[last_char] != ')') {
        if (contains_dot(token)) {
            print_column(token);
        }
        else {
            print_vector(token, variable_pool);
        }
        token = next_token(command_index, &send_message->status);
        last_char = strlen(token) - 1;
    }
    token = trim_parenthesis(token);
    if (contains_dot(token)) {
        print_column(token);
    }
    else {
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

    /*
    // Read and process each line of the combined file
    char line[1024];
    while (fgets(line, sizeof(line), readCombined)) {
        // Process each line as needed, e.g., print, parse further, etc.
        fprintf(stdout, "%s", line);
    }
    */

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
    
    // get value vector
    CatalogEntry* pvector = get(variable_pool, arg1);

    float ret;
    int sum = 0;
    int count = pvector->size;
    int div = 0;

    for (int i=0; i< count; i++) {
        if (pvector->bitvector[i] < INT_MAX && pvector->bitvector[i] > INT_MIN) {
            sum += pvector->bitvector[i];
            div += 1;
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
    // get value vector
    CatalogEntry* pvector = get(variable_pool, arg1);

    int ret = 0;
    int count = pvector->size;

    for (int i=0; i< count; i++) {
        ret += pvector->bitvector[i];
    }
    // ret now has sum
    CatalogEntry* cat = (CatalogEntry *) malloc(sizeof(CatalogEntry));
    if (!cat) {
        perror("Failed to allocate memory for CatalogEntry");
        return NULL;
    }
    strcpy(cat->name, handle);
    cat->value = ret;
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
        arg1 = trim_parenthesis(arg1);
        // get value vector
        CatalogEntry* vvector = get(variable_pool, arg1);

        int ret = INT_MIN;
        int count = vvector->size;

        for (int i=0; i < count; i++) {
            int temp = vvector->bitvector[i];
            if (temp >= ret) {
                ret = temp;          
            }
        }

        // Object for first return val
        CatalogEntry* cat = (CatalogEntry *) malloc(sizeof(CatalogEntry));
        if (!cat) {
            perror("Failed to allocate memory for CatalogEntry");
            return NULL;
        }

        strcpy(cat->name, handle);
        cat->value = ret;
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
                if (temp >= ret) {
                    ret = temp;          
                }
            }
        }
        else {
            CatalogEntry* pvector = get(variable_pool, arg1);
            for (int i=0; i < count; i++) {
                int temp = vvector->bitvector[i];
                if (temp >= ret && pvector->bitvector[i] == INT_MAX) {
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
        arg1 = trim_parenthesis(arg1);
        // get value vector
        CatalogEntry* vvector = get(variable_pool, arg1);

        int ret = INT_MAX;
        int count = vvector->size;

        for (int i=0; i < count; i++) {
            int temp = vvector->bitvector[i];
            if (temp <= ret) {
                ret = temp;          
            }
        }

        // Object for first return val
        CatalogEntry* cat = (CatalogEntry *) malloc(sizeof(CatalogEntry));
        if (!cat) {
            perror("Failed to allocate memory for CatalogEntry");
            return NULL;
        }

        strcpy(cat->name, handle);
        cat->value = ret;
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
                if (temp <= ret) {
                    ret = temp;          
                }
            }
        }
        else {
            CatalogEntry* pvector = get(variable_pool, arg1);
            for (int i=0; i < count; i++) {
                int temp = vvector->bitvector[i];
                if (temp <= ret && pvector->bitvector[i] == INT_MAX) {
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

    CatalogEntry* vvector1 = get(variable_pool, arg1);
    CatalogEntry* vvector2 = get(variable_pool, arg2);

    CatalogEntry* retvector = (CatalogEntry *) malloc(sizeof(CatalogEntry));
    if (!retvector) {
        perror("Failed to allocate memory for CatalogEntry");
        return NULL;
    }

    int count = vvector1->size;

    for (int i=0; i<count; i++) {
        retvector->bitvector[i] = (vvector1->bitvector[i]) + (vvector2->bitvector[i]);
    }

    strcpy(retvector->name, handle);
    retvector->size = count;

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

    CatalogEntry* vvector1 = get(variable_pool, arg1);
    CatalogEntry* vvector2 = get(variable_pool, arg2);

    CatalogEntry* retvector = (CatalogEntry *) malloc(sizeof(CatalogEntry));
    if (!retvector) {
        perror("Failed to allocate memory for CatalogEntry");
        return NULL;
    }

    int count = vvector1->size;


    for (int i=0; i<count; i++) {
        retvector->bitvector[i] = (vvector1->bitvector[i]) - (vvector2->bitvector[i]);
    }

    strcpy(retvector->name, handle);
    retvector->size = count;

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
/*
DbOperator* add_select_to_query(char* query_command, char* handle, message* send_message, CatalogEntry* variable_pool, Queue* batch_queue) {

}

DbOperator* parse_batch_execute(char* query_command, message* send_message, ClientContext* context) {
}
*/



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
        dbo = parse_create(query_command);
        if(dbo == NULL){
            send_message->status = INCORRECT_FORMAT;
        }
        else{
            send_message->status = OK_DONE;
        }
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    } else if (strncmp(query_command, "load", 4) == 0) {
        query_command += 4;
        dbo = parse_load(query_command, send_message);
    } else if (handle != NULL && (strncmp(query_command, "select", 6) == 0)) {
        query_command += 6;
        if (context->is_batch) {
            //dbo = add_select_to_queue(query_command, handle, send_message, variable_pool, batch_queue); //TODO:
        }
        else {
            dbo = parse_select(query_command, handle, send_message, variable_pool);
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
    }  /* else if (strncmp(query_command, "batch_queries", 13) == 0) {
        query_command += 13;
        context->is_batch = true;
    } else if (strncmp(query_command, "batch_execute", 13) == 0) {
        query_command += 13;
        dbo = parse_batch_execute(query_command, send_message, context);
    } */
  
    return "";
}
