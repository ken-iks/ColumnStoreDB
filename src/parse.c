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
            if (strcmp(target.name, name) == 0) {
                found = true;
            }
            else {
                target_node_ptr = target.next;
            } 
        }
        return (found) ? target_node_ptr : (CatalogEntry*) NULL;
    }
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
    char* name = strtok(line, ",");
    char* path = strtok(NULL, ",");
    char* typestring = strtok(NULL, ",");
    CreateType type = str_to_type(typestring);
    strcpy((*cat).filepath,path);
    strcpy((*cat).name,name);
    (*cat).t = type;
    (*cat).next = NULL;
    (*cat).line = line_num;
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

    char* data = mmap(NULL, sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        perror("mmap");
        close(fd);
        return NULL;
    }

    char* line = strtok(data, "\n");
    int count = 1;
    while (line) {
        char* line_copy = line;
        // Create entry for the line
        CatalogEntry* ct = line_to_entry(line_copy, count);
        // Add entry to hashtable
        put(ht, *ct);
        // Go to next line and increase count
        line = strtok(NULL, "\n");
        count++;
    }
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



/* Function for making directory */
char* makePath(char* name, CreateType t) {
    // dbname = name. tablename = dbname.name (split by '.') columnname = dbname.tbname.name
    char *path = malloc(100);

    if (!path) {
        return NULL;
    }

    switch (t) {

        case _DB: {
            strcpy(path, "./");
            strcat(path, name);
            return path;
            break;
        }

        case _TABLE: {
            strcpy(path, "./");
            char *token = strtok(name, ".");
            strcat(path, token);
            strcat(path, "/");
            token = strtok(NULL, ".");
            strcat(path, token);
            return path;
            break;
        }

        case _COLUMN: {
            strcpy(path, "./");
            char *token = strtok(name, ".");
            strcat(path, token);
            strcat(path, "/");
            token = strtok(NULL, ".");
            strcat(path, token);
            strcat(path, "/");
            token = strtok(NULL, ".");
            strcat(path, token);
            return path;
            break;
        }

        default:
            return NULL;
            break;    
    }
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
    fprintf(file, "%s", val);
    fclose(file);
    free(fullpath);
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
            // Check if the entry is a regular file
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

DbOperator* parse_insert(char* query_command, message* send_message) {
        if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* file_name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        //TODO: load csv file and then follow steps from chatgpt to add values to db



        } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
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
DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
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
    } else if (strncmp(query_command, 'load', 4) == 0) {
        query_command += 4;
        dbo = parse_load(query_command, send_message);
    }
    if (dbo == NULL) {
        return dbo;
    }
    
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
