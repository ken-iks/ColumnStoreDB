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
    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return NULL; //QUERY_UNSUPPORTED
    }
    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return NULL;
    }

    char* path = makePath(table_name, _TABLE);

    // Make the directory
    if (mkdir(path, 0755) == -1){
        return NULL;
    }
    // Update the catalog
    TableObj tb;
    strcpy(tb.name, table_name);
    strcpy(tb.path, path);
    // TODO: Finish

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

        char* path = makePath(db_name, _DB);

        // Make the directory
        if (mkdir(path, 0755) == -1){
            return NULL;
        }
        // Update the catalog
        DbObj db;
        strcpy(db.name, db_name);
        strcpy(db.path, path);

        FILE* file = fopen("catalogue.dat", "a+b");
        if (!file) {
            perror("Error opening file");
            return 1;
            }

        int fd = fileno(file);  // Get the file descriptor from FILE*

        // Find the current size of the file
        struct stat st;
        if (fstat(fd, &st) == -1) {
            perror("Error getting file size");
            fclose(file);
            return 1;
        }

        off_t currentSize = st.st_size;

        // TODO: Use current size to update index catalogue (maybe hashtable mapping dashboard to location)
        // TODO: In 'add table', we can have another index catalogue for where to find a given table name, then the same for columns
        // TODO: Once done, test that these things can be created (at least tables and databases) -> FIGURE OUT HOW TO DOCKER

        // Increase the size of the file to accommodate a new DbObj.
        if (ftruncate(fd, currentSize + sizeof(DbObj)) == -1) {
            perror("Error setting file size");
            fclose(file);
            return 1;
        }

        // Map the portion of the file where the new DbObj will go (i.e., the end).
        DbObj* addr = (DbObj*)mmap(NULL, sizeof(DbObj), PROT_READ | PROT_WRITE, MAP_SHARED, fd, currentSize);
        if (addr == MAP_FAILED) {
            perror("Error mapping file");
            fclose(file);
            return 1;
        }
        // May possibly be copying db into address? (perhaps)
        memcpy(addr, &db, sizeof(DbObj));

        // Cleanup
        if (munmap(addr, sizeof(DbObj)) == -1) {
            perror("Error unmapping file");
        }
        fclose(file);

        // make create operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
        dbo->operator_fields.create_operator.create_type = _DB;
        strcpy(dbo->operator_fields.create_operator.name, db_name);
        return dbo;
    }
}



/* Function for making directory */
char* makePath(char* name, CreateType t) {
    // dbname = name. tablename = dbname.name (split by '.') columnname = dbname.tbname.name

    switch (t) {

        case _DB: 
            char* path[100] = "./";
            strcat(name, path);
            return path;
            break;

        case _TABLE:
            char* path[100] = "./";
            char *token = strtok(name, ".");
            strcat(token, path);
            strcat("/", path);
            char *token = strtok(NULL, ".");
            strcat(token, path);
            return path;
            break;

        case _COLUMN:
            char* path[100] = "./";
            char *token = strtok(name, ".");
            strcat(token, path);
            strcat("/", path);
            char *token = strtok(NULL, ".");
            strcat(token, path);
            strcat("/", path);
            char *token = strtok(NULL, ".");
            strcat(token, path);
            return path;
            break;

        default:
            return -1;
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

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
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
        } 
        return dbo;
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
    }
    if (dbo == NULL) {
        return dbo;
    }
    
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
