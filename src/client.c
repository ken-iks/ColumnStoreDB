/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
 *  CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "common.h"
#include "message.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("-- Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        log_err("client connect failed: ");
        return -1;
    }

    log_info("-- Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

/**
 * Getting Started Hint:
 *      What kind of protocol or structure will you use to deliver your results from the server to the client?
 *      What kind of protocol or structure will you use to interpret results for final display to the user?
 *      
**/

void sendMessage(message send_message, int socket) {
    // Send the message_header, which tells server payload size
    if (send(socket, &(send_message), sizeof(message), 0) == -1) {
        log_err("Failed to send message header.");
        exit(1);
    }


    // Send the payload (query) to server
    if (send(socket, send_message.payload, send_message.length, 0) == -1) {
        log_err("Failed to send query payload.");
        exit(1);
    }
}

void receiveMessage(int socket) {
    message recv_message;
    int len = 0;

    // Always wait for server response (even if it is just an OK message)
    if ((len = recv(socket, &(recv_message), sizeof(message), 0)) > 0) {
        if ((recv_message.status == OK_WAIT_FOR_RESPONSE || recv_message.status == OK_DONE) &&
            (int) recv_message.length > 0) {
            // Calculate number of bytes in response package
            int num_bytes = (int) recv_message.length;
            char payload[num_bytes + 1];

            // Receive the payload and print it out
            if ((len = recv(socket, payload, num_bytes, 0)) > 0) {
                payload[num_bytes] = '\0';
                printf("%s\n", payload);
            }
        }
    }
    else {
        if (len < 0) {
            log_err("Failed to receive message.");
        }
        else {
            log_info("-- Server closed connection\n");
        }
        exit(1);
    }
}

message handleLoadQuery(char* query, int socket) {
    // extract message path
    char* path = query + 5;
    path = trim_whitespace(path);
    path = trim_quotes(path);
    size_t len = strlen(path);
    if (path[len - 1] != ')') {
        return;
    }
    path[len - 1] = '\0';

    // open file
    FILE* fp = fopen(path, "r");

    if (fp == NULL) {
        perror("Failed to open file");
        return;
    }

        
    char buf[1024];
    message send_message;
    send_message.status = 0;

    

    // read database/table/column
    if (!fgets(buf, sizeof(buf), fp)) {
        fclose(fp);
        return;
    }
    char* col_name = malloc((strlen(buf) + 1) * sizeof(char));
    if (!col_name) {
        // handle allocation error
        fclose(fp);
        return;
    }
    strcpy(col_name, buf);
    fprintf("COL NAME: %s", col_name);

    // Assuming the format db1.tbl1.col1,db1.tbl1.col2,... extract db1.tbl1
    char* token1 = strtok(col_name, ".");
    if (token1 == NULL) {
        free(col_name);
        fclose(fp);
        log_err("Error parsing table name from file.\n");
        return;
    }

    char* token2 = strtok(NULL, ".");
    if (token2 == NULL) {
        free(col_name);
        fclose(fp);
        log_err("Error parsing column name from file.\n");
        return;
    }

    char* table_name = malloc(strlen(token1) + strlen(token2) + 2); // +2 for dot and null terminator
    if (!table_name) {
        // handle allocation error
        free(col_name);
        fclose(fp);
        return;
    }
    sprintf(table_name, "%s.%s", token1, token2);
    // Now table_name contains 'db1.tbl1'
    

    // Read all rows from file and send to server
    while (fgets(buf, sizeof(buf), fp)) {
        size_t len = strlen(buf);
        if (buf[len - 1] == '\n') {
            buf[len - 1] = '\0';
        }
        char query_insert[1024]; // Adjust size as needed
        sprintf(query_insert, "relational_insert(%s,%s)", table_name, buf);
        send_message.length = strlen(query_insert);
        send_message.payload = query_insert;
        sendMessage(send_message, socket); 
        receiveMessage(socket);
    }

    fclose(fp);
}



int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;
    int len = 0;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;
    send_message.status = 0;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }
   
        // Only process input that is greater than 1 character.
        // Convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(read_buffer);
        if (send_message.length > 1) {
            // handle load messages differently from the rest
            if (strncmp(read_buffer, "load", 4) == 0) {
                handleLoadQuery(read_buffer, client_socket);
                continue;
            }

            // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message header.");
                exit(1);
            }


            // Send the payload (query) to server
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                log_err("Failed to send query payload.");
                exit(1);
            }

            // Always wait for server response (even if it is just an OK message)
            if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
                if ((recv_message.status == OK_WAIT_FOR_RESPONSE || recv_message.status == OK_DONE) &&
                    (int) recv_message.length > 0) {
                    // Calculate number of bytes in response package
                    int num_bytes = (int) recv_message.length;
                    char payload[num_bytes + 1];

                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        payload[num_bytes] = '\0';
                        printf("%s\n", payload);
                    }
                }
            }
            else {
                if (len < 0) {
                    log_err("Failed to receive message.");
                }
                else {
		            log_info("-- Server closed connection\n");
		        }
                exit(1);
            }
        }
    }
    close(client_socket);
    return 0;
}
