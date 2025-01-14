#ifndef PARSE_H__
#define PARSE_H__
#include "cs165_api.h"
#include "message.h"
#include "client_context.h"

char* parse_command(char* query_command, message* send_message, int client, ClientContext* context, CatalogHashtable* variable_pool, Queue* batch_queue);
int allocate(CatalogHashtable** ht, int size);
int deallocate(CatalogHashtable* ht);
int print_vector(char* name, CatalogHashtable* variable_pool);
int print_column(char* name);
void queue_init(Queue* queue);
#endif
