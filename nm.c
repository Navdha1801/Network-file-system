// naming_server.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>

#define PORT_NM 12390
// Define a constant for the size of the hashmap
#define HASHMAP_SIZE 100
int connectToStorageServer(char *ip_address, int port_nm_persistent);
typedef struct
{
    char ip_address[15];
    int port_nm;
    int port_nm_persistent;
    int port_client;
    char accessible_paths[100][256];
} StorageServer;
typedef struct
{
    char path[256];
    StorageServer *ss;
} HashmapEntry;

// Define a structure for the hashmap
typedef struct
{
    HashmapEntry entries[HASHMAP_SIZE];
} Hashmap;


typedef struct
{
    char operation[10]; // Added for handling READ/WRITE operations
    char path[256];     // Added for handling the requested file path
} ClientRequest;

typedef struct
{
    char operation[10];
    char path[256];
    char name[256];
} NamingServerRequest;
void logMessage(const char *message)
{
    FILE *logFile = fopen("naming_server_log.txt", "a");
    if (logFile != NULL)
    {
        fprintf(logFile, "%s\n", message);
        fclose(logFile);
    }
    else
    {
        perror("Error opening log file");
    }
}
void logConfirmation(const char *confirmation_message)
{
    char logmessage[256];
    sprintf(logmessage, "Received confirmation from storage server: %s", confirmation_message);
    logMessage(logmessage);
}
void logAcceptedClientConnection(const char *ip_address, int port)
{
    char logmessage[256];
    sprintf(logmessage, "Accepted a client connection from %s:%d", ip_address, port);
    logMessage(logmessage);
}
void logssConnection(const char *ip_address, int port)
{
    char logmessage[256];
    sprintf(logmessage, "Connected to storage server with ip_address and port as %s:%d", ip_address, port);
    logMessage(logmessage);
}

// Initialize the hashmap
void initializeHashmap(Hashmap *hashmap)
{
    for (int i = 0; i < HASHMAP_SIZE; ++i)
    {
        hashmap->entries[i].ss = NULL;
        strcpy(hashmap->entries[i].path, "");
    }
}

//  hash function for strings
unsigned int hashFunction(const char *path)
{
    unsigned int hash = 5381; // Initial value

    while (*path)
    {
        // Use prime number 33 and bitwise XOR operation
        hash = ((hash << 5) + hash) ^ (*path++);
    }

    return hash % HASHMAP_SIZE;
}


// Insert an entry into the hashmap
void insertIntoHashmap(Hashmap *hashmap, const char *path, StorageServer *ss)
{
    int index = hashFunction(path);
    strcpy(hashmap->entries[index].path, path);
    hashmap->entries[index].ss = ss;
}
int initializeNamingServer()
{
    int server_socket;

    if ((server_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    {
        perror("Socket creation failed");
        return -1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT_NM);

    if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1)
    {
        perror("Binding failed");
        close(server_socket);
        return -1;
    }

    if (listen(server_socket, 5) == -1)
    {
        perror("Listening failed");
        close(server_socket);
        return -1;
    }

    printf("Naming Server is listening on port %d...\n", PORT_NM);

    return server_socket;
}

void registerStorageServer(int client_socket, StorageServer ss)
{
    printf("Received registration from Storage Server:\n");
    printf("IP Address: %s\n", ss.ip_address);
    printf("Port (NM): %d\n", ss.port_nm);
    printf("Port(NM to SS connection): %d\n",ss.port_nm_persistent);
    printf("Port (Client): %d\n", ss.port_client);
    printf("Accessible Paths:\n");
    for (int i = 0; i < 100 && strlen(ss.accessible_paths[i]) > 0; ++i)
    {
        printf("%s\n", ss.accessible_paths[i]);
    }
    // Log the registration
    char logmessage[256];
    sprintf(logmessage, "Storage Server registered - IP: %s, Port (NM): %d,Port(NM to SS connection):%d,Port(client):%d", ss.ip_address, ss.port_nm,ss.port_nm_persistent,ss.port_client);
    logMessage(logmessage);
}

// Loop through storage servers to find the one with the accessible path
/*StorageServer *findStorageServerForPath(StorageServer storage_servers[], int num_servers, const char *path)
{
    for (int i = 0; i < num_servers; ++i)
    {
        for (int j = 0; j < 100 && strlen(storage_servers[i].accessible_paths[j]) > 0; ++j)
        {
            // printf("path is %s\n", storage_servers[i].accessible_paths[j]);
            if (strcmp(storage_servers[i].accessible_paths[j], path) == 0)
            {
                return &storage_servers[i];
            }
        }
    }
    return NULL;
}*/
// Search for a Storage Server in the hashmap
StorageServer *findStorageServerForPath(Hashmap *hashmap, const char *path)
{
    int index = hashFunction(path);
    if (strcmp(hashmap->entries[index].path, path) == 0)
    {
        return hashmap->entries[index].ss;
    }
    return NULL;
}

void serveClient(int client_socket, StorageServer storage_servers[], int num_servers,Hashmap hashmap)
{
    // Receive client request
    ClientRequest client_request;
    recv(client_socket, &client_request, sizeof(ClientRequest), 0);
    printf("Received a client request:\n");
    printf("Operation: %s\n", client_request.operation);
    // printf("hello wtfis\n");
    //  Log the client request
    char logmessage[256];
    sprintf(logmessage, "Received a client request - Operation: %s, Path: %s", client_request.operation, client_request.path);
    logMessage(logmessage);

    if (strcmp(client_request.operation, "READ") == 0 || strcmp(client_request.operation, "WRITE") == 0 || strcmp(client_request.operation, "GET_INFO") == 0)
    {
        // Handle READ operation
        StorageServer *ss = findStorageServerForPath(&hashmap, client_request.path);
        if (ss != NULL)
        {
            // Send storage server details to the client
            send(client_socket, ss, sizeof(StorageServer), 0);
            printf("details sent are: %s %d %d\n", ss->ip_address, ss->port_nm, ss->port_client);
        }
        else
        {
            // Send an error message to the client
            // send(client_socket, ss, 0, 0);
            printf("path not found on any storage server.\n");
            char error_message[] = "Path not found on any storage server.";
            send(client_socket, error_message, sizeof(error_message), 0);
        }
    }

    else if (strcmp(client_request.operation, "Dfo") == 0)
    {
        // print meow
        // printf("meow\n");
        NamingServerRequest naming_server_request;
        // recv(client_socket, &naming_server_request, sizeof(NamingServerRequest), 0);
        // send a request to the storage server to delete a file
        strcpy(naming_server_request.operation, "Dfo");
        strcpy(naming_server_request.path, client_request.path);
        // printf("Received a naming server request:\n");
        // printf("Operation: %s\n", naming_server_request.operation);
        printf("Path: %s\n", naming_server_request.path);
        StorageServer *ss = findStorageServerForPath(&hashmap, client_request.path);
        if (ss != NULL)
        {
            int server_socket = connectToStorageServer(ss->ip_address, ss->port_nm_persistent);
            // add log here
            logssConnection(ss->ip_address, ss->port_nm_persistent);

            send(server_socket, &naming_server_request, sizeof(NamingServerRequest), 0);
            char confirmation_message[256]; // Adjust the buffer size as needed
            recv(server_socket, confirmation_message, sizeof(confirmation_message), 0);
            printf("Received confirmation from storage server: %s\n", confirmation_message);

            // Log the confirmation
            logConfirmation(confirmation_message);

            send(client_socket, confirmation_message, sizeof(confirmation_message), 0);
        }
        else
        {
            printf("\npath not found on any storage server.\n");
            char error_message[] = "path not found on any storage server.";
            send(client_socket, error_message, sizeof(error_message), 0);
        }

        // send(storage_server_socket, client_request.path, sizeof(client_request.path), 0);
    }
    else if (strcmp(client_request.operation, "Dfi") == 0)
    {
        NamingServerRequest naming_server_request;
        // recv(client_socket, &naming_server_request, sizeof(NamingServerRequest), 0);
        // send a request to the storage server to delete a file
        strcpy(naming_server_request.operation, "Dfi");
        strcpy(naming_server_request.path, client_request.path);
        // printf("Received a naming server request:\n");
        // printf("Operation: %s\n", naming_server_request.operation);
        printf("Path: %s\n", naming_server_request.path);
        // printf("Name: %s\n", naming_server_request.name);
        StorageServer *ss = findStorageServerForPath(&hashmap, client_request.path);
        if (ss != NULL)
        {
            // Send storage server details to the client
            int server_socket = connectToStorageServer(ss->ip_address, ss->port_nm_persistent);

            // add log here
            logssConnection(ss->ip_address, ss->port_nm_persistent);

            send(server_socket, &naming_server_request, sizeof(NamingServerRequest), 0);
            char confirmation_message[256]; // Adjust the buffer size as needed
            recv(server_socket, confirmation_message, sizeof(confirmation_message), 0);
            printf("Received confirmation from storage server: %s\n", confirmation_message);

            // Log the confirmation
            logConfirmation(confirmation_message);

            send(client_socket, confirmation_message, sizeof(confirmation_message), 0);
        }
        else
        {
            // Send an error message to the client
            printf("\npath not found on any storage server.\n");
            char error_message[] = "path not found on any storage server.";
            send(client_socket, error_message, sizeof(error_message), 0);
            // send(client_socket, error_message, sizeof(error_message), 0);
        }
    }
    else if (strcmp(client_request.operation, "Cfo") == 0)
    {
        NamingServerRequest naming_server_request;

        // Extract the path up to the last occurrence of '/'
char partial_path[256];
const char *last_slash = strrchr(client_request.path, '/');
if (last_slash != NULL) {
    size_t length = last_slash - client_request.path;
    strncpy(partial_path, client_request.path, length);
    partial_path[length] = '\0';
} else {
    // If there is no '/', use the entire path
    strcpy(partial_path, client_request.path);
}

        // recv(client_socket, &naming_server_request, sizeof(NamingServerRequest), 0);
        // send a request to the storage server to delete a file
        strcpy(naming_server_request.operation, "Cfo");
        strcpy(naming_server_request.path, client_request.path);
        // printf("Received a naming server request:\n");
        // printf("Operation: %s\n", naming_server_request.operation);
        printf("Path: %s\n", naming_server_request.path);
        // printf("Name: %s\n", naming_server_request.name);
        StorageServer *ss = findStorageServerForPath(&hashmap, partial_path);
        if (ss != NULL)
        {
            // Send storage server details to the client
            int server_socket = connectToStorageServer(ss->ip_address, ss->port_nm_persistent);

            // add log here
            logssConnection(ss->ip_address, ss->port_nm_persistent);

            send(server_socket, &naming_server_request, sizeof(NamingServerRequest), 0);
            char confirmation_message[256]; // Adjust the buffer size as needed
            recv(server_socket, confirmation_message, sizeof(confirmation_message), 0);
            printf("Received confirmation from storage server: %s\n", confirmation_message);

            // Log the confirmation
            logConfirmation(confirmation_message);

            send(client_socket, confirmation_message, sizeof(confirmation_message), 0);
        }
        else
        {
            // Send an error message to the client
            printf("\npath not found on any storage server.\n");
            char error_message[] = "path not found on any storage server.";
            send(client_socket, error_message, sizeof(error_message), 0);
            // send(client_socket, error_message, sizeof(error_message), 0);
        }
    }
    else if (strcmp(client_request.operation, "Cfi") == 0)
    {
        NamingServerRequest naming_server_request;

        // Extract the path up to the last occurrence of '/'
char partial_path[256];
const char *last_slash = strrchr(client_request.path, '/');
if (last_slash != NULL) {
    size_t length = last_slash - client_request.path;
    strncpy(partial_path, client_request.path, length);
    partial_path[length] = '\0';
} else {
    // If there is no '/', use the entire path
    strcpy(partial_path, client_request.path);
}
        // recv(client_socket, &naming_server_request, sizeof(NamingServerRequest), 0);
        // send a request to the storage server to delete a file
        strcpy(naming_server_request.operation, "Cfi");
        strcpy(naming_server_request.path, client_request.path);
        // printf("Received a naming server request:\n");
        // printf("Operation: %s\n", naming_server_request.operation);
        printf("Path: %s\n", naming_server_request.path);
        // printf("Name: %s\n", naming_server_request.name);
        StorageServer *ss = findStorageServerForPath(&hashmap, partial_path);
        if (ss != NULL)
        {
            // Send storage server details to the client
            int server_socket = connectToStorageServer(ss->ip_address, ss->port_nm_persistent);

            // add log here
            logssConnection(ss->ip_address, ss->port_nm_persistent);

            send(server_socket, &naming_server_request, sizeof(NamingServerRequest), 0);
            char confirmation_message[256]; // Adjust the buffer size as needed
            recv(server_socket, confirmation_message, sizeof(confirmation_message), 0);
            printf("Received confirmation from storage server: %s\n", confirmation_message);

            // Log the confirmation
            logConfirmation(confirmation_message);

            send(client_socket, confirmation_message, sizeof(confirmation_message), 0);
        }
        else
        {
            // Send an error message to the client
            printf("\npath not found on any storage server.\n");
            char error_message[] = "path not found on any storage server.";
            send(client_socket, error_message, sizeof(error_message), 0);
            // send(client_socket, error_message, sizeof(error_message), 0);
        }
    }
    // close(client_socket);
}
int connectToStorageServer(char *ip_address, int port_nm_persistent)
{
    int server_socket;
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(ip_address);
    server_addr.sin_port = htons(port_nm_persistent);
    if ((server_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    {
        perror("Socket creation failed");
        return -1;
    }
    if (connect(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1)
    {
        perror("Connection to Storage Server failed");
        close(server_socket);
        return -1;
    }
    printf("Connected to Storage Server\n");
    return server_socket;
}

int main()
{
    int server_socket, client_socket;
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);


    // Maximum number of Storage Servers
    //const int MAX_SERVERS = 10; // Adjust as needed
    //StorageServer storage_servers[MAX_SERVERS];

    StorageServer *storage_servers = NULL; // Dynamic array for storage servers
    int num_servers = 0;

 

    // Initialize Naming Server
    if ((server_socket = initializeNamingServer()) == -1)
    {
        exit(EXIT_FAILURE);
    }

    // Example usage of the hashmap
    Hashmap hashmap;
    initializeHashmap(&hashmap);
      int i=0;
    // Accept connections and handle requests indefinitely
    while (1)
    {

        printf("Waiting for a connection...\n");

        // Accept a connection
        client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &client_len);
        if (client_socket == -1)
        {
            perror("Error accepting connection");
            continue; // Skip the rest of the loop and try again
        }

        // Check if it's a storage server registration
        char registration_flag;
        recv(client_socket, &registration_flag, sizeof(char), 0);

        if (registration_flag == 'S') // Storage Server registration
        {
            // Register Storage Server
            StorageServer ss;
            recv(client_socket, &ss, sizeof(StorageServer), 0);
            registerStorageServer(client_socket, ss);

            // Update the storage_servers array dynamically
            storage_servers = (StorageServer*)realloc(storage_servers, (num_servers + 1) * sizeof(StorageServer));
            if (storage_servers == NULL)
            {
                perror("Memory allocation failed");
                exit(EXIT_FAILURE);
            }
            storage_servers[num_servers++] = ss;

            // Update the hashmap
            for (int j = 0; j < 100 && strlen(storage_servers[i].accessible_paths[j]) > 0; ++j)
            {
                insertIntoHashmap(&hashmap, storage_servers[i].accessible_paths[j], &storage_servers[i]);
            }
             i++;
            //printf("Storage Server registered:\n");
            //printf("IP Address: %s\n", ss.ip_address);
            //printf("Port (NM): %d\n", ss.port_nm);
            //printf("Port (Client): %d\n", ss.port_client);
            //printf("Accessible Paths:\n");
            //for (int i = 0; i < 100 && strlen(ss.accessible_paths[i]) > 0; ++i)
            //{
                //printf("%s\n", ss.accessible_paths[i]);
            //}
        }
        else // Client request
        {
            // Log the accepted connection
            logAcceptedClientConnection(inet_ntoa(client_addr.sin_addr), PORT_NM);

            // Serve the client
            serveClient(client_socket, storage_servers, num_servers, hashmap);
        }
          
        // Close the client socket after serving
        close(client_socket);
    }
      free(storage_servers);
    // Close the server socket (this won't be reached in an infinite loop)
    close(server_socket);
    return 0;
}