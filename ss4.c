// storage_server.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/select.h>


#define IP_ADDRESS "127.0.0.1"
#define PORT_CLIENT 12367
#define PORT_NM 12390
#define MAX_FILE_CONTENT 1024
#define PORT_NM_PERSISTENT 7086
typedef struct {
    char ip_address[15];
    int port_nm;
} NamingServer;

typedef struct {
    char ip_address[15];
    int port_nm;
    int port_nm_persistent;
    int port_client;
    char accessible_paths[100][256];
} StorageServer;

typedef struct {
    char operation[10];  // Added for handling READ/WRITE operations
    char path[256];      // Added for handling the requested file path
} ClientRequest;


typedef struct
{
    char operation[10];
    char path[256];
    char name[256];


} NamingServerRequest;
// Function to create a non-blocking socket which is used to have a persistent connection  with naming server
// int createNonBlockingSocket(int port) {
//     int server_socket;
//     struct sockaddr_in server_addr;
//     // Create socket
//     if ((server_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
//         perror("Socket creation failed");
//         return -1;
//     }
//     // Set socket as non-blocking
//     int flags = fcntl(server_socket, F_GETFL, 0);
//     if (flags == -1) {
//         perror("fcntl F_GETFL");
//         close(server_socket);
//         return -1;
//     }
//     if (fcntl(server_socket, F_SETFL, flags | O_NONBLOCK) == -1) {
//         perror("fcntl F_SETFL");
//         close(server_socket);
//         return -1;
//     }
//     // Set up server address structure
//     server_addr.sin_family = AF_INET;
//     server_addr.sin_addr.s_addr = INADDR_ANY;
//     server_addr.sin_port = htons(port);
//     // Bind the socket
//     if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
//         perror("Binding failed");
//         close(server_socket);
//         return -1;
//     }
//     // Listen for incoming connections
//     if (listen(server_socket, 1) == -1) {
//         perror("Listening failed");
//         close(server_socket);
//         return -1;
//     }
//     printf("Non-blocking socket is listening on port %d...\n", port);
//     return server_socket;
// } 

//used to initialize storage server
int connect_to_ns_for_the_first_time(NamingServer nm) {
    int client_socket;
    struct sockaddr_in server_addr;

    // Create socket
    if ((client_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("Socket creation failed");
        return -1;
    }

    // Set up server address structure
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(nm.ip_address);
    server_addr.sin_port = htons(nm.port_nm);

    // Connect to Naming Server
    if (connect(client_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        perror("Connection to Naming Server failed");
        close(client_socket);
        return -1;
    }

    printf("Connected to Naming Server\n");
// Send "S" signal to Naming Server
    char signal = 'S';
    if (send(client_socket, &signal, sizeof(signal), 0) == -1) {
        perror("Error sending signal to Naming Server");
        close(client_socket);
        return -1;
    }

    return client_socket;
}


//this is listening to client
int initializeStorageServer(StorageServer ss) 
{
    int server_socket_lisetning_to_client;
    printf("y\n");
    // Create socket
    if ((server_socket_lisetning_to_client = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("Socket creation failed");
        return -1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(ss.port_client);

    // Bind the socket
    if (bind(server_socket_lisetning_to_client, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        perror("Binding failed");
        close(server_socket_lisetning_to_client);
        return -1;
    }

    // Listen for incoming connections
    if (listen(server_socket_lisetning_to_client, 1) == -1) {
        perror("Listening failed");
        close(server_socket_lisetning_to_client);
        return -1;
    }

    printf("Storage Server is listening on port for client %d...\n", ss.port_client);

    return server_socket_lisetning_to_client;
}

// Function to create a blocking socket for the Storage Server to listen for the Naming Server
int initializeStorageServerForNamingServer(StorageServer ss)
{
    int server_socket;
    struct sockaddr_in server_addr;

    // Create socket
    if ((server_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("Socket creation failed");
        return -1;
    }

    // Set up server address structure
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(ss.port_nm_persistent);

    // Bind the socket
    if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        perror("Binding failed");
        close(server_socket);
        return -1;
    }

    // Listen for incoming connections
    if (listen(server_socket, 1) == -1) {
        perror("Listening failed");
        close(server_socket);
        return -1;
    }

    printf("Storage Server is listening for Naming Server on port %d...\n", ss.port_nm_persistent);

    return server_socket;
}
void handleNamingServerRequest(int ns_socket, NamingServerRequest request )
{   
    //print inside the handle naming server request
    printf("Received request from Naming Server:\n");
    if (strcmp(request.operation, "Cfi")== 0)
    {
        FILE *file = fopen(request.path, "w");
        if (file != NULL) {
            fclose(file);
            // Send a confirmation message to the client
            char confirmation_message[] = "file created successfully.";
            send(ns_socket, confirmation_message, sizeof(confirmation_message), 0);
        } else {
            // Send an error message to the client
            char error_message[] = "Error creating file.";
            send(ns_socket, error_message, sizeof(error_message), 0);
        }
    }
    else if (strcmp(request.operation, "Cfo")== 0)
    {
        mkdir(request.path, 0777);
        // Send a confirmation message to the client
        char confirmation_message[] = "Made directory successfully.";
        send(ns_socket, confirmation_message, sizeof(confirmation_message), 0);
    }
    else if (strcmp(request.operation, "Dfi")== 0)
    {
        if (remove(request.path) == 0)
        {
            // Send a confirmation message to the client
            char confirmation_message[] = " File removed successfully.";
            send(ns_socket, confirmation_message, sizeof(confirmation_message), 0);
        }
        else
        {
            // Send an error message to the client
            char error_message[] = "Could not remove file.";
            send(ns_socket, error_message, sizeof(error_message), 0);
        }
    }
    else if (strcmp(request.operation, "Dfo")== 0)
    {
        if (rmdir(request.path) == 0)
        {
            // Send a confirmation message to the client
            char confirmation_message[] = "Directory removed succefully.";
            printf("\nDirectory removed succefully.\n");
            send(ns_socket, confirmation_message, sizeof(confirmation_message), 0);
        }
        else
        {
            // Send an error message to the client
            char error_message[] = "Could not remove directory.";
            send(ns_socket, error_message, sizeof(error_message), 0);
        }
    }
    
    }

// Function to handle client requests
void handleClientRequest(int client_socket, ClientRequest request)
{
    char buffer[MAX_FILE_CONTENT];

    if (strcmp(request.operation+1, "READ") == 0) {
        // Read from the file and send the content to the client
        FILE *file = fopen(request.path+1, "r");
        if (file != NULL) {
    //size_t bytesRead;
     //int count=0;
   // while ((bytesRead = fread(buffer, sizeof(char), sizeof(buffer), file)) > 0) {
        //send(client_socket, buffer, bytesRead, 0);
        //count++;
        //if(count>=2){
             //send(client_socket, "STOP", strlen("STOP"), 0);
             //break;
        //}
    //}
            //fread(buffer, sizeof(char), sizeof(buffer), file);
            //fclose(file);
            //send(client_socket, buffer, sizeof(buffer), 0);
            size_t bytesRead;
        char stopPacket[] = "STOP";
        int stopPacketReceived = 0;
        int count=0;
        while ((bytesRead = fread(buffer, sizeof(char), sizeof(buffer), file)) > 0) {
            send(client_socket, buffer, bytesRead, 0);
             count++;
            // Check for a "STOP" packet from the client
            //char peekBuffer[sizeof(stopPacket)];
            /*if (recv(client_socket, peekBuffer, sizeof(peekBuffer), MSG_PEEK | MSG_DONTWAIT) > 0) {
                if (strcmp(peekBuffer, stopPacket) == 0) {
                    //recv(client_socket, buffer, sizeof(buffer), 0);
                    printf("Received STOP packet. Stopping further transmission.\n");
                    stopPacketReceived = 1;
                    break;
                }
            }*/
            if(count==10){
                printf("Received STOP packet. Stopping further transmission.\n");
                    stopPacketReceived = 1;
                    break;
            }
        }

        fclose(file);

        // If a "STOP" packet was received, send it back to acknowledge
        //if (stopPacketReceived) {
            //send(client_socket, stopPacket, strlen(stopPacket), 0);
        //}
        } else {
            // Send an error message to the client
            char error_message[] = "File not found.";
            send(client_socket, error_message, sizeof(error_message), 0);
        }
    } else if (strcmp(request.operation+1, "WRITE") == 0) {
        // Receive data from the client and write to the file
        recv(client_socket, buffer, sizeof(buffer), 0);
        FILE *file = fopen(request.path+1, "w");
        if (file != NULL) {
            fwrite(buffer, sizeof(char), strlen(buffer), file);
            fclose(file);
            // Send a confirmation message to the client
            char confirmation_message[] = "Write operation successful.";
            send(client_socket, confirmation_message, sizeof(confirmation_message), 0);
        } else {
            // Send an error message to the client
            char error_message[] = "Error writing to the file.";
            send(client_socket, error_message, sizeof(error_message), 0);
        }
    } else if (strcmp(request.operation+1, "GET_INFO") == 0) {
        // Get file information (size and permissions) and send to the client
        struct stat file_stat;
        if (stat(request.path+1, &file_stat) == 0) {
            sprintf(buffer, "File Size: %ld bytes\nFile Permissions: %o", file_stat.st_size, file_stat.st_mode);
            send(client_socket, buffer, sizeof(buffer), 0);
        } else {
            // Send an error message to the client
            char error_message[] = "Error getting file information.";
            send(client_socket, error_message, sizeof(error_message), 0);
        }
    } else {
        // Handle other operations (if needed)
    }

    close(client_socket);
}
void sendDetailsToNamingServer(int server_socket, StorageServer ss)
{
    send(server_socket, &ss, sizeof(StorageServer), 0);
}

int main()
{
   
    // Connect to Naming Server
    NamingServer nm;
    // printf("Enter Naming Server IP address: ");
    // scanf("%s", nm.ip_address);
    //set the value of nm.ipaddress as 127.0.0.1
    strcpy(nm.ip_address,IP_ADDRESS);
    //12350 the port initially used to initialize the stioarge server
    printf("Enter Naming Server port: ");
    scanf("%d", &nm.port_nm);

    int nm_socket;
    if ((nm_socket = connect_to_ns_for_the_first_time(nm)) == -1) {
        //close(server_socket);
        exit(EXIT_FAILURE);
    }

    // Take user input for Storage Server details
    StorageServer ss;
    // printf("Enter Storage Server IP address: ");
    // scanf("%s", ss.ip_address);
    //set the value of ss.ipaddress as
    strcpy(ss.ip_address,IP_ADDRESS);
    // printf("Enter Storage Server port to be persistently accessed by naming server(NM): ");
    // scanf("%d", &ss.port_nm_persistent);
    // printf("Enter Storage Server port to be accessed by client: ");
    // scanf("%d", &ss.port_client);
    


    ss.port_nm = PORT_NM;
    ss.port_client = PORT_CLIENT;
    ss.port_nm_persistent = PORT_NM_PERSISTENT;
    int num_paths;
    printf("Enter the number of accessible paths: ");
    scanf("%d", &num_paths);

    printf("Enter the accessible paths:\n");
    for (int i = 0; i < num_paths; i++) {
        scanf("%s", ss.accessible_paths[i]);
    }
     
    // Example: Send vital details to Naming Server
    sendDetailsToNamingServer(nm_socket, ss);

    // Additional Storage Server logic goes here

    // Close the sockets
    //close(server_socket);
    close(nm_socket);

    int server_socket_listening_to_ns =  initializeStorageServerForNamingServer(ss);
    if (server_socket_listening_to_ns == -1)
    {
        exit(EXIT_FAILURE);
    }

    // Initialize Storage Server
    int server_socket_listening_to_client = initializeStorageServer(ss);
    if (server_socket_listening_to_client == -1)
    {
        exit(EXIT_FAILURE);
    }

    fd_set readfds;
    int max_sd = (server_socket_listening_to_client > server_socket_listening_to_ns) ?
                     server_socket_listening_to_client : server_socket_listening_to_ns;

    // Accept incoming connections and handle client requests
    while (1) {
        
       



        // printf("\n");

        FD_ZERO(&readfds);
        FD_SET(server_socket_listening_to_client, &readfds);
        FD_SET(server_socket_listening_to_ns, &readfds);

        // Monitor the sockets
        if (select(max_sd + 1, &readfds, NULL, NULL, NULL) == -1) {
            perror("Select error");
            exit(EXIT_FAILURE);
        }

        // Check for Naming Server connection
        if (FD_ISSET(server_socket_listening_to_ns, &readfds)) {
            int ns_client_socket;
            struct sockaddr_in ns_client_addr;
            socklen_t ns_client_len = sizeof(ns_client_addr);

            ns_client_socket = accept(server_socket_listening_to_ns, (struct sockaddr *)&ns_client_addr, &ns_client_len);
            if (ns_client_socket == -1) {
                perror("Error accepting Naming Server connection");
                continue;
            }

            printf("Accepted a Naming Server connection\n");

            //receive naming server request
            NamingServerRequest ns_request;
            recv(ns_client_socket, &ns_request, sizeof(NamingServerRequest), 0);
            printf("%s\n",ns_request.operation);
            // Handle Naming Server request
            handleNamingServerRequest(ns_client_socket, ns_request);

            // Handle communication with Naming Server if needed
            close(ns_client_socket); // Close the connection for now
        }

        // Check for Client connection
        if (FD_ISSET(server_socket_listening_to_client, &readfds)) {
            int client_socket;
            struct sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);

            client_socket = accept(server_socket_listening_to_client, (struct sockaddr *)&client_addr, &client_len);
            if (client_socket == -1) {
                perror("Error accepting client connection");
                continue;
            }

            printf("Accepted a client connection\n");

            // Receive client request
            ClientRequest client_request;
            recv(client_socket, &client_request, sizeof(ClientRequest), 0);

            // Handle client request
            handleClientRequest(client_socket, client_request);

            printf("\n");
        }


    }

    // Close the server socket
    //close(server_socket);
     close(server_socket_listening_to_client);
     close(server_socket_listening_to_ns);
     

    return 0;
}
