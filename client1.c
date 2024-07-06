// client.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#define PORT_NM 12390
#define MAX_FILE_CONTENT 1024

typedef struct {
    char operation[10];
    char path[256];
} ClientRequest;

typedef struct {
    char ip_address[15];
    int port_client;
} StorageServerInfo;
typedef struct {
    char ip_address[15];
    int port_nm;
    int port_nm_persistent;
    int port_client;
    char accessible_paths[100][256];
} StorageServer;

void sendRequestToNamingServer(int server_socket, ClientRequest request) {
    send(server_socket, &request, sizeof(ClientRequest), 0);
}

void handleFileOperation(int storage_server_socket, ClientRequest request) {
    char buffer[MAX_FILE_CONTENT];

    // Send the request to the storage server
    send(storage_server_socket, &request, sizeof(ClientRequest), 0);
    if(strcmp(request.operation+1, "WRITE")==0){
        // Consume any remaining characters in the input buffer
        int c;
        while ((c = getchar()) != '\n' && c != EOF);

        // Prompt the user for data to write
        printf("Enter the data to write into the file:\n");

        // Read user input using fgets
        if (fgets(buffer, sizeof(buffer), stdin) == NULL) {
            perror("Error reading user input");
            return;
        }

        // Remove the newline character from the buffer
        size_t len = strlen(buffer);
        if (len > 0 && buffer[len - 1] == '\n') {
            buffer[len - 1] = '\0';
        }

        // Send the data to the storage server
        send(storage_server_socket, buffer, sizeof(buffer), 0);
    }
    // Receive the response from the storage server
    //recv(storage_server_socket, buffer, sizeof(buffer), 0);

    // Handle the response based on the operation
    if (strcmp(request.operation+1, "READ") == 0) {
        // Print the content of the file
        //printf("File Content:\n%s\n", buffer);
        ssize_t bytesRead;
        int count=0;

    while ((bytesRead = recv(storage_server_socket, buffer, MAX_FILE_CONTENT, 0)) > 0) {
        // Print or process the received data as needed
        //if (strcmp(buffer, "STOP") == 0) {
            //printf("Operation completed.\n");
            //break;
        //}
        count++;
        printf("%.*s", (int)bytesRead, buffer);
        if (count == 10) {
        // Send a "STOP" packet to the server
        const char* stopPacket = "STOP";
        printf("\nSending STOP as 10 packets are received\n");
        //send(storage_server_socket, stopPacket, strlen(stopPacket), 0);
        break;  // Exit the loop after sending the "STOP" packet
    }
        
    }

    if (bytesRead == -1) {
        perror("Error receiving file content");
    }
    } else if (strcmp(request.operation+1, "WRITE") == 0) {
         recv(storage_server_socket, buffer, sizeof(buffer), 0);
        // Print the confirmation message
        printf("\n%s\n",buffer);
    } else if (strcmp(request.operation+1, "GET_INFO") == 0) {
         recv(storage_server_socket, buffer, sizeof(buffer), 0);
        // Print file information (size and permissions)
        printf("File Information:\n%s\n", buffer);
    }
}

int main() {
    int server_socket;
    struct sockaddr_in server_addr;
    
    // Create socket
    if ((server_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Set up server address structure
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT_NM);

    // Connect to Naming Server
    if (connect(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        perror("Connection to Naming Server failed");
        close(server_socket);
        exit(EXIT_FAILURE);
    }

    printf("Connected to Naming Server\n");

    StorageServer ss;
    int storage_server_socket;
    int nb=0;
    // Take user input for client request
    ClientRequest request;
    printf("Welcome to the network file system!\n");
    printf("1.Enter operation (READ/WRITE/GET_INFO):\n ");
    printf("2.Enter operation (Create folder[Cfo]/Create file[Cfi]/Delete folder[Dfo]/Delete file[Dfi] ):\n");
    printf("3.Enter exit to exit the program.\n ");
    scanf("%s", request.operation);
    while(strcmp(request.operation, "exit") != 0)
    {
        if(nb==1){
            // Connect to Naming Server
              server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        perror("Connection to Naming Server failed");
        close(server_socket);
        exit(EXIT_FAILURE);
    }

    printf("Connected to Naming Server\n");

        }
        //handles the READ request
       if(strcmp(request.operation, "READ") == 0 || strcmp(request.operation, "WRITE") == 0 || strcmp(request.operation, "GET_INFO") == 0 )
       {
        // Take user input for file path
        printf("Enter file path: ");
        scanf("%s", request.path);
         int l=strlen(request.operation);
         int length=strlen(request.path);
         char newstring[l+2];
         char newstringpath[length+2];
         newstring[0]='C';
         newstringpath[0]='C';
         strcpy(newstring+1,request.operation);
         strcpy(request.operation,newstring);
         //printf("%s\n",request.operation);

         strcpy(newstringpath+1,request.path);
         strcpy(request.path,newstringpath);
          //printf("%s\n",request.path);

        // Send request to Naming Server
        sendRequestToNamingServer(server_socket, request);

        // Receive Storage Server information
        int a=0;
       int result= recv(server_socket, &ss, sizeof(StorageServer), 0);
    //printf("%d\n",result);
        if (result==sizeof(StorageServer)) {
            // Print storage server details
            printf("Storage Server Details:\n");
            printf("IP Address: %s\n", ss.ip_address);
            printf("Port (NM): %d\n", ss.port_nm);
            printf("Port (Client): %d\n", ss.port_client);
            printf("Accessible Paths:\n");
            for (int i = 0; i < 100 && strlen(ss.accessible_paths[i]) > 0; ++i) 
            {
                printf("%s\n", ss.accessible_paths[i]);
            }
        } 
        else {
            // Handle other operations (WRITE, GET_INFO)
  
    printf("Path not found on any storage server.\n");
    a=1;
        }

        // Close connection to Naming Server
        //close(server_socket);

        // Connect to Storage Server
        if(a==0){
        struct sockaddr_in storage_server_addr;

        // Create socket
        if ((storage_server_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
            perror("Socket creation failed");
            exit(EXIT_FAILURE);
        }

        // Set up storage server address structure
        storage_server_addr.sin_family = AF_INET;
        storage_server_addr.sin_addr.s_addr = inet_addr(ss.ip_address);
        storage_server_addr.sin_port = htons(ss.port_client);

        // Connect to Storage Server
        if (connect(storage_server_socket, (struct sockaddr *)&storage_server_addr, sizeof(storage_server_addr)) == -1) 
        {
            
            perror("Connection to Storage Server failed");
            close(storage_server_socket);
            exit(EXIT_FAILURE);
        }

        printf("Connected to Storage Server\n");
        
        // Handle file operations like READ, WRITE, GET_INFO
        handleFileOperation(storage_server_socket, request);
        close(storage_server_socket);
        }
        }
        else if (strcmp(request.operation, "Cfo") == 0 || strcmp(request.operation, "Cfi") == 0 ) 
        {
            //input the path at which it should be created
            printf("Enter the path at which it should be created:\n");
            scanf("%s", request.path);


             int l=strlen(request.operation);
         int length=strlen(request.path);
         char newstring[l+2];
         char newstringpath[length+2];
         newstring[0]='C';
         newstringpath[0]='C';
         strcpy(newstring+1,request.operation);
         strcpy(request.operation,newstring);
         //printf("%s\n",request.operation);

         strcpy(newstringpath+1,request.path);
         strcpy(request.path,newstringpath);
          //printf("%s\n",request.path);
            //send the request to the naming server
            sendRequestToNamingServer(server_socket, request);
             char confirmation_message[256]; // Adjust the buffer size as needed
    recv(server_socket, confirmation_message, sizeof(confirmation_message), 0);

    // Process the confirmation message as needed
    printf("Received confirmation from naming server: %s\n", confirmation_message);
        }

        else if (strcmp(request.operation, "Dfo") == 0 || strcmp(request.operation, "Dfi") == 0)
        {
            printf("Enter the path which it should be deleted:\n");
            scanf("%s", request.path);

             int l=strlen(request.operation);
         int length=strlen(request.path);
         char newstring[l+2];
         char newstringpath[length+2];
         newstring[0]='C';
         newstringpath[0]='C';
         strcpy(newstring+1,request.operation);
         strcpy(request.operation,newstring);
         //printf("%s\n",request.operation);

         strcpy(newstringpath+1,request.path);
         strcpy(request.path,newstringpath);
          //printf("%s\n",request.path);

            //send the request to the naming server
            sendRequestToNamingServer(server_socket, request);
             char confirmation_message[256]; // Adjust the buffer size as needed
    recv(server_socket, confirmation_message, sizeof(confirmation_message), 0);

    // Process the confirmation message as needed
    printf("Received confirmation from naming server: %s\n", confirmation_message);
        }
        else
        {
            printf("Invalid operation\n");
        }
        // Close the socket

        printf("1.Enter operation (READ/WRITE/GET_INFO):\n ");
        printf("2.Enter operation (Create folder[Cfo]/Create file[Cfi]/Delete folder[Dfo]/Delete file[Dfi] ):\n");
        printf("3.Enter exit to exit the program.\n ");
        scanf("%s", request.operation);
         nb=1;
         close(server_socket);
    }
    //close(storage_server_socket);
    //close(server_socket);
    return 0;
}
