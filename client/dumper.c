#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <math.h>

#define PORT 8080

int main(int argc, char const *argv[]) {
    char username[1024];
    char password[1024];
    char error[1024];
    char database[1024];
    
    memset(username, 0, sizeof(username));
    memset(password, 0, sizeof(password));
    memset(error, 0, sizeof(error));

    if (argc == 2 && geteuid() == 0) {
        sprintf(username, "root");
        sprintf(password, "root");
        sprintf(database, "%s", argv[1]);
    } else if (argc == 6) {
        int fu = 0, fp = 0;
        for (int i = 0; i < argc; ++i) {
            if (strcmp(argv[i], "-u") == 0) {
                sprintf(username, "%s", argv[i+1]);
                fu = 1;
            } else if (strcmp(argv[i], "-p") == 0) {
                sprintf(password, "%s", argv[i+1]);
                fp = 1;
            } else {
                if (i == 0) {
                    continue;
                }
                if (strcmp(argv[i - 1], "-p") != 0 && strcmp(argv[i - 1], "-u") != 0 && strcmp(argv[i], "-p") != 0 && strcmp(argv[i], "-u") != 0) {
                    sprintf(database, "%s", argv[i]);
                }
            }
        }
        if (fu == 0 || fp == 0) {
            sprintf(error, "Usage: ./yoursql -u username -p password database");
            printf("%s\n", error);
            exit(EXIT_FAILURE);
        }
    } else {
        sprintf(error, "Usage: ./yoursql -u username -p password database");
        printf("%s\n", error);
        exit(EXIT_FAILURE);
    }


    struct sockaddr_in address;
    int sock = 0;
    struct sockaddr_in serv_addr;

    // msg for server
    char input[1024];

    // msg from server
    char buffer[1024];

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\n Socket creation error \n");
        return -1;
    }
  
    memset(&serv_addr, '0', sizeof(serv_addr));
  
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
      
    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        printf("\nInvalid address/ Address not supported \n");
        return -1;
    }
  
    // jika gagal menyambungkan ke server manapun maka tampil "Connection Failed"
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        printf("\nConnection Failed \n");
        return -1;
    }


    // code here
    memset(buffer, 0, sizeof(buffer));
    
    send(sock, username, strlen(username), 0);
    sleep(1);
    send(sock, password, strlen(password), 0);

    // success or fail
    read(sock, buffer, 1024);
    if (strcmp(buffer, "login success") != 0) {
        printf("Failed to login. Exiting ...\n");
        close(sock);
        return 0;
    }

    sprintf(input, "dump-database %s", database);
    send(sock, input, strlen(input), 0);
    read(sock, buffer, 1024);
    printf("%s\n", buffer);
    
    close(sock);

    return 0;
}