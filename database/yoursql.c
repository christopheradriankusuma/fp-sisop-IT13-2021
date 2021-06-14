#include <stdio.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <ctype.h>
#include <fcntl.h>
#include <errno.h>
#include <syslog.h>
#include <time.h>
#include <wait.h>

//mendefinisikan port server
#define PORT 9000
#define SO_REUSEPORT 15

typedef struct column {
    char name[1024];
    char type[8];
    char contents[100][1024];
} column;

int parsed_row, parsed_col;
char database[1024] = "";
char username[1024] = "";
char password[1024] = "";
char buffer[1024] = "";
char *server_path = "/home/kali/Desktop/Sisop/FP/fp-sisop-IT13-2021/database/databases";
char query_result[1024] = "";
char error[1024] = "";

void logging(const char *command) {
    time_t t = time(NULL);
    struct tm tm = *localtime(&t);

    FILE *fptr;
    fptr = fopen("db.log", "a");

    fprintf(fptr, "%4d-%2d:%2d %2d:%2d:%2d:%s:%s\n", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, username, command);

    fclose(fptr);
}

void count_column(char *db, char *table) {
    char file[1024], buf[1024];
    sprintf(file, "%s/%s", db, table);

    FILE *fptr;
    fptr = fopen(file, "r");

    fseek(fptr, 0, SEEK_END);
    long fsize = ftell(fptr);
    rewind(fptr);

    fread(buf, 1, fsize, fptr);

    int col = 0;
    for (char *itr = buf; *itr != '\n'; ++itr) {
        if (*itr == '\t') ++col;
    }

    parsed_col = col + 1;
    
    fclose(fptr);
}

char *copy_row(column *cols, int row, int col) {
    char *buff;
    buff = (char*)malloc(sizeof(char)*1024);
    memset(buff, 0, sizeof(buff));
    if (row == 0) {
        for (int c = 0; c < col - 1; ++c) {
            sprintf(buff, "%s%s|%s\t", buff, cols[c].name, cols[c].type);
        }
        sprintf(buff, "%s%s|%s", buff, cols[col - 1].name, cols[col - 1].type);
        return buff;
    }

    for (int k = 0; k < parsed_row; ++k) {
        if (k == row) {
            for (int c = 0; c < col - 1; ++c) {
                sprintf(buff, "%s%s\t", buff, cols[c].contents[k]);
            }
            sprintf(buff, "%s%s", buff, cols[col - 1].contents[k]);
        }
    }

    return buff;
}

char *copy_partial_row(column *cols, int argc, char *argv[], int row, int col) {
    char *buff;
    buff = (char*)malloc(sizeof(char)*1024);
    memset(buff, 0, sizeof(buff));
    if (row == 0) {
        for (int c = 0; c < col; ++c) {
            for (int i = 0; i < argc; ++i) {
                if (strcmp(cols[c].name, argv[i]) == 0) {
                    if (c < col - 1) {
                        sprintf(buff, "%s%s|%s\t", buff, cols[c].name, cols[c].type);
                    } else {
                        sprintf(buff, "%s%s|%s", buff, cols[c].name, cols[c].type);
                    }
                }
            }
        }
        return buff;
    }

    for (int k = 0; k < parsed_row; ++k) {
        if (k == row) {
            for (int c = 0; c < col; ++c) {
                for (int i = 0; i < argc; ++i) {
                    if (strcmp(cols[c].name, argv[i]) == 0) {
                        if (c < col - 1) {
                            sprintf(buff, "%s%s\t", buff, cols[c].contents[k]);
                        } else {
                            sprintf(buff, "%s%s", buff, cols[c].contents[k]);
                        }
                    }
                }
            }
        }
        // printf("\n");
    }

    return buff;

}

column* parse_db(char *db, char *table) {
    char file[1024];
    sprintf(file, "%s/%s", db, table);

    FILE *fptr;
    fptr = fopen(file, "r");

    char buf[4096];
    memset(buf, 0, sizeof(buf));

    fseek(fptr, 0, SEEK_END);
    long fsize = ftell(fptr);
    rewind(fptr);

    fread(buf, 1, fsize, fptr);

    char *token;
    token = strchr(buf, '\n');

    int col = 0, i, row = 0;
    char line[1024];
    sprintf(line, "%.*s", token - buf, buf);

    count_column(db, table);

    column *cols;
    cols = (column*)malloc(sizeof(column) * parsed_col);
    
    token = strtok(buf, "\n");
    
    while (token != NULL) {
        i = 0;
        char *mline, value[1024];
        mline = (char*)malloc(sizeof(char)*1024);
        sprintf(mline, "%s", token);
        memset(value, 0, sizeof(value));

        char *delim, first[1024];
        for (char *itr = mline; *itr != '\0'; ++itr) {
            if (*itr == '\t') {
                if (row == 0) {
                    sprintf(first, "%s", value);

                    delim = strchr(first, '|');
                    sprintf(cols[i].name, "%.*s", delim - first, first);
                    sprintf(cols[i].type, "%s", delim+1);
                } else {
                    strcpy(cols[i].contents[row], value);
                }

                ++i;
                memset(value, 0, sizeof(value));
            } else {
                strncat(value, itr, 1);
            }
        }

        if (row == 0) {
            sprintf(first, "%s", value);                    
            delim = strchr(first, '|');
            sprintf(cols[i].name, "%.*s", delim - first, first);
            sprintf(cols[i].type, "%s", delim+1);
        } else {
            strcat(cols[i].contents[row], value);
        }

        ++row;
        token = strtok(NULL, "\n");
    }
    parsed_row = row;

    // for (int k = 0; k < row; ++k) {
    //     for (int c = 0; c < col; ++c) {
    //         if (k == 0) {
    //             printf("%s|%s\t", cols[c].name, cols[c].type);
    //         } else {
    //             printf("%s\t", cols[c].contents[k]);
    //         }
    //     }
    //     printf("\n");
    // }

    fclose(fptr);

    return cols;
}


void db_create_table(char *db, char *table_name, column *columns, int num_col) {
    char table_path[1024];
    sprintf(table_path, "%s/%s", db, table_name);
    if (access(table_path, F_OK) == 0) {
        sprintf(error, "Table %s exists!", table_name);
        return;
    }

    FILE *fptr;
    fptr = fopen(table_path, "w");

    for (int i = 0; i < num_col - 1; ++i) {
        fprintf(fptr, "%s|%s\t", columns[i].name, columns[i].type);
    }
    fprintf(fptr, "%s|%s\n", columns[num_col - 1].name, columns[num_col - 1].type);

    fclose(fptr);
}

char* db_select(char *db, char *table, int argc, char *argv[], char *left, char *right) {
    char file[2048];
    sprintf(file, "%s/%s", db, table);
    if (access(file, F_OK) != 0) {
        sprintf(error, "Table %s doesn't exist!", table);
        return NULL;
    }

    FILE *fptr;
    fptr = fopen(file, "r");

    char buf[1024] = "";

    fseek(fptr, 0, SEEK_END);
    long fsize = ftell(fptr);
    rewind(fptr);

    fread(buf, 1, fsize, fptr);

    int i = 0, pos = -1;
    column *cols;
    cols = parse_db(db, table);

    char *buff;
    buff = (char*)malloc(sizeof(char)*1024);

    memset(buff, 0, strlen(buff));

    if (strlen(left) && strlen(right)) {
        if (argc == 1 && strcmp(argv[0], "*") == 0) {
            sprintf(buff, "%s\n", copy_row(cols, 0, parsed_col));
            for (int k = 1; k < parsed_row; ++k) {
                for (int c = 0; c < parsed_col; ++c) {
                    if (strcmp(cols[c].name, left) == 0 && strcmp(cols[c].contents[k], right) == 0) {
                        sprintf(buff, "%s%s\n", buff, copy_row(cols, k, parsed_col));
                    }
                }
            }
        } else {
            sprintf(buff, "%s\n", copy_partial_row(cols, argc, argv, 0, parsed_col));
            for (int k = 1; k < parsed_row; ++k) {
                for (int c = 0; c < parsed_col; ++c) {
                    if (strcmp(cols[c].name, left) == 0 && strcmp(cols[c].contents[k], right) == 0) {
                        sprintf(buff, "%s%s\n", buff, copy_partial_row(cols, argc, argv, k, parsed_col));
                    }
                }
            }
        }
    } else if (argc == 1 && strcmp(argv[0], "*") == 0) {
        sprintf(buff, "%s", buf);
    } else {    
        sprintf(buff, "%s\n", copy_partial_row(cols, argc, argv, 0, parsed_col));
        for (int k = 1; k < parsed_row; ++k) {
            sprintf(buff, "%s%s\n", buff, copy_partial_row(cols, argc, argv, k, parsed_col));
        }
    }

    *(buff + strlen(buff) - 1) = 0;

    fclose(fptr);
    return buff;
}

void db_insert(char *table, int argc, char *argv[]) {
    char file[1024];    
    sprintf(file, "%s/%s", database, table);
    if (access(file, F_OK) != 0) {
        sprintf(error, "Table %s doesn't exists!", table);
        return;
    }

    FILE *fptr;
    fptr = fopen(file, "a");

    for (int i = 0; i < argc - 1; ++i) {
        fprintf(fptr, "%s\t", argv[i]);
    }
    fprintf(fptr, "%s\n", argv[argc-1]);

    fclose(fptr);
}

void db_update(char *table, char *set_left, char *set_right, char *left, char *right) {
    char file[1024];
    sprintf(file, "%s/%s", database, table);
    if (access(file, F_OK) != 0) {
        sprintf(error, "Table %s doesn't exists!", table);
        return;
    }

    column *cols = parse_db(database, table);

    if (strlen(left) && strlen(right)) {
        int idx_left;
        for (int c = 0; c < parsed_col; ++c) {
            if (strcmp(cols[c].name, left) == 0) {
                idx_left = c;
                break;
            }
        }
        for (int c = 0; c < parsed_col; ++c) {
            if (strcmp(cols[c].name, set_left) == 0) {
                for (int r = 1; r < parsed_row; ++r) {
                    if (strcmp(cols[idx_left].contents[r], right) == 0) {
                        sprintf(cols[c].contents[r], "%s", set_right);
                    }
                }
            }
        }

        FILE *fptr;
        fptr = fopen(file, "w");

        for (int r = 0; r < parsed_row; ++r) {
            fprintf(fptr, "%s\n", copy_row(cols, r, parsed_col));
        }

        fclose(fptr);
    } else {
        for (int c = 0; c < parsed_col; ++c) {
            if (strcmp(cols[c].name, set_left) == 0) {
                for (int r = 1; r < parsed_row; ++r) {
                    sprintf(cols[c].contents[r], "%s", set_right);
                }
            }
        }

        FILE *fptr;
        fptr = fopen(file, "w");
        
        for (int r = 0; r < parsed_row; ++r) {
            fprintf(fptr, "%s\n", copy_row(cols, r, parsed_col));
        }

        fclose(fptr);
    }
}

void db_delete(char *table, char *left, char *right) {
    char file[1024];
    sprintf(file, "%s/%s", database, table);
    if (access(file, F_OK) != 0) {
        sprintf(error, "Table %s doesn't exists!", table);
        return;
    }

    FILE *fptr;
    fptr = fopen(file, "r");

    column *cols = parse_db(database, table);

    if (strlen(left) && strlen(right)) {
        char buf[1024];
        memset(buf, 0, sizeof(buf));

        for (int c = 0; c < parsed_col; ++c) {
            sprintf(buf, "%s%s|%s\t", buf, cols[c].name, cols[c].type);
        }
        buf[strlen(buf) - 1] = '\n';
        
        for (int r = 1; r < parsed_row; ++r) {
            // printf("%d %s\n", r, copy_row(cols, r, parsed_col));
            int found = 0;
            for (int c = 0; c < parsed_col; ++c) {
                if (strcmp(cols[c].name, left) == 0 && strcmp(cols[c].contents[r], right) == 0) {
                    found = 1;
                    break;
                }
            }
            if (found == 0) {
                sprintf(buf, "%s%s\n", buf, copy_row(cols, r, parsed_col));
            }
        }

        buf[strlen(buf) - 1] = 0;

        fptr = freopen(file, "w", fptr);
        fprintf(fptr, "%s\n", buf);
    } else {
        char line[1024];
        fgets(line, 1024, fptr);
        line[strlen(line) - 1] = 0;
        fptr = freopen(file, "w", fptr);
        fprintf(fptr, "%s\n", line);
    }

    fclose(fptr);
}

void db_drop_database(char *db) {
    FILE *fptr;
    fptr = fopen("rootdb/db_permission", "r");

    char buff[1024], rewrite[1024];

    fseek(fptr, 0, SEEK_END);
    long fsize = ftell(fptr);
    rewind(fptr);

    fread(buff, fsize, 1, fptr);

    char *token;
    token = strtok(buff, "\n");
    sprintf(rewrite, "%s", token);
    token = strtok(NULL, "\n");
    
    while (token != NULL) {
        char *tab;
        tab = strchr(token, '\t');

        if (strcmp(tab+1, db) != 0) {
            // printf("%s\n", token);
            sprintf(rewrite, "%s\n%s", rewrite, token);
        }

        token = strtok(NULL, "\n");
    }

    fptr = freopen("rootdb/db_permission", "w", fptr);

    char del[1024];
    sprintf(del, "rm -rf %s", db);
    if(system(del)) {
        printf("%s\n", del);
    }
    fprintf(fptr, "%s\n", rewrite);

    fclose(fptr);

    memset(database, 0, sizeof(database));
}

void db_drop_table(char *table) {
    char file[1024];
    sprintf(file, "%s/%s", database, table);
    if (access(file, F_OK) != 0) {
        sprintf(error, "Table %s doesn't exists!", table);
        return;
    }

    remove(file);
}

void db_drop_column(char *table, char *col) {
    char file[1024];
    sprintf(file, "%s/%s", database, table);
    if (access(file, F_OK) != 0) {
        sprintf(error, "Table %s doesn't exists!", table);
        return;
    }
    
    column *cols = parse_db(database, table);

    FILE *fptr;
    fptr = fopen(file, "w");

    char buf[1024];
    memset(buf, 0, sizeof(buf));

    for (int c = 0; c < parsed_col; ++c) {
        if (strcmp(cols[c].name, col) != 0) {
            sprintf(buf, "%s%s|%s\t", buf, cols[c].name, cols[c].type);
        }
    }
    buf[strlen(buf) - 1] = 0;

    for (int r = 0; r < parsed_row; ++r) {
        for (int c = 0; c < parsed_col; ++c) {
            if (strcmp(cols[c].name, col) != 0) {
                sprintf(buf, "%s%s\t", buf, cols[c].contents[r]);
            }
        }
        buf[strlen(buf) - 1] = '\n';
    }
    buf[strlen(buf) - 1] = 0;

    fprintf(fptr, "%s\n", buf);

    fclose(fptr);
}

void db_create_user(char *user, char *pwd) {
    FILE *fptr;
    fptr = fopen("rootdb/users", "a");

    fprintf(fptr, "%s\t%s\n", user, pwd);

    fclose(fptr);
}

void db_use(char *buff, char *search) {
    if (access(search, F_OK) == 0) {
        DIR *dir;
        dir = opendir(search);

        if (!dir) {
            sprintf(error, "Database %s doesn't exists!", search);
            closedir(dir);
            return;
        }

        closedir(dir);

    } else {
        sprintf(error, "Database %s doesn't exists!", search);
        return;
    }
    int found = 0;
    char *line;
    line = strtok(buff, "\n");
    line = strtok(NULL, "\n");

    while (line != NULL) {
        if (strcmp(search, line) == 0) {
            found = 1;
            break;
        }

        line = strtok(NULL, "\n");
    }
    
    if (found == 1) {
        sprintf(database, "%s", search);
    } else {
        sprintf(error, "You don't have access to %s", search);
    }
}

void db_grant(char *user, char *db) {
    FILE *fptr;
    fptr = fopen("rootdb/db_permission", "a");

    fprintf(fptr, "%s\t%s\n", user, db);

    fclose(fptr);
}

void create_database(char *db) {
    DIR *dir;
    dir = opendir(db);

    if (dir) {
        sprintf(error, "Database exists!");
        closedir(dir);
        return;
    }

    closedir(dir);

    char dir_to_make[1024];
    sprintf(dir_to_make, "mkdir %s", db);
    if (system(dir_to_make) != 0) {
        sprintf(error, "Database exists!");
        return;
    }

    FILE *fptr;
    fptr = fopen("rootdb/db_permission", "a");

    fprintf(fptr, "%s\t%s\n", username, db);

    fclose(fptr);
}


int check_syntax(char *command) {
    logging((const char *)command);
    if (*(command + strlen(command) - 1) != ';') {
        sprintf(error, "Missing ';'");
        return 0;
    }
    int len = strlen(command);

    if (strncmp(command, "CREATE USER", 11) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command+12);

        char *token;
        token = strtok(cmd, ", ;");

        char user[1024];
        sprintf(user, "%s", token);

        sprintf(cmd, "%s", command+12);
        if (strncmp(command + strlen(user) + 13, "IDENTIFIED BY", 13) != 0) {
            sprintf(error, "USAGE: CREATE USER [user] IDENTIFIED BY [password];");
            return 0;
        } else {
            token = strtok(cmd, " ");
            token = strtok(NULL, " ;");
            token = strtok(NULL, " ;,");
            if (token == NULL) {
                sprintf(error, "USAGE: CREATE USER [user] IDENTIFIED BY [password];");
                return 0;
            } else {
                char pwd[1024];
                sprintf(pwd, "%s", command + 27 + strlen(user));
                for (char *itr = pwd; *itr != ';'; ++itr) {
                    if (*itr == ',') {
                        sprintf(error, "USAGE: CREATE USER [user] IDENTIFIED BY [password];");
                        return 0;
                    }
                }
            }
        }
    } 
    else if (strncmp(command, "USE", 3) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command+4);

        char *token;
        token = strtok(cmd, ", ;");

        sprintf(cmd, "%.*s", len - 5 - strlen(token), command + 4 + strlen(token));
        for (char *itr = cmd; *itr != '\0'; ++itr) {
            if (*itr != ' ') {
                sprintf(error, "Usage: USE [database];");
                return 0;
            }
        }
    } 
    else if (strncmp(command, "GRANT PERMISSION", 16) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command+17);

        char *token;
        token = strtok(cmd, ", ;");

        char db[1024];
        sprintf(db, "%s", token);

        sprintf(cmd, "%s", command+17);
        if (strncmp(command + strlen(db) + 18, "INTO", 4) != 0) {
            sprintf(error, "USAGE: GRANT PERMISSION [database] INTO [user];");
            return 0;
        } else {
            token = strtok(cmd, " ");
            token = strtok(NULL, " ;");
            token = strtok(NULL, " ;,");
            if (token == NULL) {
                sprintf(error, "USAGE: GRANT PERMISSION [database] INTO [user];");
                return 0;
            } else {
                char user[1024];
                sprintf(user, "%.*s", len - 24 - strlen(db) - strlen(token), command + 23 + strlen(db));
                for (char *itr = user; *itr != '\0'; ++itr) {
                    if (*itr != ' ') {
                        sprintf(error, "USAGE: GRANT PERMISSION [database] INTO [user];");
                        return 0;
                    }
                }
            }
        }
    } 
    else if (strncmp(command, "CREATE DATABASE", 15) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command+16);

        char *token;
        token = strtok(cmd, ", ;");

        sprintf(cmd, "%.*s", len - 17 - strlen(token), command + 16 + strlen(token));
        for (char *itr = cmd; *itr != '\0'; ++itr) {
            if (*itr != ' ') {
                sprintf(error, "Usage: CREATE DATABASE [database];");
                return 0;
            }
        }
    } 
    else if (strncmp(command, "CREATE TABLE", 12) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command+13);

        char *token;
        token = strtok(cmd, ", ;");

        char table[1024];
        sprintf(table, "%s", token);

        sprintf(cmd, "%s", command + 14 + strlen(token));
        if (cmd[0] != '(') {
            sprintf(error, "Usage: CREATE TABLE [table] ([value1 type1, ...]);");
            return 0;
        }

        char tmp[1024];
        token = strtok(cmd, " (,");
        while (token != NULL) {
            strtok(NULL, " ,(");
            token = strtok(NULL, " (,;");
            sprintf(tmp, "%s", token);
        }

        if (tmp[strlen(tmp) - 1] != ')') {
            sprintf(error, "Usage: CREATE TABLE [table] ([value1 type1, ...]);");
            return 0;
        }
    } 
    else if (strncmp(command, "DROP DATABASE", 13) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command+14);

        char *token;
        token = strtok(cmd, ", ;");

        sprintf(cmd, "%.*s", len - 15 - strlen(token), command + 14 + strlen(token));
        for (char *itr = cmd; *itr != '\0'; ++itr) {
            if (*itr != ' ') {
                sprintf(error, "Usage: DROP DATABASE [database];");
                return 0;
            }
        }
    } 
    else if (strncmp(command, "DROP TABLE", 10) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command+11);

        char *token;
        token = strtok(cmd, ", ;");

        sprintf(cmd, "%.*s", len - 12 - strlen(token), command + 11 + strlen(token));
        for (char *itr = cmd; *itr != '\0'; ++itr) {
            if (*itr != ' ') {
                sprintf(error, "Usage: DROP TABLE [table];");
                return 0;
            }
        }
    } 
    else if (strncmp(command, "DROP COLUMN", 11) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command+12);

        char *token;
        token = strtok(cmd, ", ;");

        char db[1024];
        sprintf(db, "%s", token);

        sprintf(cmd, "%s", command+12);
        if (strncmp(command + strlen(db) + 13, "FROM", 4) != 0) {
            sprintf(error, "USAGE: DROP COLUMN [column] FROM [table];");
            return 0;
        } else {
            token = strtok(cmd, " ");
            token = strtok(NULL, " ;");
            token = strtok(NULL, " ;,");
            if (token == NULL) {
                sprintf(error, "USAGE: DROP COLUMN [column] FROM [table];");
                return 0;
            } else {
                char user[1024];
                sprintf(user, "%.*s", len - 19 - strlen(db) - strlen(token), command + 18 + strlen(db));
                for (char *itr = user; *itr != '\0'; ++itr) {
                    if (*itr != ' ') {
                        sprintf(error, "USAGE: DROP COLUMN [column] FROM [table];");
                        return 0;
                    }
                }
            }
        }

    } 
    else if (strncmp(command, "INSERT INTO", 11) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command+12);

        char *token;
        token = strtok(cmd, " ");

        char table[1024];
        sprintf(table, "%s", token);

        sprintf(cmd, "%s", command + 13 + strlen(table));
        if (cmd[0] != '(') {
            sprintf(error, "Usage: INSERT INTO [table] ([value1], ...);");
            return 0;
        }

        char tmp[1024];
        token = strtok(cmd, " (,");
        while (token != NULL) {
            sprintf(tmp, "%s", token);
            token = strtok(NULL, " (,;");
        }

        if (tmp[strlen(tmp) - 1] != ')') {
            sprintf(error, "Usage: INSERT INTO [table] ([value1], ...);");
            return 0;
        }
    } 
    else if (strncmp(command, "UPDATE", 6) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command+7);

        char *token;
        token = strtok(cmd, ", ;");

        char table[1024];
        sprintf(table, "%s", token);

        sprintf(cmd, "%s", command + 8 + strlen(table));
        if (strncmp(cmd, "SET", 3) != 0) {
            sprintf(error, "Usage: UPDATE [table] SET [col1] = [value1] [WHERE [col2] = [value2]];");
            return 0;
        } else if (strstr(command, "WHERE") != NULL) {
            sprintf(cmd, "%.*s", len - 13 - strlen(table), command + 12 + strlen(table));

            int ce = 0;
            for (char *itr = cmd; *itr != '\0'; ++itr) {
                if (*itr == '=') {
                    ++ce;
                }
            }

            if (ce != 2) {
                sprintf(error, "Usage: UPDATE [table] SET [col1] = [value1] [WHERE [col2] = [value2]];");
                return 0;
            }

            char l[1024];
            char r[1024];
            char l1[1024];
            char r1[1024];

            sprintf(l, "%s", strtok(cmd, " ="));
            sprintf(r, "%s", strtok(NULL, " ="));
            strtok(NULL, " ");
            sprintf(l1, "%s", strtok(NULL, " ="));
            sprintf(r1, "%s", strtok(NULL, " =;"));
            
            for (char *itr = l; *itr != '\0'; ++itr) {
                if (*itr == ',') {
                    sprintf(error, "Usage: UPDATE [table] SET [col1] = [value1] [WHERE [col2] = [value2]];");
                    return 0;
                }
            }
            for (char *itr = r; *itr != '\0'; ++itr) {
                if (*itr == ',') {
                    sprintf(error, "Usage: UPDATE [table] SET [col1] = [value1] [WHERE [col2] = [value2]];");
                    return 0;
                }
            }
            for (char *itr = l1; *itr != '\0'; ++itr) {
                if (*itr == ',') {
                    sprintf(error, "Usage: UPDATE [table] SET [col1] = [value1] [WHERE [col2] = [value2]];");
                    return 0;
                }
            }
            for (char *itr = r1; *itr != '\0'; ++itr) {
                if (*itr == ',') {
                    sprintf(error, "Usage: UPDATE [table] SET [col1] = [value1] [WHERE [col2] = [value2]];");
                    return 0;
                }
            }
        } else {
            sprintf(cmd, "%.*s", len - 13 - strlen(table), command + 12 + strlen(table));
            int ce = 0;
            for (char *itr = cmd; *itr != '\0'; ++itr) {
                if (*itr == '=') {
                    ++ce;
                } else if (!(('a' <= *itr && *itr <= 'z') || ('A' <= *itr && *itr <= 'Z') || *itr != '_' || *itr != ' ')) {
                    sprintf(error, "Usage: UPDATE [table] SET [col1] = [value1] [WHERE [col2] = [value2]];");
                    return 0;
                }
            }
            if (ce != 1) {
                sprintf(error, "Usage: UPDATE [table] SET [col1] = [value1] [WHERE [col2] = [value2]];");
            }
        }
    } 
    else if (strncmp(command, "DELETE FROM", 11) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command+12);

        char *token;
        token = strtok(cmd, ", ;");

        if (strstr(command, "WHERE") != NULL) {
            sprintf(cmd, "%.*s", len - 20 - strlen(token), command + 19 + strlen(token));
            
            int ce = 0;
            for (char *itr = cmd; *itr != '\0'; ++itr) {
                if (*itr == '=') {
                    ++ce;
                }
            }
            if (ce != 1) {
                sprintf(error, "Usage: DELETE FROM [table] [WHERE [col] = [value]];");
                return 0;
            } else {
                token = strtok(cmd, "=");
                for (char *itr = token; *itr != '\0'; ++itr) {
                    if (*itr == ',') {
                        sprintf(error, "Usage: DELETE FROM [table] [WHERE [col] = [value]];");
                        return 0;
                    }
                }
                token = strtok(NULL, "=;");
                for (char *itr = token; *itr != '\0'; ++itr) {
                    if (*itr == ',') {
                        sprintf(error, "Usage: DELETE FROM [table] [WHERE [col] = [value]];");
                        return 0;
                    }
                }
            }
        } else {
            sprintf(cmd, "%.*s", len - 13 - strlen(token), command + 12 + strlen(token));
            for (char *itr = cmd; *itr != '\0'; ++itr) {
                if (*itr != ' ') {
                    sprintf(error, "Usage: DELETE FROM [table] [WHERE [col] = [value]];");
                    return 0;
                }
            }
        }
    } 
    else if (strncmp(command, "SELECT", 6) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command+7);

        char *token;
        token = strtok(cmd, ", ");
        while (strcmp(token, "FROM") != 0 && token != NULL) {
            token = strtok(NULL, ", ");
        }

        if (token == NULL) {
            sprintf(error, "Usage: SELECT [value1, ...] FROM [table] [WHERE [col] = [value]];");
            return 0;
        }

        token = strtok(NULL, " ;");

        if (token == NULL) {
            sprintf(error, "Usage: SELECT [value1, ...] FROM [table] [WHERE [col] = [value]];");
            return 0;
        }

        char table[1024];
        sprintf(table, "%s", token);

        if (strstr(command, "WHERE") != NULL) {
            while (strcmp(token, "WHERE") != 0) {
                token = strtok(NULL, " ,");
            }

            char l[1024];
            char r[1024];

            sprintf(l, "%s", strtok(NULL, "= "));
            sprintf(r, "%s", strtok(NULL, "= ;"));

            for (char *itr = l; *itr != '\0'; ++itr) {
                if (*itr == ',') {
                    sprintf(error, "Usage: SELECT [value1, ...] FROM [table] [WHERE [col] = [value]];");
                    return 0;
                }
            }
            for (char *itr = r; *itr != '\0'; ++itr) {
                if (*itr == ',') {
                    sprintf(error, "Usage: SELECT [value1, ...] FROM [table] [WHERE [col] = [value]];");
                    return 0;
                }
            }

        }
    }
    else {
        sprintf(error, "Command not found!");
        return 0;
    }

    return 1;
}


void cmd_parser(char *command) {
    if (check_syntax(command) == 0) {
        sprintf(error, "%s", error);
        return;
    }
    // Autentikasi
    if (strncmp(command, "CREATE USER", 11) == 0) {
        if (strcmp(username, "root") != 0) {
            sprintf(error, "You don't have access for this command!");
            return;
        }

        char cmd[1024];
        sprintf(cmd, "%s", command+11);

        char *user, *pwd;
        user = strtok(cmd, " ");

        strtok(NULL, " ");
        strtok(NULL, " ");
        pwd = strtok(NULL, " ;\0");

        db_create_user(user, pwd);
    }
    // Autorisasi
    else if (strncmp(command, "USE", 3) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command);

        char *token;
        token = strtok(cmd, " ");
        token = strtok(NULL, " ;\0");

        char *buff;
        buff = (char*)malloc(sizeof(char)*1024);
        sprintf(buff, "%s", db_select("rootdb", "db_permission", 1, (char*[]){"database"}, "username", username));

        db_use(buff, token);
    } 
    else if (strncmp(command, "GRANT PERMISSION", 16) == 0) {
        if (strcmp(username, "root") != 0) {
            sprintf(error, "You don't have access for this command!");
            return;
        }

        char cmd[1024];
        sprintf(cmd, "%s", command + 16);

        char *token;
        token = strtok(cmd, " ");

        char db[1024];
        sprintf(db, "%s", token);

        token = strtok(NULL, " ");
        token = strtok(NULL, " ;\n\0");

        char user[1024];
        sprintf(user, "%s", token);

        db_grant(user, db);
    }
    // Data Definition Language
    else if (strncmp(command, "CREATE DATABASE", 15) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command + 15);

        char *db;
        db = strtok(cmd, " ;\0");

        create_database(db);
    }
    else if (strncmp(command, "CREATE TABLE", 12) == 0) {
        if (strlen(database) == 0) {
            sprintf(error, "You haven't opened any database yet!");
            return;
        }

        char cmd[1024];
        sprintf(cmd, "%s", command);

        int i = 0;

        char table[1024];
        while (cmd[i + 13] != ' ') {
            table[i] = cmd[i + 13];
            ++i;
        }
        table[i] = '\0';
        // printf("Table: %s\n", table);

        int j = 0;
        column columns[10];

        char *ob = strchr(cmd + i + 13, '(');
        char *cb = strchr(cmd + i + 13, ')');

        char cols_and_types[1024];
        sprintf(cols_and_types, "%.*s", cb - ob - 1, ob + 1);

        char *token;
        token = strtok(cols_and_types, ", ");
        while (token != NULL) {
            sprintf(columns[j].name, "%s", token);
            token = strtok(NULL, ", ");

            sprintf(columns[j].type, "%s", token);
            token = strtok(NULL, ", ");

            ++j;
        }

        // for (int k = 0; k < j; ++k) {
        //     printf("Column %d: %s %s\n", k + 1, columns[k].name, columns[k].type);
        // }
        db_create_table(database, table, columns, j);
    } 
    else if (strncmp(command, "DROP DATABASE", 13) == 0) {
        char cmd[1024];
        sprintf(cmd, "%s", command + 13);

        char *db;
        db = strtok(cmd, " ;\0");

        char dbs[1024];
        sprintf(dbs, "%s", db_select("rootdb", "db_permission", 1, (char*[]){"database"}, "username", username));

        char *token;
        token = strtok(dbs, "\n");
        token = strtok(NULL, "\n");
        while (token != NULL) {
            if (strcmp(token, db) == 0) {
                db_drop_database(db);
                return;
            }
            token = strtok(NULL, "\n");
        }

        // printf("You don't have such database!\n");
    } 
    else if (strncmp(command, "DROP TABLE", 10) == 0) {
        if (strlen(database) == 0) {
            sprintf(error, "You haven't opened any database yet!");
            return;
        }

        char cmd[1024];
        sprintf(cmd, "%s", command+11);

        char *token;
        token = strtok(cmd, " ;\0");

        db_drop_table(token);
    } 
    else if (strncmp(command, "DROP COLUMN", 11) == 0) {
        if (strlen(database) == 0) {
            sprintf(error, "You haven't opened any database yet!");
            return;
        }
        char cmd[1024];
        sprintf(cmd, "%s", command+11);

        char *col;
        col = strtok(cmd, " ");

        strtok(NULL, " ");

        char *table;
        table = strtok(NULL, " ;");

        db_drop_column(table, col);
    }
    // Data Manipulation Language
    else if (strncmp(command, "INSERT INTO", 11) == 0) {
        if (strlen(database) == 0) {
            sprintf(error, "You haven't opened any database yet!");
            return;
        }

        char cmd[1024];
        sprintf(cmd, "%s", command);

        char *token;

        token = strtok(cmd, " ");
        token = strtok(NULL, " ");
        token = strtok(NULL, "( ");

        char table[1024];
        sprintf(table, "%s", token);
        count_column(database, table);

        int argc = 0;
        char *argv[10];

        while (token != NULL) {
            token = strtok(NULL, "(),; ");
            if (token == NULL) break;

            argv[argc] = (char*)malloc(sizeof(char)*1024);
            sprintf(argv[argc], "%s", token);
            ++argc;
        }

        if (argc != parsed_col) {
            sprintf(error, "Wrong number of column! Expected %d but received %d", parsed_col, argc);
            return;
        }

        db_insert(table, argc, argv);
    } 
    else if (strncmp(command, "UPDATE", 6) == 0) {
        if (strlen(database) == 0) {
            sprintf(error, "You haven't opened any database yet!");
            return;
        }

        char cmd[1024];
        sprintf(cmd, "%s", command+6);

        char *token;
        token = strtok(cmd, " ");

        char table[1024];
        sprintf(table, "%s", token);

        strtok(NULL, " ");
        token = strtok(NULL, " =");

        char set_left[1024];
        sprintf(set_left, "%s", token);
        token = strtok(NULL, " ;=");
        char set_right[1024];
        sprintf(set_right, "%s", token);

        char left[1024];
        char right[1024];
        memset(left, 0, sizeof(left));
        memset(right, 0, sizeof(right));

        sprintf(cmd, "%s", command+6);
        if (strstr(cmd, "WHERE") != NULL) {
            strtok(NULL, " ");
            sprintf(left, "%s", strtok(NULL, " ="));
            sprintf(right, "%s", strtok(NULL, "=; "));
        }

        db_update(table, set_left, set_right, left, right);
    } 
    else if (strncmp(command, "DELETE FROM", 11) == 0) {
        if (strlen(database) == 0) {
            sprintf(error, "You haven't opened any database yet!");
            return;
        }
        
        char cmd[1024];
        sprintf(cmd, "%s", command+11);

        char *token;
        token = strtok(cmd, "; ");

        char table[1024];
        sprintf(table, "%s", token);

        char left[1024];
        char right[1024];

        memset(left, 0, sizeof(left));
        memset(right, 0, sizeof(right));

        sprintf(cmd, "%s", command+11);
        if (strstr(cmd, "WHERE") != NULL) {
            strtok(NULL, " ");
            sprintf(left, "%s", strtok(NULL, "= "));
            sprintf(right, "%s", strtok(NULL, "=; "));
        }

        db_delete(table, left, right);
    } 
    else if (strncmp(command, "SELECT", 6) == 0) {
        if (strlen(database) == 0) {
            sprintf(error, "You haven't opened any database yet!");
            return;
        }
        char cmd[1024];
        sprintf(cmd, "%s", command);

        int argc = 0;
        char *argv[10];
        for (int i = 0; i < 10; ++i) {
            argv[i] = (char*)malloc(sizeof(char)*1024);
        }
        char *token;

        token = strtok(cmd, " ");
        token = strtok(NULL, ", ");

        while (strcmp(token, "FROM") != 0) {
            sprintf(argv[argc++], "%s", token);
            
            token = strtok(NULL, ", ;");
        }

        token = strtok(NULL, ", ;");
        char table[1024];
        sprintf(table, "%s", token);

        sprintf(cmd, "%s", command);
        
        char left[1024];
        char right[1024];
        memset(left, 0, sizeof(left));
        memset(right, 0, sizeof(right));

        if (strstr(cmd, "WHERE") != NULL) {

            token = strtok(NULL, " =");

            token = strtok(NULL, " ;=");
            sprintf(left, "%s", token);
            // printf("token: %s\n", token);

            token = strtok(NULL, ", ;=");
            sprintf(right, "%s", token);
            // printf("token: %s\n", token);
        }
        
        sprintf(query_result, "%s", db_select(database, table, argc, argv, left, right));
    } 
    else {
        sprintf(error, "Command not found!");
    }
}

void dump(char *db) {
    char db_to_dump[1024];
    sprintf(db_to_dump, "%s/%s", server_path, db);

    if (access(db_to_dump, F_OK) != 0) {
        sprintf(error, "Database %s doesn't exists!", db);
        return;
    }

    char *buff;
    buff = (char*)malloc(sizeof(char)*1024);
    sprintf(buff, "%s", db_select("rootdb", "db_permission", 1, (char*[]){"database"}, "username", username));

    db_use(buff, db);
    if (strlen(database) == 0) {
        sprintf(query_result, "You don't have access to %s!", db);
        return;
    }

    DIR *dir = opendir(db_to_dump);
    struct dirent *dp;

    if (!dir) {
        sprintf(error, "Database %s doesn't exists!", db);
    }

    while ((dp = readdir(dir)) != NULL) {
        if (strcmp(dp->d_name, ".") != 0 && strcmp(dp->d_name, "..") != 0) {
            char file[1024];
            sprintf(file, "%s/%s", db_to_dump, dp->d_name);

            FILE *fptr;
            fptr = fopen(file, "r");

            sprintf(query_result, "%s\nDROP TABLE %s;\nCREATE TABLE %s (", query_result, dp->d_name, dp->d_name);

            char buf[1024];
            fgets(buf, 1024, fptr);
            
            char *token;
            token = strtok(buf, "\t");
            while (token != NULL) {
                char *tab;
                tab = strstr(token, "|");

                sprintf(query_result, "%s%.*s %s, ", query_result, tab - token, token, tab + 1);

                token = strtok(NULL, "\t");
            }
            query_result[strlen(query_result) - 3] = ')';
            query_result[strlen(query_result) - 2] = ';';
            query_result[strlen(query_result) - 1] = '\0';

            sprintf(query_result, "%s\n", query_result);

            while (fgets(buf, 1024, fptr) != NULL) {
                sprintf(query_result, "%sINSERT INTO %s (", query_result, dp->d_name);
                token = strtok(buf, "\t");
                while (token != NULL) {
                    sprintf(query_result, "%s%s, ", query_result, token);

                    token = strtok(NULL, "\t");
                }
                query_result[strlen(query_result) - 3] = ')';
                query_result[strlen(query_result) - 2] = ';';
                query_result[strlen(query_result) - 1] = '\0';

                sprintf(query_result, "%s\n", query_result);
            }
            // sprintf(query_result, "%s\n", query_result);

            fclose(fptr);
        }
    }

    closedir(dir);
}


int main() {
    system("mkdir -p databases/rootdb");
    system("touch databases/rootdb/users");
    system("du databases/rootdb/users | if [ `cut -f1` -eq 0 ]; then echo 'user|string\tpassword|string\nroot\troot' > databases/rootdb/users; fi");
    system("touch databases/rootdb/db_permission");
    system("du databases/rootdb/db_permission | if [ `cut -f1` -eq 0 ]; then echo 'username|string\tdatabase|string\nroot\trootdb' > databases/rootdb/db_permission; fi");

    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    // msg for client
    char *login_success = "login success";
    char *login_fail = "login failed";
    char *wrong_username = "wrong username";
      
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons( PORT );
      
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address))<0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 3) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    pid_t pid, sid;

    pid = fork();
    
    if (pid < 0) {
        exit(EXIT_FAILURE);
    }

    if (pid > 0) {
        exit(EXIT_SUCCESS);
    }

    umask(0);

    sid = setsid();
    if (sid < 0) {
        exit(EXIT_FAILURE);
    }

    if (chdir("/home/kali/Desktop/Sisop/FP/fp-sisop-IT13-2021/database/databases") < 0) {
        exit(EXIT_FAILURE);
    }

    // close(STDIN_FILENO);
    // close(STDOUT_FILENO);
    // close(STDERR_FILENO);

    int logged_in = 0;
    while (1) {
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen))<0) {
            perror("accept");
            exit(EXIT_FAILURE);
        }

        // code here
        read(new_socket, username, 1024);
        read(new_socket, password, 1024);

        char *res = db_select("rootdb", "users", 1, (char*[]){"password"}, "user", username);
        if (res == NULL) {
            send(new_socket, wrong_username, strlen(wrong_username), 0);
        } else {
            char *token;
            token = strtok(res, "\n");
            token = strtok(NULL, "\n");
            if (strcmp(token, password) == 0) {
                send(new_socket, login_success, strlen(login_success), 0);
                logged_in = 1;
            } else {
                send(new_socket, login_fail, strlen(login_fail), 0);
            }
        }

        memset(buffer, 0, sizeof(buffer));

        if (logged_in == 1) {
            while (1) {
                read(new_socket, buffer, 1024);

                if (strcmp(buffer, "exit") == 0) {
                    break;
                }

                if (strncmp(buffer, "dump-database", 13) == 0) {
                    char cmd[1024];
                    sprintf(cmd, "%s", buffer);
                    strtok(buffer, " ");
                    char *db = strtok(NULL, " ");
                    if (strcmp(db, "*") == 0) {
                        DIR *dir = opendir(server_path);
                        struct dirent *dp;

                        while ((dp = readdir(dir)) != NULL) {
                            if (strcmp(dp->d_name, ".") != 0 && strcmp(dp->d_name, "..") != 0) {
                                DIR *try = opendir(dp->d_name);
                                if (try) {
                                    dump(dp->d_name);

                                    char file[1024];
                                    sprintf(file, "%s/%s.backup", server_path, dp->d_name);

                                    FILE *fptr;
                                    fptr = fopen(file, "w");

                                    fprintf(fptr, "%s", query_result);
                                    memset(query_result, 0, sizeof(query_result));

                                    fclose(fptr);

                                    closedir(try);
                                }
                            }
                        }

                        closedir(dir);
                    } else {
                        dump(db);
                    }
                    send(new_socket, query_result, strlen(query_result), 0);
                    break;
                }

                cmd_parser(buffer);
                if (strlen(query_result)) {
                    send(new_socket, query_result, strlen(query_result), 0);
                    memset(query_result, 0, sizeof(query_result));
                } else if (strlen(error)) {
                    send(new_socket, error, strlen(error), 0);
                    memset(error, 0, sizeof(error));
                } else {
                    char *success = "query success";
                    send(new_socket, success, strlen(success), 0);
                }

                memset(buffer, 0, sizeof(buffer));
            }
        }

        close(new_socket);
        memset(database, 0, sizeof(database));
        memset(username, 0, sizeof(username));
        memset(password, 0, sizeof(password));
        memset(query_result, 0, sizeof(query_result));
        memset(error, 0, sizeof(error));
    }

    return 0;
}