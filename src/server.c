#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "protocol.h"

static volatile sig_atomic_t keep_running = 1;

static void handle_signal(int signal_number) {
    (void)signal_number;
    keep_running = 0;
}

static void print_usage(const char *program_name) {
    fprintf(stderr, "Usage: %s --port PORT [--bind ADDRESS]\n", program_name);
}

static int parse_port(const char *value) {
    char *end = NULL;
    long parsed = strtol(value, &end, 10);

    if (value[0] == '\0' || end == value || *end != '\0' || parsed < 1 || parsed > 65535) {
        return -1;
    }

    return (int)parsed;
}

static bool parse_args(int argc, char **argv, int *port, const char **bind_address) {
    *port = -1;
    *bind_address = "0.0.0.0";

    for (int index = 1; index < argc; ++index) {
        if (strcmp(argv[index], "--port") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --port\n");
                return false;
            }

            *port = parse_port(argv[++index]);
            if (*port < 0) {
                fprintf(stderr, "Invalid port: %s\n", argv[index]);
                return false;
            }
        } else if (strcmp(argv[index], "--bind") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --bind\n");
                return false;
            }

            *bind_address = argv[++index];
        } else if (strcmp(argv[index], "--help") == 0) {
            print_usage(argv[0]);
            exit(0);
        } else {
            fprintf(stderr, "Unknown argument: %s\n", argv[index]);
            return false;
        }
    }

    if (*port < 0) {
        fprintf(stderr, "--port is required\n");
        return false;
    }

    return true;
}

static bool read_load_average(double *load_1, double *load_5, double *load_15) {
    FILE *file = fopen("/proc/loadavg", "r");
    if (file == NULL) {
        return false;
    }

    int parsed = fscanf(file, "%lf %lf %lf", load_1, load_5, load_15);
    fclose(file);
    return parsed == LOAD_RESPONSE_FIELDS;
}

static bool send_response(int client_fd) {
    double load_1 = 0.0;
    double load_5 = 0.0;
    double load_15 = 0.0;
    char response[LOAD_RESPONSE_SIZE];

    if (!read_load_average(&load_1, &load_5, &load_15)) {
        fprintf(stderr, "Failed to read /proc/loadavg\n");
        return false;
    }

    int response_length = snprintf(response, sizeof(response), "%.2f %.2f %.2f\n", load_1, load_5, load_15);
    if (response_length < 0 || (size_t)response_length >= sizeof(response)) {
        fprintf(stderr, "Failed to format response\n");
        return false;
    }

    size_t total_sent = 0;
    while (total_sent < (size_t)response_length) {
        ssize_t written = send(client_fd, response + total_sent, (size_t)response_length - total_sent, 0);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }

            perror("send");
            return false;
        }

        total_sent += (size_t)written;
    }

    return true;
}

static int create_listener(const char *bind_address, int port) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return -1;
    }

    int reuse_addr = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse_addr, sizeof(reuse_addr)) < 0) {
        perror("setsockopt");
        close(server_fd);
        return -1;
    }

    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons((uint16_t)port);

    if (inet_pton(AF_INET, bind_address, &address.sin_addr) != 1) {
        fprintf(stderr, "Invalid bind address: %s\n", bind_address);
        close(server_fd);
        return -1;
    }

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind");
        close(server_fd);
        return -1;
    }

    if (listen(server_fd, SOMAXCONN) < 0) {
        perror("listen");
        close(server_fd);
        return -1;
    }

    return server_fd;
}

int main(int argc, char **argv) {
    int port = -1;
    const char *bind_address = NULL;

    if (!parse_args(argc, argv, &port, &bind_address)) {
        print_usage(argv[0]);
        return 1;
    }

    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = handle_signal;
    sigemptyset(&action.sa_mask);

    if (sigaction(SIGINT, &action, NULL) < 0 || sigaction(SIGTERM, &action, NULL) < 0) {
        perror("sigaction");
        return 1;
    }

    int server_fd = create_listener(bind_address, port);
    if (server_fd < 0) {
        return 1;
    }

    fprintf(stderr, "load_server listening on %s:%d\n", bind_address, port);

    while (keep_running) {
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR && keep_running) {
                continue;
            }

            if (!keep_running) {
                break;
            }

            perror("accept");
            close(server_fd);
            return 1;
        }

        (void)send_response(client_fd);
        close(client_fd);
    }

    close(server_fd);
    return 0;
}