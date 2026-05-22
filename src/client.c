#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "protocol.h"

struct client_config {
    const char *hosts_path;
    int expected_host_count;
    int port;
    int timeout_ms;
};

struct host_list {
    char **items;
    size_t count;
};

static void print_usage(const char *program_name) {
    fprintf(stderr, "Usage: %s --port PORT [--hosts FILE] [--timeout-ms MS] [--expect-host-count N]\n", program_name);
}

static int parse_positive_int(const char *value) {
    char *end = NULL;
    long parsed = strtol(value, &end, 10);

    if (value[0] == '\0' || end == value || *end != '\0' || parsed <= 0 || parsed > 65535) {
        return -1;
    }

    return (int)parsed;
}

static bool parse_args(int argc, char **argv, struct client_config *config) {
    config->hosts_path = DEFAULT_HOSTS_FILE;
    config->expected_host_count = DEFAULT_HOST_COUNT;
    config->port = -1;
    config->timeout_ms = DEFAULT_CONNECT_TIMEOUT_MS;

    for (int index = 1; index < argc; ++index) {
        if (strcmp(argv[index], "--hosts") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --hosts\n");
                return false;
            }

            config->hosts_path = argv[++index];
        } else if (strcmp(argv[index], "--port") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --port\n");
                return false;
            }

            config->port = parse_positive_int(argv[++index]);
            if (config->port < 0) {
                fprintf(stderr, "Invalid port: %s\n", argv[index]);
                return false;
            }
        } else if (strcmp(argv[index], "--timeout-ms") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --timeout-ms\n");
                return false;
            }

            config->timeout_ms = parse_positive_int(argv[++index]);
            if (config->timeout_ms < 0) {
                fprintf(stderr, "Invalid timeout: %s\n", argv[index]);
                return false;
            }
        } else if (strcmp(argv[index], "--expect-host-count") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --expect-host-count\n");
                return false;
            }

            config->expected_host_count = parse_positive_int(argv[++index]);
            if (config->expected_host_count < 0) {
                fprintf(stderr, "Invalid expected host count: %s\n", argv[index]);
                return false;
            }
        } else if (strcmp(argv[index], "--help") == 0) {
            print_usage(argv[0]);
            exit(0);
        } else {
            fprintf(stderr, "Unknown argument: %s\n", argv[index]);
            return false;
        }
    }

    if (config->port < 0) {
        fprintf(stderr, "--port is required\n");
        return false;
    }

    return true;
}

static char *trim_line(char *line) {
    while (*line == ' ' || *line == '\t' || *line == '\r' || *line == '\n') {
        ++line;
    }

    size_t length = strlen(line);
    while (length > 0) {
        char current = line[length - 1];
        if (current != ' ' && current != '\t' && current != '\r' && current != '\n') {
            break;
        }
        line[--length] = '\0';
    }

    return line;
}

static void free_host_list(struct host_list *hosts) {
    for (size_t index = 0; index < hosts->count; ++index) {
        free(hosts->items[index]);
    }
    free(hosts->items);
    hosts->items = NULL;
    hosts->count = 0;
}

static bool append_host(struct host_list *hosts, const char *value) {
    char **next_items = realloc(hosts->items, sizeof(*hosts->items) * (hosts->count + 1));
    if (next_items == NULL) {
        return false;
    }

    hosts->items = next_items;
    hosts->items[hosts->count] = strdup(value);
    if (hosts->items[hosts->count] == NULL) {
        return false;
    }

    ++hosts->count;
    return true;
}

static bool read_hosts_file(const char *path, struct host_list *hosts) {
    FILE *file = fopen(path, "r");
    if (file == NULL) {
        perror("fopen hosts");
        return false;
    }

    char *line = NULL;
    size_t capacity = 0;
    ssize_t line_length = 0;
    bool ok = true;

    while ((line_length = getline(&line, &capacity, file)) >= 0) {
        (void)line_length;
        char *trimmed = trim_line(line);
        if (trimmed[0] == '\0' || trimmed[0] == '#') {
            continue;
        }

        if (!append_host(hosts, trimmed)) {
            fprintf(stderr, "Failed to append host entry\n");
            ok = false;
            break;
        }
    }

    free(line);
    fclose(file);

    if (!ok) {
        free_host_list(hosts);
    }

    if (ok && hosts->count == 0) {
        fprintf(stderr, "No hosts found in %s\n", path);
        return false;
    }

    return ok;
}

static bool set_socket_blocking(int fd, bool should_block) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }

    if (should_block) {
        flags &= ~O_NONBLOCK;
    } else {
        flags |= O_NONBLOCK;
    }

    return fcntl(fd, F_SETFL, flags) == 0;
}

static bool configure_socket_timeouts(int fd, int timeout_ms) {
    struct timeval timeout;
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;

    return setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0 &&
           setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0;
}

static int connect_to_host(const char *host, int port, int timeout_ms) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char port_buffer[16];
    snprintf(port_buffer, sizeof(port_buffer), "%d", port);

    struct addrinfo *results = NULL;
    int gai_error = getaddrinfo(host, port_buffer, &hints, &results);
    if (gai_error != 0) {
        fprintf(stderr, "%s: getaddrinfo failed: %s\n", host, gai_strerror(gai_error));
        return -1;
    }

    int connected_fd = -1;

    for (struct addrinfo *current = results; current != NULL; current = current->ai_next) {
        int socket_fd = socket(current->ai_family, current->ai_socktype, current->ai_protocol);
        if (socket_fd < 0) {
            continue;
        }

        if (!set_socket_blocking(socket_fd, false)) {
            close(socket_fd);
            continue;
        }

        if (connect(socket_fd, current->ai_addr, current->ai_addrlen) == 0) {
            connected_fd = socket_fd;
        } else if (errno == EINPROGRESS) {
            struct pollfd poll_fd;
            memset(&poll_fd, 0, sizeof(poll_fd));
            poll_fd.fd = socket_fd;
            poll_fd.events = POLLOUT;

            int poll_result = poll(&poll_fd, 1, timeout_ms);
            if (poll_result > 0) {
                int socket_error = 0;
                socklen_t socket_error_size = sizeof(socket_error);
                if (getsockopt(socket_fd, SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_size) == 0 && socket_error == 0) {
                    connected_fd = socket_fd;
                }
            }
        }

        if (connected_fd >= 0) {
            if (!set_socket_blocking(connected_fd, true) || !configure_socket_timeouts(connected_fd, timeout_ms)) {
                close(connected_fd);
                connected_fd = -1;
                continue;
            }
            break;
        }

        close(socket_fd);
    }

    freeaddrinfo(results);

    if (connected_fd < 0) {
        fprintf(stderr, "%s: connection failed\n", host);
    }

    return connected_fd;
}

static bool read_response_line(int fd, double *load_1, double *load_5, double *load_15) {
    char buffer[LOAD_RESPONSE_SIZE];
    size_t used = 0;

    while (used + 1 < sizeof(buffer)) {
        ssize_t received = recv(fd, buffer + used, sizeof(buffer) - used - 1, 0);
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }

        if (received == 0) {
            break;
        }

        used += (size_t)received;
        if (memchr(buffer, '\n', used) != NULL) {
            break;
        }
    }

    buffer[used] = '\0';
    return sscanf(buffer, "%lf %lf %lf", load_1, load_5, load_15) == LOAD_RESPONSE_FIELDS;
}

static bool query_host(const char *host, int port, int timeout_ms, double *load_1, double *load_5, double *load_15) {
    int fd = connect_to_host(host, port, timeout_ms);
    if (fd < 0) {
        return false;
    }

    bool ok = read_response_line(fd, load_1, load_5, load_15);
    if (!ok) {
        fprintf(stderr, "%s: invalid server response\n", host);
    }

    close(fd);
    return ok;
}

int main(int argc, char **argv) {
    struct client_config config;
    if (!parse_args(argc, argv, &config)) {
        print_usage(argv[0]);
        return 1;
    }

    struct host_list hosts;
    memset(&hosts, 0, sizeof(hosts));
    if (!read_hosts_file(config.hosts_path, &hosts)) {
        return 1;
    }

    if ((int)hosts.count != config.expected_host_count) {
        fprintf(stderr, "Warning: expected %d hosts, found %zu\n", config.expected_host_count, hosts.count);
    }

    double sum_1 = 0.0;
    double sum_5 = 0.0;
    double sum_15 = 0.0;
    size_t success_count = 0;

    for (size_t index = 0; index < hosts.count; ++index) {
        double load_1 = 0.0;
        double load_5 = 0.0;
        double load_15 = 0.0;

        if (!query_host(hosts.items[index], config.port, config.timeout_ms, &load_1, &load_5, &load_15)) {
            continue;
        }

        printf("%s: 1m=%.2f 5m=%.2f 15m=%.2f\n",
               hosts.items[index],
               load_1,
               load_5,
               load_15);

        sum_1 += load_1;
        sum_5 += load_5;
        sum_15 += load_15;
        ++success_count;
    }

    if (success_count == 0) {
        fprintf(stderr, "No hosts returned a valid response\n");
        free_host_list(&hosts);
        return 1;
    }

        size_t total_hosts = hosts.count;

        printf("Average load across %zu/%zu hosts: 1m=%.2f 5m=%.2f 15m=%.2f\n",
           success_count,
            total_hosts,
           sum_1 / success_count,
           sum_5 / success_count,
           sum_15 / success_count);

    free_host_list(&hosts);
        return success_count == total_hosts ? 0 : 1;
}