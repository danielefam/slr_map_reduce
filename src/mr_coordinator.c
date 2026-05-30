#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "protocol.h"

struct coordinator_config {
    const char *hosts_path;
    const char *input_path;
    const char *output_path;
    const char *job_manifest_path;
    const char *job_id;
    int chunk_lines;
    int port;
    int timeout_ms;
    int reducer_limit;
};

struct host_list {
    char **items;
    size_t count;
};

struct string_list {
    char **items;
    size_t count;
    size_t capacity;
};

enum remote_status {
    REMOTE_STATUS_OK = 0,
    REMOTE_STATUS_CONNECT_FAILED,
    REMOTE_STATUS_IO_FAILED,
    REMOTE_STATUS_BAD_RESPONSE,
};

static void print_usage(const char *program_name) {
    fprintf(stderr,
            "Usage: %s --input FILE --port PORT [--hosts FILE] [--output FILE] [--chunk-lines N] [--reducers N] [--timeout-ms MS] [--job-id ID] [--job-manifest FILE]\n",
            program_name);
}

static int parse_positive_int(const char *value) {
    char *end = NULL;
    long parsed = strtol(value, &end, 10);
    if (value[0] == '\0' || end == value || *end != '\0' || parsed <= 0 || parsed > 65535) {
        return -1;
    }
    return (int)parsed;
}

static size_t min_size(size_t left, size_t right) {
    return left < right ? left : right;
}

static bool parse_args(int argc, char **argv, struct coordinator_config *config) {
    config->hosts_path = DEFAULT_HOSTS_FILE;
    config->input_path = NULL;
    config->output_path = DEFAULT_MR_RESULTS_FILE;
    config->job_manifest_path = DEFAULT_MR_JOB_FILE;
    config->job_id = NULL;
    config->chunk_lines = DEFAULT_MR_CHUNK_LINES;
    config->port = -1;
    config->timeout_ms = DEFAULT_IO_TIMEOUT_MS;
    config->reducer_limit = 0;

    for (int index = 1; index < argc; ++index) {
        if (strcmp(argv[index], "--hosts") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --hosts\n");
                return false;
            }
            config->hosts_path = argv[++index];
        } else if (strcmp(argv[index], "--input") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --input\n");
                return false;
            }
            config->input_path = argv[++index];
        } else if (strcmp(argv[index], "--output") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --output\n");
                return false;
            }
            config->output_path = argv[++index];
        } else if (strcmp(argv[index], "--chunk-lines") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --chunk-lines\n");
                return false;
            }
            config->chunk_lines = parse_positive_int(argv[++index]);
        } else if (strcmp(argv[index], "--port") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --port\n");
                return false;
            }
            config->port = parse_positive_int(argv[++index]);
        } else if (strcmp(argv[index], "--timeout-ms") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --timeout-ms\n");
                return false;
            }
            config->timeout_ms = parse_positive_int(argv[++index]);
        } else if (strcmp(argv[index], "--reducers") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --reducers\n");
                return false;
            }
            config->reducer_limit = parse_positive_int(argv[++index]);
        } else if (strcmp(argv[index], "--job-id") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --job-id\n");
                return false;
            }
            config->job_id = argv[++index];
        } else if (strcmp(argv[index], "--job-manifest") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --job-manifest\n");
                return false;
            }
            config->job_manifest_path = argv[++index];
        } else if (strcmp(argv[index], "--help") == 0) {
            print_usage(argv[0]);
            exit(0);
        } else {
            fprintf(stderr, "Unknown argument: %s\n", argv[index]);
            return false;
        }
    }

    if (config->input_path == NULL || config->port < 0 || config->chunk_lines < 0 || config->timeout_ms < 0) {
        fprintf(stderr, "--input and --port are required\n");
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
            ok = false;
            break;
        }
    }

    free(line);
    fclose(file);

    if (!ok) {
        free_host_list(hosts);
    }
    return ok && hosts->count > 0;
}

static bool string_list_append(struct string_list *list, const char *value) {
    if (list->count == list->capacity) {
        size_t next_capacity = list->capacity == 0 ? 16 : (list->capacity * 2);
        char **next_items = realloc(list->items, next_capacity * sizeof(*next_items));
        if (next_items == NULL) {
            return false;
        }
        list->items = next_items;
        list->capacity = next_capacity;
    }

    list->items[list->count] = strdup(value);
    if (list->items[list->count] == NULL) {
        return false;
    }
    ++list->count;
    return true;
}

static void free_string_list(struct string_list *list) {
    for (size_t index = 0; index < list->count; ++index) {
        free(list->items[index]);
    }
    free(list->items);
    list->items = NULL;
    list->count = 0;
    list->capacity = 0;
}

static bool read_input_chunks(const char *path, int chunk_lines, struct string_list *chunks) {
    FILE *file = fopen(path, "r");
    if (file == NULL) {
        perror("fopen input");
        return false;
    }

    char *line = NULL;
    size_t line_capacity = 0;
    ssize_t line_length = 0;
    char *chunk = NULL;
    size_t chunk_size = 0;
    size_t chunk_capacity = 0;
    int lines_in_chunk = 0;
    bool ok = true;

    while ((line_length = getline(&line, &line_capacity, file)) >= 0) {
        size_t needed = chunk_size + (size_t)line_length + 1;
        if (needed > chunk_capacity) {
            size_t next_capacity = chunk_capacity == 0 ? needed * 2 : chunk_capacity;
            while (next_capacity < needed) {
                next_capacity *= 2;
            }
            char *next_chunk = realloc(chunk, next_capacity);
            if (next_chunk == NULL) {
                ok = false;
                break;
            }
            chunk = next_chunk;
            chunk_capacity = next_capacity;
        }

        memcpy(chunk + chunk_size, line, (size_t)line_length);
        chunk_size += (size_t)line_length;
        chunk[chunk_size] = '\0';
        ++lines_in_chunk;

        if (lines_in_chunk >= chunk_lines) {
            if (!string_list_append(chunks, chunk)) {
                ok = false;
                break;
            }
            chunk_size = 0;
            lines_in_chunk = 0;
        }
    }

    if (ok && chunk_size > 0) {
        if (!string_list_append(chunks, chunk)) {
            ok = false;
        }
    }

    free(chunk);
    free(line);
    fclose(file);

    if (!ok) {
        free_string_list(chunks);
    }

    if (ok && chunks->count == 0) {
        if (!string_list_append(chunks, "")) {
            ok = false;
        }
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
    return connected_fd;
}

static bool write_all(int fd, const void *buffer, size_t length) {
    const unsigned char *cursor = buffer;
    size_t sent = 0;
    while (sent < length) {
        ssize_t written = send(fd, cursor + sent, length - sent, 0);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        sent += (size_t)written;
    }
    return true;
}

static bool read_exact(int fd, void *buffer, size_t length) {
    unsigned char *cursor = buffer;
    size_t used = 0;
    while (used < length) {
        ssize_t received = recv(fd, cursor + used, length - used, 0);
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (received == 0) {
            return false;
        }
        used += (size_t)received;
    }
    return true;
}

static bool read_line(int fd, char *buffer, size_t size) {
    size_t used = 0;
    while (used + 1 < size) {
        char current = '\0';
        ssize_t received = recv(fd, &current, 1, 0);
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (received == 0) {
            break;
        }
        if (current == '\n') {
            break;
        }
        buffer[used++] = current;
    }
    buffer[used] = '\0';
    return used > 0;
}

static enum remote_status send_command_expect_ok(const char *host,
                                                 int port,
                                                 int timeout_ms,
                                                 const char *command,
                                                 const void *payload,
                                                 size_t payload_size,
                                                 char *response,
                                                 size_t response_size) {
    int fd = connect_to_host(host, port, timeout_ms);
    if (fd < 0) {
        return REMOTE_STATUS_CONNECT_FAILED;
    }

    if (!write_all(fd, command, strlen(command)) ||
        (payload_size > 0 && !write_all(fd, payload, payload_size)) ||
        !read_line(fd, response, response_size)) {
        close(fd);
        return REMOTE_STATUS_IO_FAILED;
    }

    if (strncmp(response, "OK ", 3) != 0) {
        close(fd);
        return REMOTE_STATUS_BAD_RESPONSE;
    }

    close(fd);
    return REMOTE_STATUS_OK;
}

static enum remote_status fetch_data(const char *host,
                                     int port,
                                     int timeout_ms,
                                     const char *command,
                                     char **content,
                                     size_t *length) {
    *content = NULL;
    *length = 0;

    int fd = connect_to_host(host, port, timeout_ms);
    if (fd < 0) {
        return REMOTE_STATUS_CONNECT_FAILED;
    }

    char header[MR_CONTROL_LINE_SIZE];
    if (!write_all(fd, command, strlen(command)) || !read_line(fd, header, sizeof(header))) {
        close(fd);
        return REMOTE_STATUS_IO_FAILED;
    }

    size_t expected = 0;
    if (sscanf(header, "DATA %zu", &expected) != 1) {
        close(fd);
        return REMOTE_STATUS_BAD_RESPONSE;
    }

    char *buffer = malloc(expected + 1);
    if (buffer == NULL) {
        close(fd);
        return REMOTE_STATUS_IO_FAILED;
    }

    if (expected > 0 && !read_exact(fd, buffer, expected)) {
        free(buffer);
        close(fd);
        return REMOTE_STATUS_IO_FAILED;
    }

    buffer[expected] = '\0';
    close(fd);
    *content = buffer;
    *length = expected;
    return REMOTE_STATUS_OK;
}

static int remote_status_to_exit_code(enum remote_status status) {
    switch (status) {
        case REMOTE_STATUS_CONNECT_FAILED:
        case REMOTE_STATUS_IO_FAILED:
            return MR_EXIT_NETWORK;
        case REMOTE_STATUS_BAD_RESPONSE:
            return MR_EXIT_PROTOCOL;
        case REMOTE_STATUS_OK:
            return MR_EXIT_SUCCESS;
        default:
            return MR_EXIT_INTERNAL;
    }
}

static bool write_shell_value(FILE *file, const char *key, const char *value) {
    if (fprintf(file, "%s=\"", key) < 0) {
        return false;
    }

    for (const char *cursor = value; *cursor != '\0'; ++cursor) {
        if (*cursor == '\\' || *cursor == '"' || *cursor == '$' || *cursor == '`') {
            if (fputc('\\', file) == EOF) {
                return false;
            }
        }
        if (fputc(*cursor, file) == EOF) {
            return false;
        }
    }

    return fputs("\"\n", file) != EOF;
}

static bool write_job_manifest(const struct coordinator_config *config,
                               const char *job_id,
                               size_t map_task_count,
                               size_t worker_pool_count,
                               size_t effective_mapper_count,
                               size_t effective_reducer_count) {
    FILE *file = fopen(config->job_manifest_path, "w");
    if (file == NULL) {
        return false;
    }

    bool ok = write_shell_value(file, "JOB_ID", job_id) &&
              write_shell_value(file, "INPUT_PATH", config->input_path) &&
              write_shell_value(file, "OUTPUT_PATH", config->output_path) &&
              fprintf(file,
                      "MAP_TASK_COUNT=%zu\nWORKER_POOL_COUNT=%zu\nEFFECTIVE_MAPPER_COUNT=%zu\nEFFECTIVE_REDUCER_COUNT=%zu\nPORT=%d\n",
                      map_task_count,
                      worker_pool_count,
                      effective_mapper_count,
                      effective_reducer_count,
                      config->port) >= 0;
    fclose(file);
    return ok;
}

static char *generate_job_id(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        return NULL;
    }
    char buffer[64];
    int written = snprintf(buffer, sizeof(buffer), "job-%lld-%ld",
                           (long long)ts.tv_sec,
                           ts.tv_nsec / 1000000L);
    if (written < 0 || (size_t)written >= sizeof(buffer)) {
        return NULL;
    }
    return strdup(buffer);
}

static int compare_lines(const void *left, const void *right) {
    const char *const *lhs = left;
    const char *const *rhs = right;
    return strcmp(*lhs, *rhs);
}

static bool write_sorted_output(const char *path, struct string_list *lines) {
    qsort(lines->items, lines->count, sizeof(*lines->items), compare_lines);

    FILE *file = fopen(path, "w");
    if (file == NULL) {
        perror("fopen output");
        return false;
    }

    for (size_t index = 0; index < lines->count; ++index) {
        if (fprintf(file, "%s\n", lines->items[index]) < 0) {
            fclose(file);
            return false;
        }
    }

    fclose(file);
    return true;
}

static bool collect_result_lines(struct string_list *lines, const char *content) {
    const char *cursor = content;
    while (*cursor != '\0') {
        const char *line_end = strchr(cursor, '\n');
        size_t line_length = line_end == NULL ? strlen(cursor) : (size_t)(line_end - cursor);
        if (line_length > 0) {
            char *line = malloc(line_length + 1);
            if (line == NULL) {
                return false;
            }
            memcpy(line, cursor, line_length);
            line[line_length] = '\0';
            if (!string_list_append(lines, line)) {
                free(line);
                return false;
            }
            free(line);
        }
        if (line_end == NULL) {
            break;
        }
        cursor = line_end + 1;
    }
    return true;
}

int main(int argc, char **argv) {
    struct coordinator_config config;
    if (!parse_args(argc, argv, &config)) {
        print_usage(argv[0]);
        return MR_EXIT_USAGE;
    }

    struct host_list hosts = {0};
    if (!read_hosts_file(config.hosts_path, &hosts)) {
        fprintf(stderr, "Failed to read hosts from %s\n", config.hosts_path);
        return MR_EXIT_HOSTS;
    }

    struct string_list chunks = {0};
    if (!read_input_chunks(config.input_path, config.chunk_lines, &chunks)) {
        free_host_list(&hosts);
        return MR_EXIT_INPUT;
    }

    char *generated_job_id = NULL;
    const char *job_id = config.job_id;
    int exit_code = MR_EXIT_INTERNAL;
    char *peers = NULL;
    struct string_list output_lines = {0};

    if (job_id == NULL) {
        generated_job_id = generate_job_id();
        if (generated_job_id == NULL) {
            exit_code = MR_EXIT_SETUP;
            goto cleanup;
        }
        job_id = generated_job_id;
    }

    size_t effective_mapper_count = min_size(chunks.count, hosts.count);
    size_t effective_reducer_count = effective_mapper_count;
    if (config.reducer_limit > 0) {
        effective_reducer_count = min_size(effective_reducer_count, (size_t)config.reducer_limit);
    }
    if (effective_reducer_count == 0) {
        effective_reducer_count = 1;
    }

    if (!write_job_manifest(&config, job_id, chunks.count, hosts.count, effective_mapper_count, effective_reducer_count)) {
        fprintf(stderr, "Warning: failed to write job manifest %s\n", config.job_manifest_path);
    }

    fprintf(stderr,
            "Submitting %zu map task(s) to %zu effective mapper(s), %zu effective reducer(s), worker pool=%zu for job %s\n",
            chunks.count,
            effective_mapper_count,
            effective_reducer_count,
            hosts.count,
            job_id);

    for (size_t index = 0; index < chunks.count; ++index) {
        const char *host = hosts.items[index % effective_mapper_count];
        const char *payload = chunks.items[index];
        size_t payload_size = strlen(payload);

        char command[MR_CONTROL_LINE_SIZE];
        int written = snprintf(command, sizeof(command), "MAP %s %zu %zu %zu\n", job_id, index, effective_reducer_count, payload_size);
        if (written < 0 || (size_t)written >= sizeof(command)) {
            fprintf(stderr, "Map command too long for task %zu\n", index);
            exit_code = MR_EXIT_PROTOCOL;
            goto cleanup;
        }

        char response[MR_CONTROL_LINE_SIZE];
        enum remote_status status = send_command_expect_ok(host,
                                                            config.port,
                                                            config.timeout_ms,
                                                            command,
                                                            payload,
                                                            payload_size,
                                                            response,
                                                            sizeof(response));
        if (status != REMOTE_STATUS_OK) {
            fprintf(stderr, "Map task %zu failed on %s (%s)\n",
                    index,
                    host,
                    status == REMOTE_STATUS_BAD_RESPONSE ? "protocol" : "network");
            exit_code = remote_status_to_exit_code(status);
            goto cleanup;
        }
    }

    fprintf(stderr, "All map tasks finished for job %s\n", job_id);

    size_t peers_size = 0;
    for (size_t index = 0; index < effective_mapper_count; ++index) {
        peers_size += strlen(hosts.items[index]) + 32;
    }

    peers = malloc(peers_size + 1);
    if (peers == NULL) {
        exit_code = MR_EXIT_SETUP;
        goto cleanup;
    }

    size_t peers_used = 0;
    for (size_t index = 0; index < effective_mapper_count; ++index) {
        int written = snprintf(peers + peers_used, peers_size + 1 - peers_used, "%s %d %zu\n", hosts.items[index], config.port, index);
        if (written < 0 || (size_t)written >= peers_size + 1 - peers_used) {
            exit_code = MR_EXIT_PROTOCOL;
            goto cleanup;
        }
        peers_used += (size_t)written;
    }

    for (size_t reducer_id = 0; reducer_id < effective_reducer_count; ++reducer_id) {
        const char *host = hosts.items[reducer_id];
        char command[MR_CONTROL_LINE_SIZE];
        int written = snprintf(command, sizeof(command), "REDUCE %s %zu %zu\n", job_id, reducer_id, peers_used);
        if (written < 0 || (size_t)written >= sizeof(command)) {
            fprintf(stderr, "Reduce command too long for reducer %zu\n", reducer_id);
            exit_code = MR_EXIT_PROTOCOL;
            goto cleanup;
        }

        char response[MR_CONTROL_LINE_SIZE];
        enum remote_status status = send_command_expect_ok(host,
                                                            config.port,
                                                            config.timeout_ms,
                                                            command,
                                                            peers,
                                                            peers_used,
                                                            response,
                                                            sizeof(response));
        if (status != REMOTE_STATUS_OK) {
            fprintf(stderr, "Reduce task %zu failed on %s (%s)\n",
                    reducer_id,
                    host,
                    status == REMOTE_STATUS_BAD_RESPONSE ? "protocol" : "network");
            exit_code = remote_status_to_exit_code(status);
            goto cleanup;
        }
    }

    fprintf(stderr, "All reduce tasks finished for job %s\n", job_id);

    for (size_t reducer_id = 0; reducer_id < effective_reducer_count; ++reducer_id) {
        const char *host = hosts.items[reducer_id];
        char command[MR_CONTROL_LINE_SIZE];
        int written = snprintf(command, sizeof(command), "RESULT %s %zu\n", job_id, reducer_id);
        if (written < 0 || (size_t)written >= sizeof(command)) {
            fprintf(stderr, "Result command too long for reducer %zu\n", reducer_id);
            exit_code = MR_EXIT_PROTOCOL;
            goto cleanup;
        }

        char *content = NULL;
        size_t length = 0;
        enum remote_status status = fetch_data(host, config.port, config.timeout_ms, command, &content, &length);
        if (status != REMOTE_STATUS_OK) {
            fprintf(stderr, "Failed to fetch reducer %zu result from %s (%s)\n",
                    reducer_id,
                    host,
                    status == REMOTE_STATUS_BAD_RESPONSE ? "protocol" : "network");
            exit_code = remote_status_to_exit_code(status);
            goto cleanup;
        }
        if (!collect_result_lines(&output_lines, content)) {
            free(content);
            exit_code = MR_EXIT_INTERNAL;
            goto cleanup;
        }
        free(content);
    }

    if (!write_sorted_output(config.output_path, &output_lines)) {
        exit_code = MR_EXIT_OUTPUT;
        goto cleanup;
    }

    printf("MapReduce word count complete: %s\n", config.output_path);
    exit_code = MR_EXIT_SUCCESS;

cleanup:
    free_string_list(&output_lines);
    free(peers);
    free_string_list(&chunks);
    free_host_list(&hosts);
    free(generated_job_id);
    return exit_code;
}