#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "protocol.h"

struct worker_config {
    const char *bind_address;
    const char *scratch_root;
    int port;
    int timeout_ms;
    int worker_id;
};

struct endpoint {
    char *host;
    int port;
    int worker_id;
};

struct word_count {
    char *word;
    size_t count;
};

static volatile sig_atomic_t keep_running = 1;

static void handle_signal(int signal_number) {
    (void)signal_number;
    keep_running = 0;
}

static void print_usage(const char *program_name) {
    fprintf(stderr,
            "Usage: %s --port PORT --worker-id ID [--bind ADDRESS] [--scratch-root PATH] [--timeout-ms MS]\n",
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

static int parse_nonnegative_int(const char *value) {
    char *end = NULL;
    long parsed = strtol(value, &end, 10);

    if (value[0] == '\0' || end == value || *end != '\0' || parsed < 0 || parsed > 65535) {
        return -1;
    }

    return (int)parsed;
}

static bool parse_args(int argc, char **argv, struct worker_config *config) {
    config->bind_address = "0.0.0.0";
    config->scratch_root = DEFAULT_MR_SCRATCH_ROOT;
    config->port = -1;
    config->timeout_ms = DEFAULT_IO_TIMEOUT_MS;
    config->worker_id = -1;

    for (int index = 1; index < argc; ++index) {
        if (strcmp(argv[index], "--port") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --port\n");
                return false;
            }
            config->port = parse_positive_int(argv[++index]);
        } else if (strcmp(argv[index], "--worker-id") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --worker-id\n");
                return false;
            }
            config->worker_id = parse_nonnegative_int(argv[++index]);
        } else if (strcmp(argv[index], "--bind") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --bind\n");
                return false;
            }
            config->bind_address = argv[++index];
        } else if (strcmp(argv[index], "--scratch-root") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --scratch-root\n");
                return false;
            }
            config->scratch_root = argv[++index];
        } else if (strcmp(argv[index], "--timeout-ms") == 0) {
            if (index + 1 >= argc) {
                fprintf(stderr, "Missing value after --timeout-ms\n");
                return false;
            }
            config->timeout_ms = parse_positive_int(argv[++index]);
        } else if (strcmp(argv[index], "--help") == 0) {
            print_usage(argv[0]);
            exit(0);
        } else {
            fprintf(stderr, "Unknown argument: %s\n", argv[index]);
            return false;
        }
    }

    if (config->port < 0 || config->worker_id < 0 || config->timeout_ms < 0) {
        fprintf(stderr, "--port, --worker-id, and valid timeout are required\n");
        return false;
    }

    return true;
}

static bool ensure_directory(const char *path) {
    char buffer[MR_MAX_PATH_SIZE];
    size_t length = strlen(path);

    if (length == 0 || length >= sizeof(buffer)) {
        return false;
    }

    memcpy(buffer, path, length + 1);

    for (char *cursor = buffer + 1; *cursor != '\0'; ++cursor) {
        if (*cursor != '/') {
            continue;
        }

        *cursor = '\0';
        if (mkdir(buffer, 0700) < 0 && errno != EEXIST) {
            return false;
        }
        *cursor = '/';
    }

    if (mkdir(buffer, 0700) < 0 && errno != EEXIST) {
        return false;
    }

    return true;
}

static bool format_path(char *buffer, size_t size, const char *root, const char *job_id, const char *suffix) {
    int written = snprintf(buffer, size, "%s/%s/%s", root, job_id, suffix);
    return written >= 0 && (size_t)written < size;
}

static bool write_all(int fd, const void *buffer, size_t length) {
    const unsigned char *cursor = buffer;
    size_t written_total = 0;

    while (written_total < length) {
        ssize_t written = send(fd, cursor + written_total, length - written_total, 0);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        written_total += (size_t)written;
    }

    return true;
}

static bool read_exact(int fd, void *buffer, size_t length) {
    unsigned char *cursor = buffer;
    size_t received_total = 0;

    while (received_total < length) {
        ssize_t received = recv(fd, cursor + received_total, length - received_total, 0);
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (received == 0) {
            return false;
        }
        received_total += (size_t)received;
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

static int hash_word(const char *word) {
    uint32_t hash = 5381U;
    for (const unsigned char *cursor = (const unsigned char *)word; *cursor != '\0'; ++cursor) {
        hash = ((hash << 5) + hash) + *cursor;
    }
    return (int)hash;
}

static bool append_partition_record(const struct worker_config *config,
                                    const char *job_id,
                                    int reducer_id,
                                    const char *word) {
    char map_dir[MR_MAX_PATH_SIZE];
    char file_path[MR_MAX_PATH_SIZE];

    if (!format_path(map_dir, sizeof(map_dir), config->scratch_root, job_id, "maps") ||
        !ensure_directory(map_dir)) {
        return false;
    }

    int written = snprintf(file_path,
                           sizeof(file_path),
                           "%s/worker-%d-part-%d.txt",
                           map_dir,
                           config->worker_id,
                           reducer_id);
    if (written < 0 || (size_t)written >= sizeof(file_path)) {
        return false;
    }

    FILE *file = fopen(file_path, "a");
    if (file == NULL) {
        return false;
    }

    bool ok = fprintf(file, "%s\t1\n", word) >= 0;
    fclose(file);
    return ok;
}

static bool process_map_payload(const struct worker_config *config,
                                const char *job_id,
                                int reducer_count,
                                const char *payload,
                                size_t payload_size,
                                size_t *emitted_records) {
    char word[MR_MAX_WORD_SIZE];
    size_t word_length = 0;
    *emitted_records = 0;

    for (size_t index = 0; index <= payload_size; ++index) {
        unsigned char current = index < payload_size ? (unsigned char)payload[index] : (unsigned char)' ';
        if (isalnum(current)) {
            if (word_length + 1 < sizeof(word)) {
                word[word_length++] = (char)tolower(current);
            }
            continue;
        }

        if (word_length == 0) {
            continue;
        }

        word[word_length] = '\0';
        int reducer_id = hash_word(word) % reducer_count;
        if (reducer_id < 0) {
            reducer_id += reducer_count;
        }
        if (!append_partition_record(config, job_id, reducer_id, word)) {
            return false;
        }
        ++(*emitted_records);
        word_length = 0;
    }

    return true;
}

static bool send_data_response(int fd, const void *buffer, size_t length) {
    char header[MR_CONTROL_LINE_SIZE];
    int written = snprintf(header, sizeof(header), "DATA %zu\n", length);
    if (written < 0 || (size_t)written >= sizeof(header)) {
        return false;
    }
    return write_all(fd, header, (size_t)written) && (length == 0 || write_all(fd, buffer, length));
}

static bool send_simple_response(int fd, const char *message) {
    return write_all(fd, message, strlen(message));
}

static bool read_file_or_empty(const char *path, char **content, size_t *length) {
    *content = NULL;
    *length = 0;

    FILE *file = fopen(path, "r");
    if (file == NULL) {
        if (errno == ENOENT) {
            return true;
        }
        return false;
    }

    if (fseek(file, 0, SEEK_END) != 0) {
        fclose(file);
        return false;
    }

    long size = ftell(file);
    if (size < 0) {
        fclose(file);
        return false;
    }
    rewind(file);

    char *buffer = malloc((size_t)size + 1);
    if (buffer == NULL) {
        fclose(file);
        return false;
    }

    size_t read_size = fread(buffer, 1, (size_t)size, file);
    fclose(file);
    if (read_size != (size_t)size) {
        free(buffer);
        return false;
    }

    buffer[read_size] = '\0';
    *content = buffer;
    *length = read_size;
    return true;
}

static bool load_partition_content(const struct worker_config *config,
                                   const char *job_id,
                                   int worker_id,
                                   int reducer_id,
                                   char **content,
                                   size_t *length) {
    char file_path[MR_MAX_PATH_SIZE];
    int written = snprintf(file_path,
                           sizeof(file_path),
                           "%s/%s/maps/worker-%d-part-%d.txt",
                           config->scratch_root,
                           job_id,
                           worker_id,
                           reducer_id);
    if (written < 0 || (size_t)written >= sizeof(file_path)) {
        return false;
    }

    return read_file_or_empty(file_path, content, length);
}

static int connect_to_host(const char *host, int port, int timeout_ms) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char port_buffer[16];
    snprintf(port_buffer, sizeof(port_buffer), "%d", port);

    struct addrinfo *results = NULL;
    if (getaddrinfo(host, port_buffer, &hints, &results) != 0) {
        return -1;
    }

    int connected_fd = -1;
    for (struct addrinfo *current = results; current != NULL; current = current->ai_next) {
        int socket_fd = socket(current->ai_family, current->ai_socktype, current->ai_protocol);
        if (socket_fd < 0) {
            continue;
        }

        int flags = 0;
        if ((flags = fcntl(socket_fd, F_GETFL, 0)) >= 0) {
            (void)fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK);
        }

        if (connect(socket_fd, current->ai_addr, current->ai_addrlen) == 0) {
            connected_fd = socket_fd;
        } else if (errno == EINPROGRESS) {
            struct pollfd poll_fd;
            memset(&poll_fd, 0, sizeof(poll_fd));
            poll_fd.fd = socket_fd;
            poll_fd.events = POLLOUT;

            if (poll(&poll_fd, 1, timeout_ms) > 0) {
                int socket_error = 0;
                socklen_t socket_error_size = sizeof(socket_error);
                if (getsockopt(socket_fd, SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_size) == 0 && socket_error == 0) {
                    connected_fd = socket_fd;
                }
            }
        }

        if (connected_fd >= 0) {
            int connected_flags = fcntl(connected_fd, F_GETFL, 0);
            if (connected_flags < 0 || fcntl(connected_fd, F_SETFL, connected_flags & ~O_NONBLOCK) < 0) {
                close(connected_fd);
                connected_fd = -1;
                continue;
            }

            struct timeval timeout;
            timeout.tv_sec = timeout_ms / 1000;
            timeout.tv_usec = (timeout_ms % 1000) * 1000;
            if (setsockopt(connected_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0 ||
                setsockopt(connected_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0) {
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

static bool fetch_remote_data(const char *host,
                              int port,
                              int timeout_ms,
                              const char *command,
                              char **content,
                              size_t *length) {
    *content = NULL;
    *length = 0;

    int fd = connect_to_host(host, port, timeout_ms);
    if (fd < 0) {
        return false;
    }

    char line[MR_CONTROL_LINE_SIZE];
    if (!write_all(fd, command, strlen(command)) || !read_line(fd, line, sizeof(line))) {
        close(fd);
        return false;
    }

    size_t expected = 0;
    if (sscanf(line, "DATA %zu", &expected) != 1) {
        close(fd);
        return false;
    }

    char *buffer = malloc(expected + 1);
    if (buffer == NULL) {
        close(fd);
        return false;
    }

    if (expected > 0 && !read_exact(fd, buffer, expected)) {
        free(buffer);
        close(fd);
        return false;
    }

    buffer[expected] = '\0';
    close(fd);
    *content = buffer;
    *length = expected;
    return true;
}

static bool append_count(struct word_count **items,
                         size_t *count,
                         size_t *capacity,
                         const char *word,
                         size_t delta) {
    for (size_t index = 0; index < *count; ++index) {
        if (strcmp((*items)[index].word, word) == 0) {
            (*items)[index].count += delta;
            return true;
        }
    }

    if (*count == *capacity) {
        size_t next_capacity = *capacity == 0 ? 64 : (*capacity * 2);
        struct word_count *next_items = realloc(*items, next_capacity * sizeof(*next_items));
        if (next_items == NULL) {
            return false;
        }
        *items = next_items;
        *capacity = next_capacity;
    }

    (*items)[*count].word = strdup(word);
    if ((*items)[*count].word == NULL) {
        return false;
    }
    (*items)[*count].count = delta;
    ++(*count);
    return true;
}

static void free_counts(struct word_count *items, size_t count) {
    for (size_t index = 0; index < count; ++index) {
        free(items[index].word);
    }
    free(items);
}

static bool merge_partition_text(struct word_count **counts,
                                 size_t *count,
                                 size_t *capacity,
                                 const char *text) {
    const char *cursor = text;
    while (*cursor != '\0') {
        const char *line_end = strchr(cursor, '\n');
        size_t line_length = line_end == NULL ? strlen(cursor) : (size_t)(line_end - cursor);
        if (line_length > 0) {
            const char *tab = memchr(cursor, '\t', line_length);
            if (tab != NULL) {
                size_t word_length = (size_t)(tab - cursor);
                if (word_length > 0 && word_length < MR_MAX_WORD_SIZE) {
                    char word[MR_MAX_WORD_SIZE];
                    memcpy(word, cursor, word_length);
                    word[word_length] = '\0';
                    size_t delta = (size_t)strtoull(tab + 1, NULL, 10);
                    if (delta == 0) {
                        delta = 1;
                    }
                    if (!append_count(counts, count, capacity, word, delta)) {
                        return false;
                    }
                }
            }
        }
        if (line_end == NULL) {
            break;
        }
        cursor = line_end + 1;
    }
    return true;
}

static int compare_counts(const void *left, const void *right) {
    const struct word_count *lhs = left;
    const struct word_count *rhs = right;
    return strcmp(lhs->word, rhs->word);
}

static bool write_reduce_output(const struct worker_config *config,
                                const char *job_id,
                                int reducer_id,
                                struct word_count *counts,
                                size_t count,
                                char *output_path,
                                size_t output_path_size) {
    char output_dir[MR_MAX_PATH_SIZE];
    if (!format_path(output_dir, sizeof(output_dir), config->scratch_root, job_id, "outputs") ||
        !ensure_directory(output_dir)) {
        return false;
    }

    int written = snprintf(output_path,
                           output_path_size,
                           "%s/reduce-%d.txt",
                           output_dir,
                           reducer_id);
    if (written < 0 || (size_t)written >= output_path_size) {
        return false;
    }

    qsort(counts, count, sizeof(*counts), compare_counts);

    FILE *file = fopen(output_path, "w");
    if (file == NULL) {
        return false;
    }

    for (size_t index = 0; index < count; ++index) {
        if (fprintf(file, "%s\t%zu\n", counts[index].word, counts[index].count) < 0) {
            fclose(file);
            return false;
        }
    }

    fclose(file);
    return true;
}

static bool parse_endpoints(char *payload, struct endpoint **items, size_t *count) {
    *items = NULL;
    *count = 0;
    size_t capacity = 0;

    char *line = strtok(payload, "\n");
    while (line != NULL) {
        char host[256];
        int port = 0;
        int worker_id = -1;
        int fields = sscanf(line, "%255s %d %d", host, &port, &worker_id);
        if ((fields == 2 || fields == 3) && port > 0) {
            if (*count == capacity) {
                size_t next_capacity = capacity == 0 ? 8 : (capacity * 2);
                struct endpoint *next_items = realloc(*items, next_capacity * sizeof(*next_items));
                if (next_items == NULL) {
                    return false;
                }
                *items = next_items;
                capacity = next_capacity;
            }
            (*items)[*count].host = strdup(host);
            if ((*items)[*count].host == NULL) {
                return false;
            }
            (*items)[*count].port = port;
            (*items)[*count].worker_id = worker_id;
            ++(*count);
        }
        line = strtok(NULL, "\n");
    }

    return true;
}

static void free_endpoints(struct endpoint *items, size_t count) {
    for (size_t index = 0; index < count; ++index) {
        free(items[index].host);
    }
    free(items);
}

static bool handle_fetch_partition(const struct worker_config *config, int client_fd, const char *job_id, int reducer_id) {
    char *content = NULL;
    size_t length = 0;
    if (!load_partition_content(config, job_id, config->worker_id, reducer_id, &content, &length)) {
        return send_simple_response(client_fd, "ERR fetch-failed\n");
    }

    bool ok = send_data_response(client_fd, content == NULL ? "" : content, length);
    free(content);
    return ok;
}

static bool handle_get_result(const struct worker_config *config, int client_fd, const char *job_id, int reducer_id) {
    char file_path[MR_MAX_PATH_SIZE];
    int written = snprintf(file_path,
                           sizeof(file_path),
                           "%s/%s/outputs/reduce-%d.txt",
                           config->scratch_root,
                           job_id,
                           reducer_id);
    if (written < 0 || (size_t)written >= sizeof(file_path)) {
        return send_simple_response(client_fd, "ERR path-too-long\n");
    }

    char *content = NULL;
    size_t length = 0;
    if (!read_file_or_empty(file_path, &content, &length)) {
        return send_simple_response(client_fd, "ERR result-failed\n");
    }

    bool ok = send_data_response(client_fd, content == NULL ? "" : content, length);
    free(content);
    return ok;
}

static bool handle_map(const struct worker_config *config,
                       int client_fd,
                       const char *job_id,
                       int task_id,
                       int reducer_count,
                       size_t payload_size) {
    char *payload = malloc(payload_size + 1);
    if (payload == NULL) {
        return send_simple_response(client_fd, "ERR out-of-memory\n");
    }

    if (payload_size > 0 && !read_exact(client_fd, payload, payload_size)) {
        free(payload);
        return false;
    }

    payload[payload_size] = '\0';
    size_t emitted_records = 0;
    bool ok = process_map_payload(config, job_id, reducer_count, payload, payload_size, &emitted_records);
    free(payload);
    if (!ok) {
        return send_simple_response(client_fd, "ERR map-failed\n");
    }

    char response[MR_CONTROL_LINE_SIZE];
    int written = snprintf(response,
                           sizeof(response),
                           "OK MAP %s %d %zu\n",
                           job_id,
                           task_id,
                           emitted_records);
    if (written < 0 || (size_t)written >= sizeof(response)) {
        return false;
    }
    return send_simple_response(client_fd, response);
}

static bool handle_reduce(const struct worker_config *config,
                          int client_fd,
                          const char *job_id,
                          int reducer_id,
                          size_t peers_size) {
    char *payload = malloc(peers_size + 1);
    if (payload == NULL) {
        return send_simple_response(client_fd, "ERR out-of-memory\n");
    }

    if (peers_size > 0 && !read_exact(client_fd, payload, peers_size)) {
        free(payload);
        return false;
    }
    payload[peers_size] = '\0';

    struct endpoint *endpoints = NULL;
    size_t endpoint_count = 0;
    if (!parse_endpoints(payload, &endpoints, &endpoint_count)) {
        free(payload);
        free_endpoints(endpoints, endpoint_count);
        return send_simple_response(client_fd, "ERR bad-peers\n");
    }
    free(payload);

    struct word_count *counts = NULL;
    size_t count = 0;
    size_t capacity = 0;
    size_t total_values = 0;

    char command[MR_CONTROL_LINE_SIZE];
    int written = snprintf(command, sizeof(command), "FETCH %s %d\n", job_id, reducer_id);
    if (written < 0 || (size_t)written >= sizeof(command)) {
        free_endpoints(endpoints, endpoint_count);
        return send_simple_response(client_fd, "ERR bad-command\n");
    }

    for (size_t index = 0; index < endpoint_count; ++index) {
        char *content = NULL;
        size_t length = 0;
        if (endpoints[index].worker_id == config->worker_id) {
            if (!load_partition_content(config, job_id, config->worker_id, reducer_id, &content, &length)) {
                free_endpoints(endpoints, endpoint_count);
                free_counts(counts, count);
                return send_simple_response(client_fd, "ERR fetch-self\n");
            }
        } else if (!fetch_remote_data(endpoints[index].host, endpoints[index].port, config->timeout_ms, command, &content, &length)) {
            free_endpoints(endpoints, endpoint_count);
            free_counts(counts, count);
            return send_simple_response(client_fd, "ERR fetch-peer\n");
        }
        for (size_t offset = 0; offset < length; ++offset) {
            if (content[offset] == '\n') {
                ++total_values;
            }
        }
        if (!merge_partition_text(&counts, &count, &capacity, content)) {
            free(content);
            free_endpoints(endpoints, endpoint_count);
            free_counts(counts, count);
            return send_simple_response(client_fd, "ERR reduce-merge\n");
        }
        free(content);
    }

    free_endpoints(endpoints, endpoint_count);

    char output_path[MR_MAX_PATH_SIZE];
    if (!write_reduce_output(config, job_id, reducer_id, counts, count, output_path, sizeof(output_path))) {
        free_counts(counts, count);
        return send_simple_response(client_fd, "ERR reduce-write\n");
    }
    free_counts(counts, count);

    char response[MR_CONTROL_LINE_SIZE];
    written = snprintf(response,
                       sizeof(response),
                       "OK REDUCE %s %d %zu %zu %s\n",
                       job_id,
                       reducer_id,
                       count,
                       total_values,
                       output_path);
    if (written < 0 || (size_t)written >= sizeof(response)) {
        return false;
    }

    return send_simple_response(client_fd, response);
}

static bool dispatch_request(const struct worker_config *config, int client_fd) {
    char line[MR_CONTROL_LINE_SIZE];
    if (!read_line(client_fd, line, sizeof(line))) {
        return false;
    }

    if (strcmp(line, "PING") == 0) {
        char response[MR_CONTROL_LINE_SIZE];
        int written = snprintf(response, sizeof(response), "PONG %d\n", config->worker_id);
        return written >= 0 && (size_t)written < sizeof(response) && send_simple_response(client_fd, response);
    }

    char job_id[128];
    int value_a = 0;
    int value_b = 0;
    size_t payload_size = 0;

    if (sscanf(line, "MAP %127s %d %d %zu", job_id, &value_a, &value_b, &payload_size) == 4) {
        return handle_map(config, client_fd, job_id, value_a, value_b, payload_size);
    }
    if (sscanf(line, "FETCH %127s %d", job_id, &value_a) == 2) {
        return handle_fetch_partition(config, client_fd, job_id, value_a);
    }
    if (sscanf(line, "REDUCE %127s %d %zu", job_id, &value_a, &payload_size) == 3) {
        return handle_reduce(config, client_fd, job_id, value_a, payload_size);
    }
    if (sscanf(line, "RESULT %127s %d", job_id, &value_a) == 2) {
        return handle_get_result(config, client_fd, job_id, value_a);
    }

    return send_simple_response(client_fd, "ERR unknown-command\n");
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
    struct worker_config config;
    if (!parse_args(argc, argv, &config)) {
        print_usage(argv[0]);
        return 1;
    }

    if (!ensure_directory(config.scratch_root)) {
        perror("mkdir scratch-root");
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

    int server_fd = create_listener(config.bind_address, config.port);
    if (server_fd < 0) {
        return 1;
    }

    fprintf(stderr,
            "mr_worker listening on %s:%d worker_id=%d scratch_root=%s\n",
            config.bind_address,
            config.port,
            config.worker_id,
            config.scratch_root);

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

        (void)dispatch_request(&config, client_fd);
        close(client_fd);
    }

    close(server_fd);
    return 0;
}