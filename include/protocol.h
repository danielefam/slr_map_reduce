#ifndef PROTOCOL_H
#define PROTOCOL_H

#define LOAD_PROTOCOL_VERSION 1
#define MR_PROTOCOL_VERSION 1
#define DEFAULT_HOST_COUNT 100
#define DEFAULT_CONNECT_TIMEOUT_MS 2000
#define DEFAULT_IO_TIMEOUT_MS 2000
#define DEFAULT_CANDIDATE_PORT_START 20000
#define DEFAULT_CANDIDATE_PORT_END 20999

#define DEFAULT_HOSTS_FILE "run/hosts.txt"
#define DEFAULT_MANIFEST_FILE "run/manifest.env"
#define DEFAULT_MR_MANIFEST_FILE "run/mr_manifest.env"
#define DEFAULT_MR_JOB_FILE "run/mr_job.env"
#define DEFAULT_MR_RESULTS_FILE "run/mr_wordcount.txt"

#define DEFAULT_MR_CHUNK_LINES 200
#define DEFAULT_MR_SCRATCH_ROOT "/tmp/slr_map_reduce"
#define DEFAULT_MR_RPC_RETRIES 6
#define DEFAULT_MR_RPC_RETRY_DELAY_MS 400

#define LOAD_RESPONSE_FIELDS 3
#define LOAD_RESPONSE_SIZE 128

#define MR_CONTROL_LINE_SIZE 1024
#define MR_IO_BUFFER_SIZE 4096
#define MR_MAX_PATH_SIZE 4096
#define MR_MAX_WORD_SIZE 128

/*
 * Stable MapReduce coordinator/script exit codes.
 * Keep these values backward-compatible once published.
 */
#define MR_EXIT_SUCCESS 0
#define MR_EXIT_USAGE 2
#define MR_EXIT_HOSTS 3
#define MR_EXIT_INPUT 4
#define MR_EXIT_SETUP 5
#define MR_EXIT_NETWORK 6
#define MR_EXIT_PROTOCOL 7
#define MR_EXIT_OUTPUT 8
#define MR_EXIT_INTERNAL 9

#endif