CC ?= cc
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -O2
CPPFLAGS += -D_POSIX_C_SOURCE=200809L -Iinclude
BIN_DIR := bin
HOST_COUNT ?= 100
PORT ?= 23000
HOSTS_FILE ?= run/hosts.txt
MANIFEST ?= run/manifest.env
PORT_START ?= 20000
PORT_END ?= 20099
REMOTE_DIRNAME ?= slr_map_reduce_bundle

TARGETS := $(BIN_DIR)/load_server $(BIN_DIR)/load_client

.PHONY: all clean directories smoke select-hosts select-port deploy status stop clean-remote redeploy run-client experiment

all: directories $(TARGETS)

directories:
	mkdir -p $(BIN_DIR) run

$(BIN_DIR)/load_server: src/server.c include/protocol.h | directories
	$(CC) $(CPPFLAGS) $(CFLAGS) $< -o $@

$(BIN_DIR)/load_client: src/client.c include/protocol.h | directories
	$(CC) $(CPPFLAGS) $(CFLAGS) $< -o $@

smoke: all
	bash scripts/smoke_local.sh $(PORT)

select-hosts:
	bash scripts/select_hosts.sh --count $(HOST_COUNT) --output $(HOSTS_FILE)

select-port:
	bash scripts/select_port.sh --hosts $(HOSTS_FILE) --output $(MANIFEST) --start-port $(PORT_START) --end-port $(PORT_END)

deploy: all
	bash scripts/deploy.sh --count $(HOST_COUNT) --manifest $(MANIFEST) --start-port $(PORT_START) --end-port $(PORT_END) --remote-dirname $(REMOTE_DIRNAME)

status:
	bash scripts/status.sh --manifest $(MANIFEST)

stop:
	bash scripts/stop.sh --manifest $(MANIFEST)

clean-remote:
	bash scripts/clean_remote.sh --manifest $(MANIFEST)

redeploy: all
	bash scripts/clean_remote.sh --manifest $(MANIFEST)
	bash scripts/deploy.sh --count $(HOST_COUNT) --manifest $(MANIFEST) --start-port $(PORT_START) --end-port $(PORT_END) --remote-dirname $(REMOTE_DIRNAME)

run-client: all
	bash scripts/run_client.sh --manifest $(MANIFEST)

experiment: deploy
	bash scripts/status.sh --manifest $(MANIFEST)
	bash scripts/run_client.sh --manifest $(MANIFEST)

clean:
	rm -rf $(BIN_DIR)