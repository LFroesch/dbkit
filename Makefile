BIN := bobdb
BUILD_TARGET := .
INSTALL_DIR ?= $(HOME)/.local/bin
ALIASES := bob bdb
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

build:
	go build -ldflags "-s -w -X main.version=$(VERSION)" -o $(BIN) $(BUILD_TARGET)

install: build
	mkdir -p $(INSTALL_DIR)
	install -m 0755 $(BIN) $(INSTALL_DIR)/$(BIN)
	for alias in $(ALIASES); do ln -sf $(BIN) $(INSTALL_DIR)/$$alias; done

.PHONY: build install
