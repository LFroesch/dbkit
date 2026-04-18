BIN := bobdb
BUILD_TARGET := .
INSTALL_DIR ?= $(HOME)/.local/bin
ALIASES := bob bdb

build:
	go build -o $(BIN) $(BUILD_TARGET)

install: build
	mkdir -p $(INSTALL_DIR)
	install -m 0755 $(BIN) $(INSTALL_DIR)/$(BIN)
	for alias in $(ALIASES); do ln -sf $(BIN) $(INSTALL_DIR)/$$alias; done

.PHONY: build install
