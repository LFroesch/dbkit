BIN := dbkit
BUILD_TARGET := .
INSTALL_DIR ?= $(HOME)/.local/bin
VERSION ?= dev
LDFLAGS := -s -w -X main.version=$(VERSION)

build:
	go build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN) $(BUILD_TARGET)

install: build
	mkdir -p $(INSTALL_DIR)
	install -m 0755 $(BIN) $(INSTALL_DIR)/$(BIN)

test:
	go test ./...

vet:
	go vet ./...

clean:
	rm -f $(BIN)

.PHONY: build install test vet clean
