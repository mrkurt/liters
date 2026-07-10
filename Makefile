# liters build helpers.
#
# `make oracle` builds the Go reference binaries used by the interop test
# suite ("the oracle"): the litestream CLI from reference/litestream and the
# ltx CLI at the exact version litestream pins. Tests locate them via
# LITERS_ORACLE_DIR (defaults to target/oracle) and skip if absent.

ORACLE_DIR := $(CURDIR)/target/oracle
LITESTREAM := $(ORACLE_DIR)/litestream
LTX_CLI    := $(ORACLE_DIR)/ltx
HELPER     := $(ORACLE_DIR)/oracle-helper
LTX_VERSION := v0.5.1

.PHONY: oracle test clean-oracle

oracle: $(LITESTREAM) $(LTX_CLI) $(HELPER)

$(HELPER): tests/oracle-helper/main.go tests/oracle-helper/go.mod
	mkdir -p $(ORACLE_DIR)
	cd tests/oracle-helper && go build -o $(HELPER) .

$(LITESTREAM): $(wildcard reference/litestream/*.go) reference/litestream/go.mod
	mkdir -p $(ORACLE_DIR)
	cd reference/litestream && go build -o $(LITESTREAM) ./cmd/litestream

$(LTX_CLI):
	mkdir -p $(ORACLE_DIR)
	GOBIN=$(ORACLE_DIR) go install github.com/superfly/ltx/cmd/ltx@$(LTX_VERSION)

test: oracle
	LITERS_ORACLE_DIR=$(ORACLE_DIR) cargo test --workspace

clean-oracle:
	rm -rf $(ORACLE_DIR)
