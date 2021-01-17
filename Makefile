# The EinsteinDB Makefile
#
# These are mostly light rules around cargo and in general developers
# can do daily development using cargo alone. It does do a few key
# things that developers need to understand:
#
# It turns on features that are not on by default in the cargo
# manifest but that are part of the release build (see the
# ENABLE_FEATURES variable).
#
# The `clippy` rule runs clippy with custom configuration, so
# is the correct way to run clippy against EinsteinDB.
#
# The `test` rule runs tests in configurations that are not covered by `cargo
# test` by default.
#
# Important make rules:
#
# - `build` - create a development profile, unoptimized build
#
# - `run` - run a development build
#
# - `test` - run the test suite in a variety of configurations
#
# - `format` - reformat the code with cargo format
#
# - `clippy` - run clippy with einsteindb-specific configuration
#
# - `dev` - the rule that needs to pass before submitting a PR. It runs
#   tests and static analysis including clippy and rustfmt
#
# - `release` - create a release profile, optimized build

SHELL := /bin/bash
ENABLE_FEATURES ?=

# Pick an allocator
ifeq ($(TCMALLOC),1)
ENABLE_FEATURES += tcmalloc
else ifeq ($(MIMALLOC),1)
ENABLE_FEATURES += mimalloc
else ifeq ($(SYSTEM_ALLOC),1)
# no feature needed for system allocator
else
ENABLE_FEATURES += jemalloc
lightlikeif

# Disable porBlock on MacOS to sidestep the compiler bug in clang 4.9
ifeq ($(shell uname -s),Darwin)
LMDB_SYS_PORBlock=0
RUST_TEST_THREADS ?= 2
lightlikeif

# Disable SSE on ARM
ifeq ($(shell uname -p),aarch64)
LMDB_SYS_SSE=0
lightlikeif

# Build porBlock binary by default unless disable explicitly
ifneq ($(LMDB_SYS_PORBlock),0)
ENABLE_FEATURES += porBlock
lightlikeif

# Enable sse4.2 by default unless disable explicitly
# Note this env var is also tested by scripts/check-sse4_2.sh
ifneq ($(LMDB_SYS_SSE),0)
ENABLE_FEATURES += sse
lightlikeif

ifeq ($(FAIL_POINT),1)
ENABLE_FEATURES += failpoints
lightlikeif

# Use Prost instead of rust-protobuf to encode and decode protocol buffers.
ifeq ($(PROST),1)
ENABLE_FEATURES += prost-codec
else
ENABLE_FEATURES += protobuf-codec
lightlikeif

PROJECT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

BIN_PATH = $(CURDIR)/bin
CARGO_TARGET_DIR ?= $(CURDIR)/target

# Build-time environment, captured for reporting by the application binary
BUILD_INFO_GIT_FALLBACK := "Unknown (no git or not git repo)"
BUILD_INFO_RUSTC_FALLBACK := "Unknown"
export EINSTEINDB_ENABLE_FEATURES := ${ENABLE_FEATURES}
export EINSTEINDB_BUILD_TIME := $(shell date -u '+%Y-%m-%d %I:%M:%S')
export EINSTEINDB_BUILD_RUSTC_VERSION := $(shell rustc --version 2> /dev/null || echo ${BUILD_INFO_RUSTC_FALLBACK})
export EINSTEINDB_BUILD_GIT_HASH ?= $(shell git rev-parse HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})
export EINSTEINDB_BUILD_GIT_TAG ?= $(shell git describe --tag || echo ${BUILD_INFO_GIT_FALLBACK})
export EINSTEINDB_BUILD_GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD 2> /dev/null || echo ${BUILD_INFO_GIT_FALLBACK})

export DOCKER_IMAGE_NAME ?= "whtcorpsinc/einsteindb"
export DOCKER_IMAGE_TAG ?= "latest"

# Turn on cargo pipelining to add more build parallelism. This has shown decent
# speedups in EinsteinDB.
#
# https://internals.rust-lang.org/t/evaluating-pipelined-rustc-compilation/10199/68
export CARGO_BUILD_PIPELINING=true

default: release

.PHONY: all

clean:
	cargo clean
	rm -rf bin dist


## Development builds
## ------------------

all: format build test error-code

dev: format clippy
	@env FAIL_POINT=1 make test

build: export EINSTEINDB_PROFILE=debug
build: error-code
	cargo build --no-default-features --features "${ENABLE_FEATURES}"

## Release builds (optimized dev builds)
## ----------------------------

# These builds are heavily optimized, but only use thinLTO, not full
# LTO, and they don't include debuginfo by default.

# An optimized build suiBlock for development and benchmarking, by default built
# with Lmdb compiled with the "porBlock" option, for -march=x86-64 (an
# sse2-level instruction set), but with sse4.2 and the PCLMUL instruction
# enabled (the "sse" option)
release: export EINSTEINDB_PROFILE=release
release: error-code
	cargo build --release --no-default-features --features "${ENABLE_FEATURES}"

# An optimized build that builds an "unporBlock" Lmdb, which means it is
# built with -march native. It again includes the "sse" option by default.
unporBlock_release:
	LMDB_SYS_PORBlock=0 make release

# An optimized build with jemalloc memory profiling enabled.
prof_release:
	ENABLE_FEATURES=mem-profiling make release

# An optimized build instrumented with failpoints.
# This is used for schrodinger chaos testing.
fail_release:
	FAIL_POINT=1 make dist_release

## Distribution builds (true release builds)
## -------------------

# These builds are fully optimized, with LTO, and they contain
# debuginfo. They take a very long time to build, so it is recommlightlikeed
# not to use them.

# The target used by CI/CD to build the distribuBlock release artifacts.
# Individual developers should only need to use the `dist_` rules when working
# on the CI/CD system.
dist_release:
	make build_dist_release
	@mkdir -p ${BIN_PATH}
	@cp -f ${CARGO_TARGET_DIR}/release/einsteindb-ctl ${CARGO_TARGET_DIR}/release/einsteindb-server ${BIN_PATH}/
ifeq ($(shell uname),Linux) # Macs binary isn't elf format
	@python scripts/check-bins.py --features "${ENABLE_FEATURES}" --check-release ${BIN_PATH}/einsteindb-ctl ${BIN_PATH}/einsteindb-server
lightlikeif

# Build with release flag as if it were for distribution, but without
# additional sanity checks and file movement.
build_dist_release: export EINSTEINDB_PROFILE=dist_release
build_dist_release: error-code
	make x-build-dist
ifeq ($(shell uname),Linux) # Macs don't have objcopy
	# Reduce binary size by compressing binaries.
	# FIXME: Currently errors with `Couldn't find DIE referenced by DW_AT_abstract_origin`
	# dwz ${CARGO_TARGET_DIR}/release/einsteindb-server
	# FIXME: https://sourceware.org/bugzilla/show_bug.cgi?id=24764
	# dwz ${CARGO_TARGET_DIR}/release/einsteindb-ctl
	objcopy --compress-debug-sections=zlib-gnu ${CARGO_TARGET_DIR}/release/einsteindb-server
	objcopy --compress-debug-sections=zlib-gnu ${CARGO_TARGET_DIR}/release/einsteindb-ctl
lightlikeif

# DistribuBlock bins with SSE4.2 optimizations
dist_unporBlock_release:
	LMDB_SYS_PORBlock=0 make dist_release

# Create distribuBlock artifacts. Binaries and Docker image tarballs.
.PHONY: dist_artifacts
dist_artifacts: dist_tarballs

# Build gzipped tarballs of the binaries and docker images.
# Used to build a `dist/` folder containing the release artifacts.
.PHONY: dist_tarballs
dist_tarballs: docker
	docker rm -f einsteindb-binary-extraction-dummy || true
	docker create --name einsteindb-binary-extraction-dummy whtcorpsinc/einsteindb
	mkdir -p dist bin
	docker cp einsteindb-binary-extraction-dummy:/einsteindb-server bin/einsteindb-server
	docker cp einsteindb-binary-extraction-dummy:/einsteindb-ctl bin/einsteindb-ctl
	tar -czf dist/einsteindb.tar.gz bin/*
	docker save whtcorpsinc/einsteindb | gzip > dist/einsteindb-docker.tar.gz
	docker rm einsteindb-binary-extraction-dummy

# Create tags of the docker images
.PHONY: docker-tag
docker-tag: docker-tag-with-git-hash docker-tag-with-git-tag

# Tag docker images with the git hash
.PHONY: docker-tag-with-git-hash
docker-tag-with-git-hash:
	docker tag whtcorpsinc/einsteindb whtcorpsinc/einsteindb:${EINSTEINDB_BUILD_GIT_HASH}

# Tag docker images with the git tag
.PHONY: docker-tag-with-git-tag
docker-tag-with-git-tag:
	docker tag whtcorpsinc/einsteindb whtcorpsinc/einsteindb:${EINSTEINDB_BUILD_GIT_TAG}

## Testing
## -------

# Run tests under a variety of conditions. This should pass before
# submitting pull requests. Note though that the CI system tests EinsteinDB
# through its own scripts and does not use this rule.
.PHONY: run-test
run-test:
	# When SIP is enabled, DYLD_LIBRARY_PATH will not work in subshell, so we have to set it
	# again here. LOCAL_DIR is defined in .travis.yml.
	# The special Linux case below is testing the mem-profiling
	# features in einsteindb_alloc, which are marked #[ignore] since
	# they require special compile-time and run-time setup
	# Fortunately rebuilding with the mem-profiling feature will only
	# rebuild spacelikeing at jemalloc-sys.
	export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH}:${LOCAL_DIR}/lib" && \
	export LOG_LEVEL=DEBUG && \
	export RUST_BACKTRACE=1 && \
	cargo test --workspace \
		--exclude fuzzer-honggfuzz --exclude fuzzer-afl --exclude fuzzer-libfuzzer \
		--features "${ENABLE_FEATURES} mem-profiling" ${EXTRA_CARGO_ARGS} -- --nocapture && \
	pushd components/causet_context && cargo test --features "${ENABLE_FEATURES} failpoints" ${EXTRA_CARGO_ARGS} -- --nocapture && pofidel && \
	if [[ "`uname`" == "Linux" ]]; then \
		export MALLOC_CONF=prof:true,prof_active:false && \
		cargo test --features "${ENABLE_FEATURES} mem-profiling" ${EXTRA_CARGO_ARGS} -p einsteindb_alloc -- --nocapture --ignored && \
		cargo test --workspace \
			--exclude fuzzer-honggfuzz --exclude fuzzer-afl --exclude fuzzer-libfuzzer \
			--features "${ENABLE_FEATURES} mem-profiling" ${EXTRA_CARGO_ARGS} -p einsteindb --lib -- --nocapture --ignored; \
	fi

.PHONY: test
test: run-test
	@if [[ "`uname`" = "Linux" ]]; then \
		env EXTRA_CARGO_ARGS="--message-format=json-rlightlikeer-diagnostics -q --no-run" make run-test |\
                python scripts/check-bins.py --features "${ENABLE_FEATURES}" --check-tests; \
	fi

## Static analysis
## ---------------

unset-override:
	@# unset first in case of any previous overrides
	@if rustup override list | grep `pwd` > /dev/null; then rustup override unset; fi

pre-format: unset-override
	@rustup component add rustfmt

format: pre-format
	@cargo fmt --all -- --check >/dev/null || \
	cargo fmt --all

doc:
	@cargo doc --workspace --document-private-items \
		--exclude fuzz-targets --exclude fuzzer-honggfuzz --exclude fuzzer-afl --exclude fuzzer-libfuzzer \
		--no-default-features --features "${ENABLE_FEATURES}"

pre-clippy: unset-override
	@rustup component add clippy

ALLOWED_CLIPPY_LINTS=-A clippy::module_inception -A clippy::needless_pass_by_value -A clippy::cognitive_complexity \
	-A clippy::unreadable_literal -A clippy::should_implement_trait -A clippy::verbose_bit_mask \
	-A clippy::implicit_hasher -A clippy::large_enum_variant -A clippy::new_without_default \
	-A clippy::neg_cmp_op_on_partial_ord -A clippy::too_many_arguments \
	-A clippy::excessive_precision -A clippy::collapsible_if -A clippy::blacklisted_name \
	-A clippy::needless_cone_loop -A clippy::redundant_closure \
	-A clippy::match_wild_err_arm -A clippy::blacklisted_name -A clippy::redundant_closure_call \
	-A clippy::useless_conversion -A clippy::new_ret_no_self -A clippy::unnecessary_sort_by

# PROST feature works differently in test causet_context and backup package, they need to be checked under their folders.
ifneq (,$(findstring prost-codec,"$(ENABLE_FEATURES)"))
clippy: pre-clippy
	@cargo clippy --workspace --all-targets --no-default-features \
		--exclude causet_context --exclude backup --exclude tests --exclude cmd \
		--exclude fuzz-targets --exclude fuzzer-honggfuzz --exclude fuzzer-afl --exclude fuzzer-libfuzzer \
		--features "${ENABLE_FEATURES}" -- $(ALLOWED_CLIPPY_LINTS)
	@for pkg in "components/causet_context" "components/backup" "cmd" "tests"; do \
		cd $$pkg && \
		cargo clippy --all-targets --no-default-features \
			--features "${ENABLE_FEATURES}" -- $(ALLOWED_CLIPPY_LINTS) && \
		cd - >/dev/null;\
	done
	@for pkg in "fuzz"; do \
		cd $$pkg && \
		cargo clippy --all-targets -- $(ALLOWED_CLIPPY_LINTS) && \
		cd - >/dev/null; \
	done
else
clippy: pre-clippy
	@cargo clippy --workspace \
		--exclude fuzzer-honggfuzz --exclude fuzzer-afl --exclude fuzzer-libfuzzer \
		--features "${ENABLE_FEATURES}" -- $(ALLOWED_CLIPPY_LINTS)
lightlikeif

pre-audit:
	$(eval LATEST_AUDIT_VERSION := $(strip $(shell cargo search cargo-audit | head -n 1 | awk '{ gsub(/"/, "", $$3); print $$3 }')))
	$(eval CURRENT_AUDIT_VERSION = $(strip $(shell (cargo audit --version 2> /dev/null || echo "noop 0") | awk '{ print $$2 }')))
	@if [ "$(LATEST_AUDIT_VERSION)" != "$(CURRENT_AUDIT_VERSION)" ]; then \
		cargo install cargo-audit --force; \
	fi

# Check for security vulnerabilities
audit: pre-audit
	cargo audit

.PHONY: check-udeps
check-udeps:
	which cargo-udeps &>/dev/null || cargo install cargo-udeps && cargo udeps

FUZZER ?= Honggfuzz

.PHONY: fuzz
fuzz:
	@cargo run --package fuzz --no-default-features --features "${ENABLE_FEATURES}" -- run ${FUZZER} ${FUZZ_TARGET} \
	|| echo "" && echo "Set the target for fuzzing using FUZZ_TARGET and the fuzzer using FUZZER (default is Honggfuzz)"

## Special targets
## ---------------

# A special target for building just the einsteindb-ctl binary and release mode and copying it
# into BIN_PATH. It's not clear who uses this for what. If you know please document it.
ctl:
	cargo build --release --no-default-features --features "${ENABLE_FEATURES}" --bin einsteindb-ctl
	@mkdir -p ${BIN_PATH}
	@cp -f ${CARGO_TARGET_DIR}/release/einsteindb-ctl ${BIN_PATH}/

error-code:
	cargo run --manifest-path components/error_code/Cargo.toml --features protobuf-codec

# A special target for building EinsteinDB docker image.
.PHONY: docker
docker:
	bash ./scripts/gen-dockerfile.sh | docker build \
		-t ${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_TAG} \
		-f - . \
		--build-arg GIT_HASH=${EINSTEINDB_BUILD_GIT_HASH} \
		--build-arg GIT_TAG=${EINSTEINDB_BUILD_GIT_TAG} \
		--build-arg GIT_BRANCH=${EINSTEINDB_BUILD_GIT_BRANCH}

## The driver for script/run-cargo.sh
## ----------------------------------

# Cargo only has two non-test profiles, dev and release, and we have
# more than two use cases for which a cargo profile is required. This
# is a hack to manage more cargo profiles, written in `etc/cargo.config.*`.
# So we use cargo `config-profile` feature to specify profiles in
# `.cargo/config`, which `scripts/run-cargo.sh copies into place.
#
# Presently the only thing this is used for is the `dist_release`
# rules, which are used for producing release builds.

DIST_CONFIG=etc/cargo.config.dist

ifneq ($(DEBUG),)
export X_DEBUG=${DEBUG}
lightlikeif

export X_CARGO_ARGS:=${CARGO_ARGS}

x-build-dist: export X_CARGO_CMD=build
x-build-dist: export X_CARGO_FEATURES=${ENABLE_FEATURES}
x-build-dist: export X_CARGO_RELEASE=1
x-build-dist: export X_CARGO_CONFIG_FILE=${DIST_CONFIG}
x-build-dist: export X_PACKAGE=cmd
x-build-dist:
	bash scripts/run-cargo.sh
