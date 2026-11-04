// Copyright (c) 2026-present, Facebook, Inc. All rights reserved.
// Licensed under the repository's GPLv2 and Apache 2.0 licenses.
// Diagnostic LD_PRELOAD shim only; never use for production or durability
// tests. cc -shared -fPIC -O2 tools/zero_range_cost_probe.c -o
// /tmp/zero-cost.so -ldl
// ROCKSDB_ZERO_COST_MODE=zero|keep_all|keep_wal|keep_flush|keep_compaction|skip_flush
// ROCKSDB_ZERO_COST_OUTPUT=/absolute/path/to/summary.json
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <execinfo.h>
#include <fcntl.h>
#include <linux/falloc.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int (*real_fallocate)(int, int, off_t, off_t);
static const char* mode;
static const char* output;
struct cost {
  unsigned long long zero_calls, keep_calls, skipped_calls, bytes, nanos,
      failures;
};
static struct cost costs[4];
static const char* names[] = {"wal", "flush", "compaction", "other"};

__attribute__((constructor)) static void initialize(void) {
  real_fallocate = dlsym(RTLD_NEXT, "fallocate");
  mode = getenv("ROCKSDB_ZERO_COST_MODE");
  output = getenv("ROCKSDB_ZERO_COST_OUTPUT");
  if (!mode) mode = "zero";
  if (!real_fallocate || !output ||
      (strcmp(mode, "zero") && strcmp(mode, "keep_all") &&
       strcmp(mode, "keep_wal") && strcmp(mode, "keep_flush") &&
       strcmp(mode, "keep_compaction") && strcmp(mode, "skip_flush"))) {
    fputs("zero-range cost probe: invalid mode/output or missing fallocate\n",
          stderr);
    _exit(125);
  }
}

static int source(int fd) {
  char link[64], path[4096];
  snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
  ssize_t size = readlink(link, path, sizeof(path) - 1);
  if (size < 0) return 3;
  path[size] = 0;
  if (size >= 4 && !strcmp(path + size - 4, ".log")) return 0;
  // Attribute SST calls by the live C++ call chain, never by SST suffix alone.
  void* frames[48];
  int count = backtrace(frames, 48);
  for (int i = 0; i < count; ++i) {
    Dl_info info;
    if (dladdr(frames[i], &info) && info.dli_sname) {
      if (strstr(info.dli_sname, "CompactionJob")) return 2;
      if (strstr(info.dli_sname, "BuildTable")) return 1;
    }
  }
  return 3;
}

int fallocate(int fd, int flags, off_t offset, off_t length) {
  // All variants run this same attribution code. Change only the kernel flag;
  // Writer geometry, sealing and synchronization remain untouched.
  int saved_errno = errno;
  int kind = source(fd);
  // Isolate the cost of newly added Flush allocation while retaining the same
  // Writer accounting, seal and sync strategy. This is a diagnostic simulation
  // of successful reservation, never a supported production allocation mode.
  if (flags == FALLOC_FL_ZERO_RANGE && kind == 1 &&
      !strcmp(mode, "skip_flush")) {
    __sync_fetch_and_add(&costs[kind].skipped_calls, 1);
    __sync_fetch_and_add(&costs[kind].bytes, (unsigned long long)length);
    errno = saved_errno;
    return 0;
  }
  if (flags == FALLOC_FL_ZERO_RANGE &&
      (!strcmp(mode, "keep_all") || (kind == 0 && !strcmp(mode, "keep_wal")) ||
       (kind == 1 && !strcmp(mode, "keep_flush")) ||
       (kind == 2 && !strcmp(mode, "keep_compaction")))) {
    flags = FALLOC_FL_KEEP_SIZE;
  }
  struct timespec begin, end;
  clock_gettime(CLOCK_MONOTONIC, &begin);
  errno = saved_errno;
  int result = real_fallocate(fd, flags, offset, length);
  saved_errno = errno;
  clock_gettime(CLOCK_MONOTONIC, &end);
  struct cost* cost = &costs[kind];
  __sync_fetch_and_add(
      flags == FALLOC_FL_ZERO_RANGE ? &cost->zero_calls : &cost->keep_calls, 1);
  __sync_fetch_and_add(&cost->bytes, (unsigned long long)length);
  __sync_fetch_and_add(
      &cost->nanos,
      (end.tv_sec - begin.tv_sec) * 1000000000LL + end.tv_nsec - begin.tv_nsec);
  if (result) __sync_fetch_and_add(&cost->failures, 1);
  errno = saved_errno;
  return result;
}

__attribute__((destructor)) static void report(void) {
  FILE* file = fopen(output, "w");
  if (!file) {
    perror("zero-range cost output");
    _exit(125);
  }
  fprintf(file, "{\"mode\":\"%s\",\"sources\":{", mode);
  for (int i = 0; i < 4; ++i) {
    struct cost* c = &costs[i];
    fprintf(file,
            "%s\"%s\":{\"zero_calls\":%llu,\"keep_calls\":%llu,\"skipped_"
            "calls\":%llu,\"bytes\":%llu,"
            "\"syscall_nanos\":%llu,\"failures\":%llu}",
            i ? "," : "", names[i], c->zero_calls, c->keep_calls,
            c->skipped_calls, c->bytes, c->nanos, c->failures);
  }
  fputs("}}\n", file);
  if (fclose(file)) {
    perror("zero-range cost output close");
    _exit(125);
  }
}
