#pragma once

#include <cerrno>
#include <cstring>
#include <sstream>
#include <stdexcept>

#include <unistd.h>

#define THROW(msg) \
  { \
    std::stringstream ss; \
    ss << msg << ", line " << __LINE__; \
    throw std::runtime_error(ss.str()); \
  }

#define CHECK(expr) \
  if ((expr) == -1) THROW(#expr << ": " << strerror(errno));

void read_bytes(int fd, char* buf, size_t len);
void write_bytes(int fd, const char* buf, size_t len);

