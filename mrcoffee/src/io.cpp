#include <unistd.h>

#include "io.h"

void read_bytes(int fd, char* buf, size_t len) {
  ssize_t rlen;
  size_t off = 0;
  while (off < len) {
    off += rlen = read(fd, buf+off, len-off);
    if (rlen == -1) {
      THROW("read: " << strerror(errno));
    }
    else if (rlen == 0) {
      THROW("read: unexpected EOF"); 
    }
  }
}

void write_bytes(int fd, const char* buf, size_t len) {
  ssize_t wlen;
  size_t off = 0;
  while (off < len) {
    off += wlen = write(fd, buf+off, len-off);
    if (wlen == -1) {
      THROW("write: " << strerror(errno));
    }
  }
}
