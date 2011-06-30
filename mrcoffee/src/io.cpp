/*
   Copyright 2011, Lightbox Technologies, Inc

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

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
