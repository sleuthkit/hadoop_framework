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

