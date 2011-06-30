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

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include <endian.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <boost/lexical_cast.hpp>
#include <boost/scoped_array.hpp>
#include <boost/shared_ptr.hpp>

#include "io.h"

void closer(int* sock) {
  // Always Be Closing
  if (close(*sock) == -1) {
    THROW("close: " << strerror(errno));
  }
}

int main(int argc, char** argv) {
  try {

    if (argc != 2) {
      THROW("incorrect number of arguments");
    }

    // get the socket 
    boost::shared_ptr<int> c_sock(new int, closer);
    *c_sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (*c_sock == -1) {
      THROW("socket: " << strerror(errno));
    }

    // connect to the server
    sockaddr_un s_addr;
    memset(&s_addr, 0, sizeof(s_addr));
    s_addr.sun_family = AF_UNIX;
    strncpy(s_addr.sun_path, argv[1], sizeof(s_addr.sun_path));

    CHECK(connect(*c_sock, (sockaddr*) &s_addr, sizeof(sockaddr_un)));

    // send lines from stdin to the server
    int fd;
    size_t len;
    std::string line;

    char buf[4096];

    while (std::getline(std::cin, line)) {
      // parse command line into arguments

      // send length of command line
      len = htobe64(line.length()+1);
      write_bytes(*c_sock, (char*) &len, sizeof(len));

      // convert spaces to nulls
      boost::scoped_array<char> cmd(new char[line.length()+1]);
      memcpy(cmd.get(), line.c_str(), line.length()+1);     

      for (unsigned int i = 0; i < line.length()+1; ++i) {
        if (cmd[i] == ' ') {
          cmd[i] = '\0';
        }
      }

      // send the command line
      write_bytes(*c_sock, cmd.get(), line.length()+1);
 
      // send data length (zero, in our case)
      len = htobe64(0);
      write_bytes(*c_sock, (char*) &len, sizeof(len));

      bool out_done = false, err_done = false;
      while (!out_done || !err_done) {
        // read file descriptor
        read_bytes(*c_sock, (char*) &fd, sizeof(fd));
        fd = be32toh(fd);

        // read length of block
        read_bytes(*c_sock, (char*) &len, sizeof(len));
        len = be64toh(len);

        // check whether one of the streams has ended
        if (len == 0) {
          if (fd == STDOUT_FILENO) {
            out_done = true;
            continue;
          }
          else if (fd == STDERR_FILENO) {
            err_done = true;
            continue;
          }
        }

        // read the data block
        size_t size;
        for ( ; len > 0; len -= size) {
          size = std::min(sizeof(buf), len);
          read_bytes(*c_sock, buf, size);
          write_bytes(fd, buf, size);
        }
      }
    }

// TODO: close socket
  }
  catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    exit(1);
  }

  return 0;
}
