
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include <endian.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

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

    if (argc != 3) {
      THROW("incorrect number of arguments");
    }

    // get the socket
    boost::shared_ptr<int> c_sock(new int, closer);
    *c_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (*c_sock == -1) {
      THROW("socket: " << strerror(errno));
    }

    // get the port
    const in_port_t port = htobe16(boost::lexical_cast<in_port_t>(argv[2]));

    // connect to the server
    in_addr ipaddr;
    if (inet_aton(argv[1], &ipaddr) == 0) {
      THROW("inet_aton: " << strerror(errno));
    }

    sockaddr_in s_addr;
    s_addr.sin_family = AF_INET;
    s_addr.sin_port = port;
    s_addr.sin_addr = ipaddr; 

    CHECK(connect(*c_sock, (sockaddr*) &s_addr, sizeof(sockaddr_in)));

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
