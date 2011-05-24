
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <boost/scoped_array.hpp>
#include <boost/shared_ptr.hpp>

#define THROW(msg) \
  { \
    std::stringstream ss; \
    ss << msg << ", line " << __LINE__; \
    throw std::runtime_error(ss.str()); \
  }

void closer(int* sock) {
  // Always Be Closing
  if (close(*sock) == -1) {
    THROW("close: " << strerror(errno));
  }
}

int main(int argc, char** argv) {
  try {

    // get the socket
    boost::shared_ptr<int> c_sock(new int, closer);
    *c_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (*c_sock == -1) {
      THROW("socket: " << strerror(errno));
    }

    // connect to the server
    in_addr ipaddr;
    ipaddr.s_addr = inet_addr("127.0.0.1");

    sockaddr_in s_addr;
    s_addr.sin_family = AF_INET;
    s_addr.sin_port = 31337;
    s_addr.sin_addr = ipaddr; 

    if (connect(*c_sock, (sockaddr*) &s_addr, sizeof(sockaddr_in)) == -1) {
      THROW("connect: " << strerror(errno));
    }

    // send lines from stdin to the server
    ssize_t rlen, wlen;
    size_t len, off;
    std::string line;

    char buf[4096];

    while (std::getline(std::cin, line)) {
      // parse command line into arguments

      // send length of command line
      len = line.length()+1;
      off = 0;
      while (off < sizeof(len)) {
        off += wlen = send(*c_sock, &len+off, sizeof(len)-off, 0);     
        if (wlen == -1) {
          THROW("send: " << strerror(errno));
        }
      }

      // convert spaces to nulls
      boost::scoped_array<char> cmd(new char[line.length()+1]);
      memcpy(cmd.get(), line.c_str(), line.length()+1);     

      for (unsigned int i = 0; i < line.length()+1; ++i) {
        if (cmd[i] == ' ') {
          cmd[i] = '\0';
        }
      }

      // send the command line
      off = 0;
      while (off < line.length()+1) {
        off += wlen = send(*c_sock, cmd.get()+off, line.length()+1-off, 0);
        if (wlen == -1) {
          THROW("send: " << strerror(errno));
        }
      }
 
      // send data length
      len = 0;
      off = 0;
      while (off < sizeof(len)) {
        off += wlen = send(*c_sock, &len+off, sizeof(len)-off, 0);
        if (wlen == -1) {
          THROW("send: " << strerror(errno));
        }
      }

      // read length of command's stdout
      off = 0;
      while (off < sizeof(len)) {
        off += rlen = recv(*c_sock, &len+off, sizeof(len)-off, 0);
        if (rlen == -1) {
          THROW("recv: " << strerror(errno));
        }
        else if (rlen == 0) {
          THROW("recv: client shut down socket"); 
        }
      }

      // read command's stdout
      off = 0;
      while (off < len) {
        off += rlen = recv(*c_sock, buf, std::min(sizeof(buf), len-off), 0);
        if (rlen == -1) {
          THROW("recv: " << strerror(errno));
        }
        else if (rlen == 0) {
          THROW("recv: client shut down socket"); 
        }

        wlen = write(STDOUT_FILENO, buf, rlen);
        if (wlen == -1) {
          THROW("write: " << strerror(errno));
        }
      }

      // read length of command's stderr
      off = 0;
      while (off < sizeof(len)) {
        off += rlen = recv(*c_sock, &len+off, sizeof(len)-off, 0);
        if (rlen == -1) {
          THROW("recv: " << strerror(errno));
        }
        else if (rlen == 0) {
          THROW("recv: client shut down socket"); 
        }
      }

      // read command's stderr
      off = 0;
      while (off < len) {
        off += rlen = recv(*c_sock, buf, std::min(sizeof(buf), len-off), 0);
        if (rlen == -1) {
          THROW("recv: " << strerror(errno));
        }
        else if (rlen == 0) {
          THROW("recv: client shut down socket"); 
        }

        wlen = write(STDERR_FILENO, buf, rlen);
        if (wlen == -1) {
          THROW("write: " << strerror(errno));
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
