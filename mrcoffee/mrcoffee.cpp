
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <unistd.h>
#include <sys/socket.h>
#include <sys/wait.h>
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

int main(int argc, char** arvg) {
  try {
    // get the socket
    boost::shared_ptr<int> s_sock(new int, closer);
    *s_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (*s_sock == -1) {
      THROW("socket: " << strerror(errno));
    }

    // bind to all interfaces 
    in_addr ipaddr;
    ipaddr.s_addr = INADDR_ANY;

    sockaddr_in s_addr;
    s_addr.sin_family = AF_INET;
    s_addr.sin_port   = 31337;
    s_addr.sin_addr   = ipaddr;

    if (bind(*s_sock, (sockaddr*) &s_addr, sizeof(sockaddr_in)) == -1) {
      THROW("bind: " << strerror(errno));
    }

    char buf[4096];
    ssize_t rlen, wlen;
    size_t count, len, off;

    bool done = false;
    do {
      // listen on our socket
      if (listen(*s_sock, 10) == -1) {
        THROW("listen: " << strerror(errno));
      }

      // accept connection from client
      sockaddr_in c_addr;
      socklen_t c_addr_len = sizeof(sockaddr_in);

      boost::shared_ptr<int> c_sock(new int, closer);
      *c_sock = accept(*s_sock, (sockaddr*) &c_addr, &c_addr_len);
      if (*c_sock == -1) {
        THROW("accept: " << strerror(errno));
      } 

      do {
        // read command length from client
        count = sizeof(len);
        while (count > 0) {
          rlen = recv(*c_sock, &len, count, 0);
          if (rlen == -1) {
            THROW("recv: " << strerror(errno));
          }
          else if (rlen == 0) {
            THROW("recv: client shut down socket"); 
          }

          count -= rlen;
        }

        // read command line from client
        boost::scoped_array<char> cmd(new char[len]);

        off = 0;
        while (off < len) {
          rlen = recv(*c_sock, cmd.get()+off, len-off, 0);
          if (rlen == -1) {
            THROW("recv: " << strerror(errno));
          }
          else if (rlen == 0) {
            THROW("recv: client shut down socket"); 
          }

          count -= rlen;
          off += rlen;
        }

        // break command line into command and arguments
// TODO: crap way of doing this, clobbers guarded spaces
        std::vector<char*> args;
        args.push_back(cmd.get());
        for (unsigned int i = 0; i < len; ++i) {
          if (cmd[i] == ' ') {
            cmd[i] = '\0';
            args.push_back(cmd.get()+i+1);
          }
        }

        int cp_pipe[2], pc_pipe[2];

        // set up the pipes        
        if (pipe(cp_pipe) == -1) {
          THROW("pipe: " << strerror(errno));
        }

        if (pipe(pc_pipe) == -1) {
          THROW("pipe: " << strerror(errno));
        }

        // fork the child
        const int pid = fork();
        if (pid == -1) {
          THROW("fork: " << strerror(errno));
        }

        if (pid == 0) {   // we are the child

          // close the pipe ends we don't use
          if (close(pc_pipe[0]) == -1) {
            THROW("close: " << strerror(errno));
          }

          if (close(cp_pipe[1]) == -1) {
            THROW("close: " << strerror(errno));
          }

          // read child's stdin from parent
          if (dup2(STDIN_FILENO, pc_pipe[1]) == -1) {
            THROW("dup2: " << strerror(errno));
          }

          // send child's stdout to parent
          if (dup2(STDOUT_FILENO, cp_pipe[0]) == -1) {
            THROW("dup2: " << strerror(errno));
          }

          // run the command
          if (execvp(args[0], args.data()) == -1) {
            THROW("execvp: " << strerror(errno));
          }

          THROW("wtf: execvp returned something other than -1");
        }
        else {            // we are the parent

          // close the pipe ends we don't use
          if (close(cp_pipe[0]) == -1) {
            THROW("close: " << strerror(errno));
          }

          if (close(pc_pipe[1]) == -1) {
            THROW("close: " << strerror(errno));
          }

          // read data length from client
          count = sizeof(len);
          while (count > 0) {
            rlen = recv(*c_sock, &len, count, 0);
            if (rlen == -1) {
              THROW("recv: " << strerror(errno));
            }
            else if (rlen == 0) {
              THROW("recv: client shut down socket"); 
            }

            count -= rlen;
          }

          while (count > 0) {
            // read input from socket
            rlen = recv(*c_sock, buf, 0, 0);
            if (rlen == -1) {
              THROW("recv: " << strerror(errno));
            }
            else if (rlen == 0) {
              THROW("recv: client shut down socket"); 
            }

            count -= rlen;

            // write input to child's stdin
            wlen = write(pc_pipe[0], buf, rlen);
            if (wlen == -1) {
              THROW("send: " << strerror(errno));
            }
          }
       
          // close the pipe to the child 
          if (close(pc_pipe[0]) == -1) {
            THROW("close: " << strerror(errno));
          }

          // read output from child's stdout
          while ((rlen = read(cp_pipe[1], buf, 0))) {
            if (rlen == -1) {
              THROW("read: " << strerror(errno));
            }

/*
            // write output to socket
            wlen = send(*c_sock, buf, rlen, 0);
            if (wlen == -1) {
              THROW("send: " << strerror(errno));
            }
*/

            std::cout << buf << std::endl;
          }
        
          // close the pipe from the child
          if (close(cp_pipe[1]) == -1) {
            THROW("close: " << strerror(errno));
          }

          // wait for the child to exit
          if (waitpid(pid, NULL, 0) == -1) {
            THROW("waitpid: " << strerror(errno));
          }
        }

      } while (!done);
// TODO: do something to disconnect properly
    } while (!done);
  }
  catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    exit(1);
  }

  return 0;
}
