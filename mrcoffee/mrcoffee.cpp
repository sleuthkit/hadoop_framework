
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <iterator>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <unistd.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/wait.h>

#include <boost/scoped_array.hpp>

#define THROW(msg) \
  { \
    std::stringstream ss; \
    ss << msg << ", line " << __LINE__; \
    throw std::runtime_error(ss.str()); \
  }

/*
void closer(int* sock) {
  // Always Be Closing
  if (close(*sock) == -1) {
    THROW("close: " << strerror(errno));
  }
}
*/

int exec_cmd(char** argv, int* in_pipe, int* out_pipe, int* err_pipe) {
  // fork the child
  const int pid = fork();
  if (pid == -1) {
    THROW("fork: " << strerror(errno));
  }

  if (pid != 0) {
    // we are the parent
    return pid;
  }

  // close the pipe ends we don't use
  if (close(in_pipe[1]) == -1) {
    THROW("close: " << strerror(errno));
  }

  if (close(out_pipe[0]) == -1) {
    THROW("close: " << strerror(errno));
  }

  if (close(err_pipe[0]) == -1) {
    THROW("close: " << strerror(errno));
  }

  // read child's stdin from parent
  if (dup2(in_pipe[0], STDIN_FILENO) == -1) {
    THROW("dup2: " << strerror(errno));
  }

  if (close(in_pipe[0]) == -1) {
    THROW("close: " << strerror(errno));
  }

  // send child's stdout to parent
  if (dup2(out_pipe[1], STDOUT_FILENO) == -1) {
    THROW("dup2: " << strerror(errno));
  }

  if (close(out_pipe[1]) == -1) {
    THROW("close: " << strerror(errno));
  }

  // send child's stderr to parent
  if (dup2(err_pipe[1], STDERR_FILENO) == -1) {
    THROW("dup2: " << strerror(errno));
  }

  if (close(err_pipe[1]) == -1) {
    THROW("close: " << strerror(errno));
  }

  // run the command
  if (execvp(argv[0], argv) == -1) {
    THROW("execvp: " << strerror(errno));
  }

  THROW("wtf: execvp returned something other than -1");
}

int main(int argc, char** argv) {
  try {
    // get the socket
    int s_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (s_sock == -1) {
      THROW("socket: " << strerror(errno));
    }

    // bind to all interfaces 
    in_addr ipaddr;
    ipaddr.s_addr = INADDR_ANY;

    sockaddr_in s_addr;
    memset(&s_addr, 0, sizeof(s_addr));
    s_addr.sin_family = AF_INET;
    s_addr.sin_port   = 31337;
    s_addr.sin_addr   = ipaddr;

    if (bind(s_sock, (sockaddr*) &s_addr, sizeof(s_addr)) == -1) {
      THROW("bind: " << strerror(errno));
    }

    char buf[4096];
    ssize_t rlen, wlen;
    size_t count, len, off;

    bool done = false;
    while (1) {
      // listen on our socket
      if (listen(s_sock, 10) == -1) {
        THROW("listen: " << strerror(errno));
      }

      // accept connection from client
      sockaddr_in c_addr;
      memset(&c_addr, 0, sizeof(c_addr));
      socklen_t c_addr_len = sizeof(c_addr);

      int c_sock = accept(s_sock, (sockaddr*) &c_addr, &c_addr_len);
      if (c_sock == -1) {
        THROW("accept: " << strerror(errno));
      } 

      while (1) {
        // read command length from client
        count = sizeof(len);
        while (count > 0) {
          rlen = recv(c_sock, &len, count, 0);
          if (rlen == -1) {
            THROW("recv: " << strerror(errno));
          }
          else if (rlen == 0) {
            // client shut down socket
            goto CLIENT_CLEANUP;
          }

          count -= rlen;
        }

        // read command line from client
        boost::scoped_array<char> cmd(new char[len]);

        off = 0;
        while (off < len) {
          rlen = recv(c_sock, cmd.get()+off, len-off, 0);
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

        // set up the pipes; pipes named from the POV of the child
        int in_pipe[2], out_pipe[2], err_pipe[2];

        if (pipe(in_pipe) == -1) {
          THROW("pipe: " << strerror(errno));
        }

        if (pipe(out_pipe) == -1) {
          THROW("pipe: " << strerror(errno));
        }

        if (pipe(err_pipe) == -1) {
          THROW("pipe: " << strerror(errno));
        }

        // fork the child to run the command
        const int ch_pid = exec_cmd(args.data(), in_pipe, out_pipe, err_pipe);

        // close the pipe ends we don't use
        if (close(out_pipe[1]) == -1) {
          THROW("close: " << strerror(errno));
        }

        if (close(err_pipe[1]) == -1) {
          THROW("close: " << strerror(errno));
        }

        if (close(in_pipe[0]) == -1) {
          THROW("close: " << strerror(errno));
        }

        std::vector<char> ch_out, ch_err;

        // read from client socket, child stdout, stderr as data is available
        fd_set rfds;
        int nfds;
       
        do {
          nfds = 0;

          FD_ZERO(&rfds);
      
          if (out_pipe[0] != -1) {
            FD_SET(out_pipe[0], &rfds);
            nfds = std::max(nfds, out_pipe[0]);
          }

          if (err_pipe[0] != -1) {
            FD_SET(err_pipe[0], &rfds);
            nfds = std::max(nfds, err_pipe[0]);
          }

          FD_SET(c_sock, &rfds);
          nfds = std::max(nfds, c_sock);

          if (select(nfds+1, &rfds, NULL, NULL, NULL) == -1) {
            THROW("select: " << strerror(errno));
          }

          if (FD_ISSET(c_sock, &rfds)) {
/*
            // read from client socket
            rlen = read(c_sock, buf, sizeof(buf));
            if (rlen == -1) {
              THROW("read: " << strerror(errno));
            }
            else if (rlen == 0) {
               
            }
            else {
              // write to child stdin
              wlen = write(in_pipe[1], buf, rlen);
              if (wlen == -1) {
                THROW("write: " << strerror(errno));
              }
            }
*/
          }
          
          if (FD_ISSET(out_pipe[0], &rfds)) {
            // read from child stdout
            rlen = read(out_pipe[0], buf, sizeof(buf));
            if (rlen == -1) {
              THROW("read: " << strerror(errno));
            }
            else if (rlen == 0) {
              // child closed stdout
              if (close(out_pipe[0]) == -1) {
                THROW("close: " << strerror(errno));
              }

              out_pipe[0] = -1;
            }
            else {
              // store child's stdout in ch_out
              ch_out.insert(ch_out.end(), buf, buf+rlen);
            }
          }
      
          if (FD_ISSET(err_pipe[0], &rfds)) {
            // read from child stdout
            rlen = read(err_pipe[0], buf, sizeof(buf));
            if (rlen == -1) {
              THROW("read: " << strerror(errno));
            }
            else if (rlen == 0) {
              // child closed stderr
              if (close(err_pipe[0]) == -1) {
                THROW("close: " << strerror(errno));
              }

              err_pipe[0] = -1;
            }
            else {
              // store child stderr in ch_err
              ch_err.insert(ch_err.end(), buf, buf+rlen);
            }
          }
        } while (out_pipe[0] != -1 && err_pipe[0] != -1);

        // close the child's pipes
        if (close(in_pipe[1]) == -1) {
          THROW("close: " << strerror(errno));
        }

/*
        if (close(out_pipe[0]) == -1) {
          THROW("close: " << strerror(errno));
        }

        if (close(err_pipe[0]) == -1) {
          THROW("close: " << strerror(errno));
        }
*/

        // wait for the child to exit
        if (waitpid(ch_pid, NULL, 0) == -1) {
          THROW("waitpid: " << strerror(errno));
        }

        // print child's 
        std::copy(ch_out.begin(), ch_out.end(),
          std::ostream_iterator<char>(std::cout)); 
        
        std::copy(ch_err.begin(), ch_err.end(),
          std::ostream_iterator<char>(std::cerr));

/*
        // read data length from client
        count = sizeof(len);
        while (count > 0) {
          rlen = recv(c_sock, &len, count, 0);
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
          rlen = recv(c_sock, buf, 0, 0);
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

        // close the child's in 
        if (close(in_pipe[1]) == -1) {
          THROW("close: " << strerror(errno));
        }

        // wait for the child to exit
        if (waitpid(ch_pid, NULL, 0) == -1) {
          THROW("waitpid: " << strerror(errno));
        }

        // wait for the out pump to exit
        if (waitpid(out_pid, NULL, 0) == -1) {
          THROW("waitpid: " << strerror(errno));
        }
        
        // wait for the err pump to exit
        if (waitpid(err_pid, NULL, 0) == -1) {
          THROW("waitpid: " << strerror(errno));
        }
*/
      }

      CLIENT_CLEANUP:
     
      // close the client socket 
      if (close(c_sock) == -1) {
        THROW("close: " << strerror(errno));
      }
    }

    // close the server socket
    if (close(s_sock) == -1) {
      THROW("close: " << strerror(errno));
    }
  }
  catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    exit(1);
  }

  return 0;
}
