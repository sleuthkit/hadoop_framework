
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <vector>

#include <endian.h>
#include <fcntl.h>
#include <sys/select.h>
#include <sys/wait.h>

#include <boost/scoped_array.hpp>

#include "io.h"

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
  if (in_pipe[1] != -1) {
    CHECK(close((in_pipe[1])));
  }

  CHECK(close((out_pipe[0])));
  CHECK(close((err_pipe[0])));

  // read child's stdin from parent
  if (in_pipe[0] != -1) {
    CHECK(dup2(in_pipe[0], STDIN_FILENO));
    CHECK(close(in_pipe[0]));
  }
  else {
    CHECK(close(STDIN_FILENO));
  }

  // send child's stdout to parent
  CHECK(dup2(out_pipe[1], STDOUT_FILENO));
  CHECK(close(out_pipe[1]));

  // send child's stderr to parent
  CHECK(dup2(err_pipe[1], STDERR_FILENO));
  CHECK(close(err_pipe[1]));

  // run the command
  CHECK(execvp(argv[0], argv));
  THROW("wtf: execvp returned something other than -1");
}

void drain(
  char* src, size_t& src_written, size_t& src_available, int cfd,
  char* dst, size_t& dst_available, size_t dst_size)
{
  static const size_t header_size = sizeof(int) + sizeof(size_t);

  if (src_available > src_written) {
    const size_t buf_remaining = dst_size - dst_available;
    if (buf_remaining > header_size) {
      const int fd = htobe32(cfd);

      memcpy(dst+dst_available, &fd, sizeof(fd));
      dst_available += sizeof(fd);    

      const size_t block_size =
        std::min(buf_remaining-header_size, src_available-src_written);
      const size_t len = htobe64(block_size);
      memcpy(dst+dst_available, &len, sizeof(len));
      dst_available += sizeof(len);

std::cerr << cfd << ": wrote " << block_size << " bytes" << std::endl;

      memcpy(dst+dst_available, src+src_written, block_size);
      dst_available += block_size;

      src_written += block_size;
      if (src_written == src_available) {
        src_written = src_available = 0;
      }
    }
  }
}

bool handle_client_request(int cl_fd) {
  ssize_t rlen, wlen;
  size_t len, off;

  // read command length from client
  off = 0;
  while (off < sizeof(len)) {
    off += rlen = read(cl_fd, &len+off, sizeof(len)-off);
    if (rlen == -1) {
      THROW("read: " << strerror(errno));
    }
    else if (rlen == 0) {
      // client has disconnected
      return false;
    }
  }

  len = be64toh(len);

  // read command line from client
  boost::scoped_array<char> cmdline(new char[len]);
  read_bytes(cl_fd, cmdline.get(), len);

  // break command line into command and arguments
  std::vector<char*> args;
  args.push_back(cmdline.get());
  for (unsigned int i = 0; i < len-1; ++i) {
    if (cmdline[i] == '\0') {
      args.push_back(cmdline.get()+i+1);
    }
  }
  
  args.push_back(NULL); // last arg for execvp must be NULL

  // read data length from client
  size_t in_len;
  read_bytes(cl_fd, (char*) &in_len, sizeof(in_len));
  in_len = be64toh(in_len);

  // set up the pipes; pipes named from the POV of the child
  int in_pipe[2], out_pipe[2], err_pipe[2];

  if (in_len > 0) {
    // create in pipe only if the client will send data
    CHECK(pipe(in_pipe));
  }
  else {
    in_pipe[0] = in_pipe[1] = -1;
  }

  CHECK(pipe(out_pipe));
  CHECK(pipe(err_pipe));

  // fork the child to run the command
  const int ch_pid = exec_cmd(args.data(), in_pipe, out_pipe, err_pipe);

  // close the pipe ends we don't use
  CHECK(close((out_pipe[1])));
  CHECK(close((err_pipe[1])));

  if (in_pipe[0] != -1) {
    CHECK(close((in_pipe[0])));
  }

  // read from client socket, child stdout, stderr and write to child
  // stdin as data is available
  fd_set rfds, wfds;
  int nfds;

// FIXME: inefficient, maybe make these class members?
  const size_t header_size = sizeof(int) + sizeof(size_t);
  const size_t buf_size = 4096;

  // NB: cl_buf is larger than out_buf and err_buf by the length of one
  // header to handle the commmon case where out_buf or err_buf are full.
  char in_buf[buf_size], out_buf[buf_size], err_buf[buf_size],
       cl_buf[buf_size+header_size];

  size_t in_available = 0, in_written = 0;
  size_t out_available = 0, out_written = 0;
  size_t err_available = 0, err_written = 0;
  size_t cl_available = 0, cl_written = 0;

  do {
    // drain child stdout buffer to client buffer
    drain(out_buf, out_written, out_available, STDOUT_FILENO,
          cl_buf, cl_available, sizeof(cl_buf));

    // drain child stderr buffer to client buffer
    drain(err_buf, err_written, err_available, STDERR_FILENO,
          cl_buf, cl_available, sizeof(cl_buf));

    // create the set of file descriptors on which to select
    nfds = -1;
    FD_ZERO(&rfds);
    FD_ZERO(&wfds);

    if (out_pipe[0] != -1 && out_available < sizeof(out_buf)) {
      FD_SET(out_pipe[0], &rfds);
      nfds = std::max(nfds, out_pipe[0]);
    }

    if (err_pipe[0] != -1 && err_available < sizeof(err_buf)) {
      FD_SET(err_pipe[0], &rfds);
      nfds = std::max(nfds, err_pipe[0]);
    }

    if (cl_available > cl_written) {
      FD_SET(cl_fd, &wfds);
      nfds = std::max(nfds, cl_fd);
    }

    if (in_len > 0) {
      FD_SET(cl_fd, &rfds);
      nfds = std::max(nfds, cl_fd);
    }

    if (in_pipe[1] != -1 && in_available > in_written) {
      FD_SET(in_pipe[1], &wfds);
      nfds = std::max(nfds, in_pipe[1]);
    }

    // don't call select unless there's an active file descriptor,
    // probably should not happen, but would cause select to hang
    if (nfds < 0) {
      break;
    }

    // determine whether any file descriptors are ready
    CHECK(select(nfds+1, &rfds, &wfds, NULL, NULL));

    // write data to child stdin if available
    if (FD_ISSET(in_pipe[1], &wfds)) {
      wlen = write(in_pipe[1], in_buf+in_written, in_available-in_written);
      if (wlen == -1) {
        THROW("write: " << strerror(errno));
      }
      else {
        in_written += wlen;

        if (in_written == in_available) {
          // output has caught up with input
          in_written = in_available = 0;

          if (in_len == 0) {
            // close child's stdin
            CHECK(close(in_pipe[1]));
            in_pipe[1] = -1;
          }
        }
      }
    }

    // read data from client if there is space in the buffer
    if (FD_ISSET(cl_fd, &rfds) && in_available < sizeof(in_buf)) {
      // read from client
      rlen = read(cl_fd, in_buf+in_available,
                  std::min(sizeof(in_buf)-in_available, in_len));
      if (rlen == -1) {
        THROW("read: " << strerror(errno));
      }
      else if (rlen == 0) {
        THROW("read: client shut down socket"); 
      }
      else {
        in_len -= rlen;
        in_available += rlen;
      }
    }

    // write child stdout and stderr data to client if available
    if (FD_ISSET(cl_fd, &wfds)) {
      wlen = write(cl_fd, cl_buf+cl_written, cl_available-cl_written);
      if (wlen == -1) {
        THROW("write: " << strerror(errno));
      }
      else {
        cl_written += wlen;

        if (cl_written == cl_available) {
          // output has caught up with input
          cl_written = cl_available = 0;
        }
      }
    }

    // read data from from child stdout if there is space in the buffer
    if (FD_ISSET(out_pipe[0], &rfds)) {
      // read from child stdout
      rlen = read(out_pipe[0], out_buf+out_available,
                               sizeof(out_buf)-out_available);
      if (rlen == -1) {
        THROW("read: " << strerror(errno));
      }
      else if (rlen == 0) {
        // child closed stdout
        CHECK(close(out_pipe[0]));
        out_pipe[0] = -1;
      }
      else {
        out_available += rlen;
      }
    }

    // read data from child stderr if there is space in the buffer
    if (FD_ISSET(err_pipe[0], &rfds)) {
      // read from child sterr
      rlen = read(err_pipe[0], err_buf+err_available,
                  sizeof(err_buf)-err_available);
      if (rlen == -1) {
        THROW("read: " << strerror(errno));
      }
      else if (rlen == 0) {
        // child closed stderr
        CHECK(close(err_pipe[0]));
        err_pipe[0] = -1;
      }
      else {
        err_available += rlen;
      }
    }

  } while (in_pipe[1] != -1 || out_pipe[0] != -1 || err_pipe[0] != -1 ||
           out_available > out_written || err_available > err_written ||
           cl_available > cl_written);

  // write final stdout block to client
  int fd = htobe32(STDOUT_FILENO);
  size_t zero = 0;
  
  write_bytes(cl_fd, (char*) &fd, sizeof(fd));
  write_bytes(cl_fd, (char*) &zero, sizeof(zero));

std::cerr << STDOUT_FILENO << ": wrote 0 bytes" << std::endl;

  // write final stderr block to client 
  fd = htobe32(STDERR_FILENO);
  write_bytes(cl_fd, (char*) &fd, sizeof(fd));
  write_bytes(cl_fd, (char*) &zero, sizeof(zero));

std::cerr << STDERR_FILENO << ": wrote 0 bytes" << std::endl;

  // wait for the child to exit
  CHECK(waitpid(ch_pid, NULL, 0));

  return true;
}

int main(int argc, char** argv) {
  int chdev = -1;

  try {
    if (argc != 2) {
      THROW("incorrect number of arguments");
    }

    // open the character device
    CHECK((chdev = open(argv[1], O_RDWR))); 

    // handles client requests
    try {
      while (handle_client_request(chdev));
    }
    catch (std::exception& e) {
      close(chdev);
      std::cerr << e.what() << std::endl;
      exit(1);
    }

    // close the client 
    CHECK(close((chdev)));
    exit(0);
  }
  catch (std::exception& e) {
    close(chdev);
    std::cerr << e.what() << std::endl;
    exit(1);
  }

  return 0;
}
