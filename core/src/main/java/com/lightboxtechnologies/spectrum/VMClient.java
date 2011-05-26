package com.lightboxtechnologies.spectrum;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;

import com.lightboxtechnologies.io.IOUtils;

public class VMClient {

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      throw new IllegalArgumentException("incorrect number of arguments");
    }

    // set up the socket
    final InetAddress addr = InetAddress.getByName(args[0]);

    // get the port
    final short port = Short.parseShort(args[1]);

    Socket sock = null;
    try {
      sock = new Socket(addr, port); 

      DataInputStream in = null;
      try {
        in = new DataInputStream(sock.getInputStream());

        DataOutputStream out = null;
        try {
          out = new DataOutputStream(sock.getOutputStream());

          // send lines from stdin to the server
          BufferedReader stdin = null;
          try {
            stdin = new BufferedReader(new InputStreamReader(System.in));
   
            final byte[] buf = new byte[4096];
            String line;
            while ((line = stdin.readLine()) != null) {
              // parse command line into arguments
              
              // send length of command line
              out.writeLong(line.length()+1);

              // convert spaces to nulls
              // NB: This is an ugly hack. Split up your command line
              // arguments yourself. 
              final byte[] cmd = new byte[line.length()+1];
              System.arraycopy(line.getBytes(), 0, cmd, 0, line.length());
              for (int i = 0; i < cmd.length; ++i) {
                if (cmd[i] == ' ') cmd[i] = 0; 
              }
              cmd[cmd.length-1] = 0;

              // send the command line
              out.write(cmd);

              // send data length (zero, in our case)
              out.writeLong(0);

              boolean out_done = false, err_done = false;
              while (!out_done || !err_done) {
                // read file descriptor
                final int fd = in.readInt();

                // read length of block
                long len = in.readLong();

                // check whether one of the streams has ended
                if (len == 0) {
                  if (fd == 1) {      // stdout
                    out_done = true;
                    continue;
                  }
                  else if (fd == 2) { // stderr
                    err_done = true;
                    continue;
                  }
                }

                // read the data block
                int size;
                for ( ; len > 0; len -= size) {
                  size = (int) Math.min(buf.length, len);
                  in.readFully(buf, 0, size);
                  (fd == 1 ? System.out : System.err).write(buf, 0, size);
                }
              }
            }

            stdin.close();
          }
          finally {
            IOUtils.closeQuietly(stdin);
          }

          out.close();
        }
        finally {
        }

        in.close();
      }
      finally {
        IOUtils.closeQuietly(in);
      }

      sock.close();
    }
    finally {
      IOUtils.closeQuietly(sock);
    }
  }
}
