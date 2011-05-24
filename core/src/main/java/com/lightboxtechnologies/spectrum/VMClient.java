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

    if (args.length != 1) {
      throw new IllegalArgumentException("incorrect number of arguments");
    }

    // get the port
    final short port = Short.parseShort(args[0]);

    // set up the socket
    final InetAddress addr = InetAddress.getByName(null); // loopback

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
   
            byte[] size = new byte[8]; 
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
              
              // read length of command's stdout
              final int outlen = (int) in.readLong();

              // read command's stdout
              final byte[] cmdout = new byte[outlen];
              in.readFully(cmdout);
             
              // read length of command's stderr
              final int errlen = (int) in.readLong();

              // read command's stderr
              final byte[] cmderr = new byte[errlen];
              in.readFully(cmderr);

              System.out.write(cmdout); 
              System.err.write(cmderr); 
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
