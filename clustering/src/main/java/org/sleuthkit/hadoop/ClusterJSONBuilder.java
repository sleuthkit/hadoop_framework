package org.sleuthkit.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ClusterJSONBuilder {
// Concatenates file data from 
    
    public static void buildReport(Path countReport, Path clusterReport, Path outFile)
    throws IOException {
        
        
        FSDataOutputStream out = FileSystem.get(new Configuration()).create(outFile, true);
        out.write("var docClusterCounts = ".getBytes());
        writeFileToStream(countReport, out);
        out.write("\n\nvar docClusters = ".getBytes());
        writeFileToStream(clusterReport, out);
        out.flush();
        out.close();

    }
    
    public static void writeFileToStream(Path path, FSDataOutputStream stream) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        
        FSDataInputStream in = fs.open(path);

        byte[] bytes = new byte[1024];
        int i = in.read(bytes);
        while ( i != -1) {
            stream.write(bytes, 0, i);
            i = in.read(bytes);
        }
    }
}
