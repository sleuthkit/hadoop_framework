/*
Copyright 2011 Basis Technology Corp.

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

package org.sleuthkit.hadoop.scoring;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Generates an output report from the cross image scoring json output files. */
public class CrossImageJSONOutputBuilder {
 
 public static void buildReport(Path report, Path outFile)
 throws IOException {
     
     FSDataOutputStream out = FileSystem.get(new Configuration()).create(outFile, true);
     out.write("var similarityscores = ".getBytes());
     writeFileToStream(report, out);
     out.flush();
     out.close();

 }
 
 public static void writeFileToStream(Path path, FSDataOutputStream stream)
 throws IOException {
     
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

