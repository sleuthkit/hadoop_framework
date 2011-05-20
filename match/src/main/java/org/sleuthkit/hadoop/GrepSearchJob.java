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

package org.sleuthkit.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import com.lightboxtechnologies.spectrum.FsEntry;
import com.lightboxtechnologies.spectrum.FsEntryHBaseInputFormat;
import com.lightboxtechnologies.spectrum.FsEntryHBaseOutputFormat;
import com.lightboxtechnologies.spectrum.HBaseTables;

/* Extracts matching regexes from input files and counts them. */
public class GrepSearchJob {
    public GrepSearchJob() {}


    public static final String DEFAULT_INPUT_DIR =  "hdfs://localhost/texaspete/text/";
    public static final String DEFAULT_OUTPUT_DIR = "hdfs://localhost/texaspete/grepped/";


    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            // TODO: We don't support groups yet. We could add them.
            System.out.println("Grep <table> <deviceID> <regexFile> [<group>]");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }
        return runPipeline(args[0], args[1], args[2]);
    }
    
    // Kicks off a mapreduce job that does the following:
    // 1. Scan the HBASE table for files on the drive specified by the
    // device ID.
    // 2. Run the java regexes from the given regexFile on that file.
    public static int runPipeline(String table, String deviceID, String regexFile) {
        
        try {
            Job job = new Job();
            job.setJarByClass(GrepSearchJob.class);

            job.setJobName("TP$IMG_ID_NUMBER$CommonName$Grep");

            FileSystem fs = FileSystem.get(job.getConfiguration());
            //fs.delete(new Path(outputdir), true);
            Path inFile = new Path(regexFile);
            FSDataInputStream in = fs.open(inFile);

            // Read the regex file, set a property on the configuration object
            // to store them in a place accessible by all of the child jobs.

            byte[] bytes = new byte[1024];

            StringBuilder b = new StringBuilder();
            int i = in.read(bytes);
            while ( i != -1) {
                b.append(new String(bytes).substring(0, i));
                i = in.read(bytes);
            }
            System.out.println("regexes are: " + b.toString());
            String regexes = b.toString();

            job.getConfiguration().set("mapred.mapper.regex", regexes);
            
            
            job.setMapperClass(GrepMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FsEntry.class);

            // we are not reducing.
            job.setNumReduceTasks(0);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FsEntry.class);

            job.setInputFormatClass(FsEntryHBaseInputFormat.class);
            job.setOutputFormatClass(FsEntryHBaseOutputFormat.class);
            
            final Scan scan = new Scan();
            scan.addFamily(HBaseTables.ENTRIES_COLFAM_B);
            job.getConfiguration().set(TableInputFormat.INPUT_TABLE, table);
            job.getConfiguration().set(TableInputFormat.SCAN, convertScanToString(scan));


            System.out.println("About to run the job...");
            
            return job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception ex) {
            ex.printStackTrace();
            return 2;
        }
    }

    
    static String convertScanToString(Scan scan) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        DataOutputStream dos = null;
        try {
          dos = new DataOutputStream(out);
          scan.write(dos);
          dos.close();
        }
        finally {
          IOUtils.closeQuietly(dos);
        }

        return Base64.encodeBytes(out.toByteArray());
      }

    
    public static void main(String[] args) throws Exception {
        new GrepSearchJob().run(args);
        //int res = ToolRunner.run(new Configuration(), new GrepSearchJob(), args);
        System.exit(0);
    }

}