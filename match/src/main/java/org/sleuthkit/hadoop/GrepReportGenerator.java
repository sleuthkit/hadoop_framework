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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightboxtechnologies.spectrum.FsEntryHBaseInputFormat;
import com.lightboxtechnologies.spectrum.HBaseTables;

// Contains methods to generate reports based on the output of the grep
// search engine.
public class GrepReportGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(GrepReportGenerator.class);
    
    public static void runPipeline(String regexFile, String deviceID, String friendlyName) {
        // STEP 1: Generate 'a' value file which maps regexes to numbers.
        
        try {
            // Read in the regex file.
            FileSystem fs = FileSystem.get(new Configuration());
            Path inFile = new Path(regexFile);
            FSDataInputStream in = fs.open(inFile);

            byte[] bytes = new byte[1024];

            StringBuilder b = new StringBuilder();
            int i = in.read(bytes);
            while ( i != -1) {
                b.append(new String(bytes).substring(0, i));
                i = in.read(bytes);
            }
            
            
            // Stringified version of the regex file.
            String regexes = b.toString();
            
            /* String[] regexList = regexes.split("\n");
            
            JSONArray ar = new JSONArray();
            int a = 0;

            for (String regex : regexList) {
                JSONObject obj = new JSONObject();
                obj.put("a", a);
                obj.put("kw", regex);
                ar.add(obj);
            }
            
            // Dump JSON to file
            
            Path outFile = new Path(regexJSONDest);
            
            FSDataOutputStream out = fs.create(outFile, true);
            out.writeUTF(ar.toJSONString());
            out.flush();
            out.close();
            */
            
            Job job = SKJobFactory.createJob(deviceID, friendlyName, JobNames.GREP_COUNT_MATCHED_EXPRS_JSON);
            job.setJarByClass(GrepCountMapper.class);
            job.setMapperClass(GrepCountMapper.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(LongWritable.class);
            
            job.getConfiguration().set("mapred.mapper.regex", regexes);

            // we must have precisely one reducer.
            job.setNumReduceTasks(1);
            job.setReducerClass(GrepCountReducer.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(FsEntryHBaseInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, new Path("/texaspete/data/" + deviceID + "/grep/count"));
            
            final Scan scan = new Scan();
            scan.addFamily(HBaseTables.ENTRIES_COLFAM_B);
            job.getConfiguration().set(TableInputFormat.INPUT_TABLE, "entries");
            job.getConfiguration().set(TableInputFormat.SCAN, convertScanToString(scan));

            job.waitForCompletion(true);
            ///////////////////////////////////////////////////////////////////
            
            
            job = SKJobFactory.createJob(deviceID, friendlyName, JobNames.GREP_MATCHED_EXPRS_JSON);
            job.setJarByClass(GrepMatchMapper.class);
            job.setMapperClass(GrepMatchMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);
            
            job.getConfiguration().set("mapred.mapper.regex", regexes);

            // we must have precisely one reducer.
            job.setNumReduceTasks(1);
            job.setReducerClass(GrepMatchReducer.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(FsEntryHBaseInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            
            job.getConfiguration().set(TableInputFormat.INPUT_TABLE, "entries");
            job.getConfiguration().set(TableInputFormat.SCAN, convertScanToString(scan));
            
            TextOutputFormat.setOutputPath(job, new Path("/texaspete/data/" + deviceID + "/grep/matchinfo"));

            job.waitForCompletion(true);
            
            GrepJSONBuilder.buildReport(new Path("/texaspete/data/" + deviceID + "/grep/count/part-r-00000"),
                    new Path("/texaspete/data/" + deviceID + "/grep/matchinfo/part-r-00000"),
                    new Path("/texaspete/data/" + deviceID + "/grep/json"));
            
        } catch (Exception ex) {
            LOG.error("Exception while attempting to output grep.", ex);
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


    
}
