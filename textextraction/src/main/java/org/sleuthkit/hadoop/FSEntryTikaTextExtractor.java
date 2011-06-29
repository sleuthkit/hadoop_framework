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

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.mapreduce.Job;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightboxtechnologies.spectrum.FsEntry;
import com.lightboxtechnologies.spectrum.ImmutableHexWritable;
import com.lightboxtechnologies.spectrum.FsEntryHBaseInputFormat;
import com.lightboxtechnologies.spectrum.FsEntryHBaseOutputFormat;

public class FSEntryTikaTextExtractor {

    static class TikaTextExtractorMapper extends SKMapper<ImmutableHexWritable, FsEntry, ImmutableHexWritable, FsEntry> {

        public static final Logger LOG = LoggerFactory.getLogger(FSEntryTikaTextExtractor.class);

        @Override
        public void map(ImmutableHexWritable key, FsEntry value, Context context) throws IOException, InterruptedException {
            // Don't extract text for known good files.
            if (isKnownGood(value)) { return; }
            
            InputStream proxy = value.getInputStream();
            System.out.println("Extracting Text from file:" + key.toString());
            if (proxy == null) { return; } //No stream for this file. Get out.
            try {
                String output = new Tika().parseToString(proxy);
                value.put(HBaseConstants.EXTRACTED_TEXT, output);
                context.write(key, value);
            }
            catch (TikaException e) {
                //keep on going
                LOG.warn("Could not extract text from file " + key + ".");
            } finally {
                proxy.close();
            }
        }
    }

    public static void reportUsageAndExit() {
        System.err.println("Usage: TikaTextExtractor <tablename> <family:columnPath> <family:columnContent> <sequenceFileNameHDFS>");
        System.exit(-1);
    }

    public static void runTask(String table, String imageID, String friendlyName) throws Exception {
        
        Job job = SKJobFactory.createJob(imageID, friendlyName, JobNames.TIKA_TEXT_EXTRACTION);

        
        job.setJarByClass(FSEntryTikaTextExtractor.class);
        job.setMapperClass(TikaTextExtractorMapper.class);

        // We don't need a combiner or a reducer for this job. We aren't
        // writing anything out either.
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(ImmutableHexWritable.class);
        job.setOutputValueClass(FsEntry.class);
        job.setInputFormatClass(FsEntryHBaseInputFormat.class);
        job.setOutputFormatClass(FsEntryHBaseOutputFormat.class);

        FsEntryHBaseInputFormat.setupJob(job, imageID);

        System.out.println("Spinning off TextExtraction Job...");
        job.waitForCompletion(true);
    }

    public static void main (String[] argv) throws Exception { 
        runPipeline(argv[0], argv[1], argv[2]);
    }
    
    public static int runPipeline(String tablename, String deviceID, String friendlyName) throws Exception{
        // We probably won't have that outdir there down the line, but for
        // testing I've left it in. Since there are not yet set methods, we
        // are simply dumping the outputted text to a sequence file. Once we
        // get one, we'll have to move that part to separate step.
        runTask(tablename, deviceID, friendlyName);
        return 0;

    }

}
