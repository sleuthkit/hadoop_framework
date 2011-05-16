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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

// Contains classes and utility methods that will convert sequence files with
// BytesWritable keys to Text keys. This allows us to run the mahout classes
// over them, because although the underlying mapper and reducer classes are
// written in such a way that would render this a non-issue, the Driver class
// tells Hadoop to expect Text as keys.
public class SequenceBytesToText {

    public class SequenceByteToTextMapper 
    extends Mapper<BytesWritable, Writable, Text, Writable> {
        
        @Override
        public void setup(Context context) {
        }
        
        @Override
        public void map(BytesWritable key, Writable value, Context context)
        throws IOException {
            Text t = new Text(key.getBytes());
            try {
                context.write(t, key);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    public static int sequenceDirectory(String inputDir, String outputDir) throws InterruptedException, ClassNotFoundException {
        Job job;
        try {
            job = new Job();
            
            job.setJarByClass(ClusterDocuments.class);

            job.setJobName("TP$IMG_ID_NUMBER$CommonName$SequenceBytesToText");

            FileInputFormat.setInputPaths(job, new Path(inputDir));
            FileOutputFormat.setOutputPath(job, new Path(outputDir));

            job.setMapperClass(SequenceByteToTextMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Writable.class);

            job.setReducerClass(Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Writable.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return 0;

    }
    
    
}
