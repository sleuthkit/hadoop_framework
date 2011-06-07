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
import org.sleuthkit.hadoop.SequenceBytesToText.SequenceByteToTextMapper;

public class TextToSequenceBytes {

    public class SequenceTexttoByteMapper 
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