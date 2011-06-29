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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.sleuthkit.hadoop.JobNames;
import org.sleuthkit.hadoop.SKJobFactory;
import org.sleuthkit.hadoop.SKMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrossImageScorerJob {
    public static final Logger LOG = LoggerFactory.getLogger(CrossImageScorerJob.class);
    
    public static enum FileCount { FILES };
    
    public static void runPipeline(String imgDir, String imgID){
        try {
            Path crossImageDir = new Path(imgDir + "/crossimg/data/");
            Path scoreDir = new Path(imgDir + "/crossimg/scores/");

            Job j = SKJobFactory.createJob(imgID, "FRIENDLYNAME", JobNames.CROSS_IMG_SIM_SCORING);
            j.setInputFormatClass(TableInputFormat.class);
            j.setOutputFormatClass(SequenceFileOutputFormat.class);
            j.setMapperClass(CrossImageScoreMapper.class);
            j.setReducerClass(CrossImageScoreReducer.class);

            j.setMapOutputKeyClass(BytesWritable.class);
            j.setMapOutputValueClass(BytesWritable.class);

            j.setOutputKeyClass(BytesWritable.class);
            j.setOutputValueClass(BytesArrayWritable.class);

            j.setJarByClass(CrossImageScoreMapper.class);
            SequenceFileOutputFormat.setOutputPath(j, crossImageDir);

            final Scan scan = new Scan();

            // This isn't good (who would ever want to use a regex to check string length?)
            // However, short of writing an entire input format, this is the best we can do.
            // It seems to improve performance by >50% with NSRL loaded in, so it's better
            // than nothing.
            scan.setFilter(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(".{20,}")));

            HBaseConfiguration.addHbaseResources(j.getConfiguration());

            j.getConfiguration().set(TableInputFormat.INPUT_TABLE, "hash");
            j.getConfiguration().set(TableInputFormat.SCAN, convertScanToString(scan));
            j.getConfiguration().set(SKMapper.ID_KEY, imgID);

            j.waitForCompletion(true);

            // get the files in this image from the hadoop counter.
            long filesInImage = j.getCounters().findCounter(FileCount.FILES).getValue();

            j = SKJobFactory.createJob(imgID, "FRIENDLYNAME", JobNames.CROSS_IMG_SIM_SCORING_CALC);
            j.getConfiguration().setLong(IIFScoreReducer.FILES_IN_IMAGE, filesInImage);
            // TODO: Get the number of images from the images table. This is pretty key for IIF.
            j.getConfiguration().setLong(IIFScoreMapper.TOTAL_IMAGES, 11);

            j.setMapperClass(IIFScoreMapper.class);
            j.setReducerClass(IIFScoreReducer.class);
            j.setJarByClass(IIFScoreMapper.class);

            j.setInputFormatClass(SequenceFileInputFormat.class);
            j.setOutputFormatClass(TextOutputFormat.class);

            j.setMapOutputKeyClass(BytesWritable.class);
            j.setMapOutputValueClass(DoubleWritable.class);

            j.setOutputKeyClass(NullWritable.class);
            j.setOutputValueClass(Text.class);


            SequenceFileOutputFormat.setOutputPath(j, scoreDir);
            SequenceFileInputFormat.setInputPaths(j, crossImageDir);

            j.waitForCompletion(true);
        } catch (IOException ex) {
            LOG.error("Failure while performing HDFS file IO.", ex);            
        } catch (ClassNotFoundException ex) {
            LOG.error("Error running job; class not found.", ex);            
        } catch (InterruptedException ex) {
            LOG.error("Hadoop job interrupted.", ex);
        }
    }
    
    public static void main(String[] argv) {
        if (argv.length != 1) {
            System.out.println("Usage: CrossImageScorerJob <img_dir> <image_hash>");
        }
        runPipeline(argv[0], argv[1]);
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
