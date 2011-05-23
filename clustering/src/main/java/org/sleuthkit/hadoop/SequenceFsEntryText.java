package org.sleuthkit.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.lightboxtechnologies.spectrum.FsEntryHBaseInputFormat;
import com.lightboxtechnologies.spectrum.HBaseTables;

public class SequenceFsEntryText {

    public static void main(String[] argv) throws Exception {
        runTask(argv[0], argv[1], argv[2]);
    }
    
    public static void runTask(String table, String outDir, String id) throws Exception {
        final Configuration conf = new Configuration();
        //final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        final Job job = new Job(conf, "Text Extraction with FsEntry");
        job.setJarByClass(SequenceFsEntryText.class);
        job.setMapperClass(SequenceFsEntryTextMapper.class);

        // We don't need a combiner or a reducer for this job. We aren't
        // writing anything out either.
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(FsEntryHBaseInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileOutputFormat.setOutputPath(job, new Path(outDir));


        final Scan scan = new Scan();
        scan.addFamily(HBaseTables.ENTRIES_COLFAM_B);
        job.getConfiguration().set(TableInputFormat.INPUT_TABLE, table);
        job.getConfiguration().set(TableInputFormat.SCAN, convertScanToString(scan));
        job.getConfiguration().set(SKMapper.ID_KEY, id);

        job.waitForCompletion(true);
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
