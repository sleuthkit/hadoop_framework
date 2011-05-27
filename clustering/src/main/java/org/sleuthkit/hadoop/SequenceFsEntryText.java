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

    public static final String GREP_MATCHES_TO_SEARCH = "org.sleuthkit.grepsearchfield";
    
    public static void main(String[] argv) throws Exception {
        runTask(argv[0], argv[1], argv[2], argv[3]);
    }
    
    public static void runTask(String table, String outDir, String id, String friendlyName) throws Exception {
        //final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = SKJobFactory.createJob(id, friendlyName, "GrepMatchesToSequenceFiles");
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
        // we want to search the default grep results. If there are any, then
        // dump the text to a file.
        job.getConfiguration().set(GREP_MATCHES_TO_SEARCH, HBaseConstants.GREP_RESULTS);
        
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
