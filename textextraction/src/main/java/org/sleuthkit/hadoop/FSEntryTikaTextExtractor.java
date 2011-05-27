package org.sleuthkit.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.tika.Tika;

import com.lightboxtechnologies.spectrum.FsEntry;
import com.lightboxtechnologies.spectrum.FsEntryHBaseInputFormat;
import com.lightboxtechnologies.spectrum.FsEntryHBaseOutputFormat;
import com.lightboxtechnologies.spectrum.HBaseTables;
public class FSEntryTikaTextExtractor {

    static class TikaTextExtractorMapper extends SKMapper<Text, FsEntry, Text, FsEntry> {

        @Override
        public void map(Text key, FsEntry value, Context context) throws IOException {
            InputStream proxy = value.getInputStream();
            System.out.println("Extracting Text from file:" + key.toString());
            try {
                String output = new Tika().parseToString(proxy);
                value.put(HBaseConstants.EXTRACTED_TEXT, output);
                context.write(key, value);
            }
            catch (Exception e) {
                //keep on going
                System.err.println("Failed to extract text from a file: " + new String(key.getBytes()));
                e.printStackTrace();
            }
        }
    }

    public static void reportUsageAndExit() {
        System.err.println("Usage: TikaTextExtractor <tablename> <family:columnPath> <family:columnContent> <sequenceFileNameHDFS>");
        System.exit(-1);
    }

    public static void runTask(String table, String imageID, String friendlyName) throws Exception {
        
        Job job = SKJobFactory.createJob(imageID, friendlyName, "TikaTextExtraction");

        
        job.setJarByClass(FSEntryTikaTextExtractor.class);
        job.setMapperClass(TikaTextExtractorMapper.class);

        // We don't need a combiner or a reducer for this job. We aren't
        // writing anything out either.
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FsEntry.class);
        job.setInputFormatClass(FsEntryHBaseInputFormat.class);
        job.setOutputFormatClass(FsEntryHBaseOutputFormat.class);

        final Scan scan = new Scan();
        scan.addFamily(HBaseTables.ENTRIES_COLFAM_B);
        job.getConfiguration().set(TableInputFormat.INPUT_TABLE, table);
        job.getConfiguration().set(TableInputFormat.SCAN, convertScanToString(scan));
        job.getConfiguration().set(SKMapper.ID_KEY, imageID);
        System.out.println("Spinning of MR Job...");
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
