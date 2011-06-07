package org.sleuthkit.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.lightboxtechnologies.spectrum.HBaseTables;

public class CrossImageScorerJob {

    public static void runPipeline(String imgID) throws IOException {
        Job j = SKJobFactory.createJob(imgID, "FRIENDLYNAME", JobNames.CROSS_IMG_SIM_SCORING);
        j.setInputFormatClass(TableInputFormat.class);
        j.setMapperClass(CrossImageScoreMapper.class);
        
        
        final Scan scan = new Scan();
        scan.addFamily(HBaseTables.ENTRIES_COLFAM_B);
        j.getConfiguration().set(TableInputFormat.INPUT_TABLE, "entries");
        j.getConfiguration().set(TableInputFormat.SCAN, convertScanToString(scan));
        j.getConfiguration().set(SKMapper.ID_KEY, imgID);
        
        
        // Reduce!
        
        j = SKJobFactory.createJob(imgID, "FRIENDLYNAME", JobNames.CROSS_IMG_SIM_SCORING_CALC);
        j.setInputFormatClass(SequenceFileInputFormat.class);
        j.setOutputFormatClass(SequenceFileOutputFormat.class);
       
    }
    
    public static void main(String[] argv) throws IOException {
        runPipeline(argv[0]);
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
