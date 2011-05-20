package org.sleuthkit.hadoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.tika.Tika;
import org.sleuthkit.hadoop.TikaTextExtractor.TikaTextExtractorMapper;

import com.lightboxtechnologies.spectrum.FolderCount;
import com.lightboxtechnologies.spectrum.FsEntry;
import com.lightboxtechnologies.spectrum.FsEntryHBaseInputFormat;
import com.lightboxtechnologies.spectrum.HBaseTables;
import com.lightboxtechnologies.spectrum.FolderCount.FolderCountMapper;

public class FSEntryTikaTextExtractor {
    
    /*
		private static int PATH_COL = 0;
		private static int CONTENT_COL = 1;
		private static String SEQ_FILE_NAME = "sequenceFilePath";
		private static String JOB_NAME = "TikaTextExtractor";
//		private static byte [][] family = new byte[2][]; // data:data
//		private static byte [][] column = new byte[2][]; // path cont

		private static byte [] FAMILY = Bytes.toBytes("data");

		private static byte [] COL_CONT = Bytes.toBytes("cont");
		private static byte [] COL_PATH = Bytes.toBytes("path");

		static class TikaTextExtractorMapper extends Mapper<Text, FsEntry, Text, FsEntry> {
			private SequenceFile.Writer writer = null;

			@Override
			protected void setup(Context context) throws IOException, InterruptedException {
				System.out.println(HBaseFileImporter.HDFS_PROP_NAME + "=" + context.getConfiguration().get(HBaseFileImporter.HDFS_PROP_NAME));
				String sfPath = context.getConfiguration().get(SEQ_FILE_NAME);
				if (sfPath == null) {
					throw new IOException(SEQ_FILE_NAME + " is not set in configuration");
				}
				openSequenceFile(sfPath, context.getConfiguration());
				super.setup(context);
			}

			@Override
			protected void cleanup(Context context) throws IOException, InterruptedException {
				if (writer != null) {
					writer.close();
					writer = null;
				}
				super.cleanup(context);
			}

			@Override
			public void map(Text key, FsEntry value, Context context) throws IOException {
				InputStream is = null;
				//first check if file content is in row:
				byte[] value = values.getValue(FAMILY, COL_CONT);
				if (value == null) {	//don't have content - must have path
					byte[] path = values.getValue(FAMILY, COL_PATH);
					if (path == null) {
						throw new IOException("No path or content in row " + row);
					}
					String filePath = new String(path);
					FileSystem fs = FileSystem.get(context.getConfiguration());
					System.out.println("opening file: " + filePath);
					is = fs.open(new Path(filePath));
				}
				else {	//have content
					is = new ByteArrayInputStream(value);
				}
				try {
					writeToSequenceFile(row.get(), new Tika().parseToString(is));
				}
				catch (Exception e) {
					//keep on going
					System.err.println("Failed to extract text from file in row" + row);
					e.printStackTrace();
				}
			}

			public void openSequenceFile(String filePath, Configuration conf) throws IOException {
				FileSystem fs = FileSystem.get(conf);
				Path path = new Path(filePath);
				writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class);
			}

			public void writeToSequenceFile(byte[] key, String value) throws IOException {
				if (value.length() > 0) {
					writer.append(new Text(key), new Text(value));
				}
			}
		}

		public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
			String tableName = args[0];
			Job job = new Job(conf, JOB_NAME + "_" + tableName);
			job.setJarByClass(TikaTextExtractor.class);

			Scan scan = new Scan();
			for (int i = 0; i <= 1; i++) {
				String[] fields = args[i+1].split(":");
//				if (fields.length != 2) {
//					reportUsageAndExit();
//				}
//				else {
//					//family[i] = Bytes.toBytes(fields[0]);
//					//column[i] = Bytes.toBytes(fields[1]);
//					scan.addColumn(FAMILY, column[i]);
	//
//				}
				scan.addColumn(FAMILY, COL_PATH);
				scan.addColumn(FAMILY, COL_CONT);
			}



			job.getConfiguration().set(SEQ_FILE_NAME, args[3]);
			job.setOutputFormatClass(NullOutputFormat.class);
			TableMapReduceUtil.initTableMapperJob(tableName, scan, TikaTextExtractorMapper.class, ImmutableBytesWritable.class, Result.class, job);
			job.setNumReduceTasks(0);
			return job;
		}

		public static void reportUsageAndExit() {
			System.err.println("Usage: TikaTextExtractor <tablename> <family:columnPath> <family:columnContent> <sequenceFileNameHDFS>");
			System.exit(-1);
		}

		public static void main(String[] args) throws Exception {
		    final Configuration conf = new Configuration();
		    final String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		    if (otherArgs.length != 2) {
		      System.err.println("Usage: FolderCount <table> <outpath>");
		      System.exit(2);
		    }

		    final Job job = new Job(conf, "FolderCount");
		    job.setJarByClass(FSEntryTikaTextExtractor.class);
		    job.setMapperClass(TikaTextExtractorMapper.class);
		    
		    
		    job.setCombinerClass(IntSumReducer.class);
		    job.setReducerClass(IntSumReducer.class);
		    job.setNumReduceTasks(1);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    job.setInputFormatClass(FsEntryHBaseInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);

		    final Scan scan = new Scan();
		    scan.addFamily(HBaseTables.ENTRIES_COLFAM_B);
		    job.getConfiguration().set(TableInputFormat.INPUT_TABLE, otherArgs[0]);
		    job.getConfiguration().set(TableInputFormat.SCAN, convertScanToString(scan));

		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

		public static int runPipeline(String tablename, String columnPath, String columnContent, String sequenceFileName) throws Exception{
		    Configuration conf = HBaseConfiguration.create();
		    String[] otherArgs = new GenericOptionsParser(conf, new String[] {tablename, columnPath, columnContent, sequenceFileName}).getRemainingArgs();

		    Job job = createSubmittableJob(conf, otherArgs);
		    return job.waitForCompletion(true) ? 0 : 1;

		}
		*/
}
