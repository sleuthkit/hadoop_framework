package com.lightboxtechnologies.nsrl;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.commons.codec.binary.Hex;

/**
 * A MR job to dump an HBase table to a text file.
 *
 * @author Joel Uckelman
 */
public class TableDumper {

  static class TableDumperMapper extends TableMapper<Text,Text> {

    private final Hex hex = new Hex();

    @Override
    public void map(ImmutableBytesWritable key, Result values, Context context)
                                     throws IOException, InterruptedException {

      final byte[] keyb = key.get();

      final NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> map1 = values.getMap();

      final StringBuilder sb = new StringBuilder();

      for (Map.Entry<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> e1 : map1.entrySet()) {
        final String family = new String(e1.getKey());
        sb.append(family).append(" => [");

        boolean e2_first = true;
        for (Map.Entry<byte[],NavigableMap<Long,byte[]>> e2 : e1.getValue().entrySet()) {
          if (e2_first) {
            e2_first = false;
          }
          else {
            sb.append(", ");
          }

          final String col = new String(e2.getKey());
          sb.append(col).append(" => [");
          for (Map.Entry<Long,byte[]> e3 : e2.getValue().entrySet()) {
            sb.append(e3.getKey()).append(" => ");

            if (col.equals("crc32") || col.equals("md5") || col.equals("sha1")) {
              sb.append(new String(hex.encode(e3.getValue())));
            }
            else if (col.equals("filesize")) {
              sb.append(Bytes.toLong(e3.getValue()));
            }
            else {
              sb.append('1');
            }
          }
          sb.append("]");
        }
        sb.append(']');
      }

      context.write(new Text(hex.encode(key.get())), new Text(sb.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    final HBaseConfiguration conf = new HBaseConfiguration();

    final String[] otherArgs =
      new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println(
        "Usage: TableDumper <table> <outpath>"
      );
      System.exit(2);
    }

    final String table_name = otherArgs[0];
    final String output_filename = otherArgs[1];

    final Job job = new Job(conf, "TableDumper");
    job.setJarByClass(TableDumper.class);

    final Scan scan = new Scan();

    TableMapReduceUtil.initTableMapperJob(
      table_name,
      scan,
      TableDumperMapper.class,
      Text.class,
      Text.class,
      job
    );

    job.setOutputFormatClass(TextOutputFormat.class);

    TextOutputFormat.setOutputPath(job, new Path(output_filename));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
