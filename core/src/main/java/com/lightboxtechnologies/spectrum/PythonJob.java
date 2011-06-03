/*
Copyright 2011, Lightbox Technologies, Inc

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

package com.lightboxtechnologies.spectrum;

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import javax.script.*;


public class PythonJob {

  public static final Log LOG = LogFactory.getLog(PythonJob.class.getName());

  enum Counters {
    INPUT_RECORDS,
    OUTPUT_RECORDS,
    WARNINGS,
    ERRORS
  }

  static interface BoxerUnboxer<T,B extends Writable> {
    public B getWritable();
    public B set(Object o);
    public Class<B> getBoxClass();
    public T unbox(Writable w);
  }

  static class BytesBoxerUnboxer implements BoxerUnboxer<byte[], BytesWritable> {
    private final BytesWritable Box = new BytesWritable();

    public BytesWritable getWritable() {
      return Box;
    }
    
    public BytesWritable set(Object o) {
      if (o instanceof BytesWritable) {
        return (BytesWritable)o;
      }
      byte[] b = (byte[])o;
      Box.set(b, 0, b.length);
      return Box;
    }

    public Class<BytesWritable> getBoxClass() {
      return BytesWritable.class;
    }

    public byte[] unbox(Writable w) {
      return ((BytesWritable)w).getBytes();
    }
  }

  static class TextBoxerUnboxer implements BoxerUnboxer<String,Text> {
    private final Text Box = new Text();

    public Text getWritable() {
      return Box;
    }

    public Text set(Object o) {
      if (o instanceof Text) {
        return (Text)o;
      }

      Box.set((String)o);
      return Box;
    }

    public Class<Text> getBoxClass() {
      return Text.class;
    }

    public String unbox(Writable w) {
      return ((Text)w).toString();
    }
  }

  static class LongBoxerUnboxer implements BoxerUnboxer<Long,LongWritable> {
    private final LongWritable Box = new LongWritable();

    public LongWritable getWritable() {
      return Box;
    }

    public LongWritable set(Object o) {
      if (o instanceof LongWritable) {
        return (LongWritable)o;
      }
      else if (o instanceof BigInteger) {
        Box.set(((BigInteger)o).longValue());
      }
      else {
        Box.set((Long)o);
      }
      return Box;
    }

    public Class<LongWritable> getBoxClass() {
      return LongWritable.class;
    }

    public Long unbox(Writable w) {
      return ((LongWritable)w).get();
    }
  }

  static class DoubleBoxerUnboxer implements BoxerUnboxer<Double,DoubleWritable> {
    private final DoubleWritable Box = new DoubleWritable();

    public DoubleWritable getWritable() {
      return Box;
    }

    public DoubleWritable set(Object o) {
      if (o instanceof DoubleWritable) {
        return (DoubleWritable)o;
      }

      Box.set((Double) o);
      return Box;
    }

    public Class<DoubleWritable> getBoxClass() {
      return DoubleWritable.class;
    }

    public Double unbox(Writable w) {
      return ((DoubleWritable)w).get();
    }
  }

  static class JsonBoxerUnboxer implements BoxerUnboxer<Object,JsonWritable> {
    private final JsonWritable Box = new JsonWritable();

    public JsonWritable getWritable() {
      return Box;
    }

    public JsonWritable set(Object o) {
      if (o instanceof Map) {
        Box.set((Map)o);
      }
      else if (o instanceof List) {
        Box.set((List)o);
      }
      else {
        throw new RuntimeException(
          "Object is not JSON serializable: " + o.getClass().getName()
        );
      }
      return Box;
    }

    public Class<JsonWritable> getBoxClass() {
      return JsonWritable.class;
    }

    public Object unbox(Writable w) {
      return ((JsonWritable)w).get();
    }
  }

  static BoxerUnboxer<?,? extends Writable> createOutputType(String type) {
    if ("text".equals(type)) {
      return new TextBoxerUnboxer();
    }
    else if ("long".equals(type)) {
      return new LongBoxerUnboxer();
    }
    else if ("double".equals(type)) {
      return new DoubleBoxerUnboxer();
    }
    else if ("json".equals(type)) {
      return new JsonBoxerUnboxer();
    }
    else if ("bytes".equals(type)) {
      return new BytesBoxerUnboxer();
    }
    return null;
  }

  public static class PyEngine<IK,IV,OK extends Writable,OV extends Writable> {
    private Log ScriptLog;
    private boolean FirstTime = true;
    private ScriptEngine Engine;
    Invocable Invoker;
    private BoxerUnboxer<?,OK> OutKey;
    private BoxerUnboxer<?,OV> OutValue;
    private TaskInputOutputContext<IK,IV,OK,OV> Ctx;

    PyEngine() {
      LOG.info("Initializing Python engine");

      Engine = new ScriptEngineManager().getEngineByName("python");

      //Engine = new PyScriptEngineFactory().getScriptEngine();
      //new ScriptEngineManager().getEngineByName("python");
      if (Engine == null) {
        LOG.error("Could not create Python engine!");
      }
      else {
        LOG.info("Created Python engine");
        Invoker = (Invocable)Engine;
      }
    }

    // These functions mirror the Python logging functions,
    // and map to the appropriate Apache settings.
    public void debug(Object msg) {
      ScriptLog.debug(msg);
    }

    public void info(Object msg) {
      ScriptLog.info(msg);
    }

    public void warning(Object msg) {
      ScriptLog.warn(msg);
      Ctx.getCounter(Counters.WARNINGS).increment(1);
    }

    public void error(Object msg) {
      ScriptLog.error(msg);
      Ctx.getCounter(Counters.ERRORS).increment(1);
    }

    public void critical(Object msg) {
      ScriptLog.fatal(msg);
    }

    public BoxerUnboxer<?,OK> getOutKey() {
      return OutKey;
    }

    public BoxerUnboxer<?,OV> getOutValue() {
      return OutValue;
    }

    @SuppressWarnings("unchecked")
    public BoxerUnboxer<?,OK> createOutKey(String type) {
      return (BoxerUnboxer<?,OK>) createOutputType(type);
    }

    @SuppressWarnings("unchecked")
    public BoxerUnboxer<?,OV> createOutValue(String type) {
      return (BoxerUnboxer<?,OV>) createOutputType(type);
    }

    public void eval(Reader script, String scriptName) {
      LOG.info("Evaluating script " + scriptName);
      try {
        Engine.eval(script);
        LOG.info("Evaluated script successfully");
        String kt = (String)Engine.get("keyType"),
               vt = (String)Engine.get("valueType");
        LOG.info("keyType = " + kt + "; valueType = " + vt);
        OutKey = createOutKey(kt);
        OutValue = createOutValue(vt);
//        LOG.debug(script.);
        ScriptLog = LogFactory.getLog(new Path(scriptName).getName());
      }
      catch (ScriptException err) {
        LOG.warn("Script had a problem in evaluation");
        throw new RuntimeException(err);
      }
    }

    public Class<? extends OK> getKeyClass() {
      return OutKey.getBoxClass();
    }

    public Class<? extends OV> getValueClass() {
      return OutValue.getBoxClass();
    }

    public void call(String fn, Object key, Object value,
                                     TaskInputOutputContext<IK,IV,OK,OV> ctx) {
      Ctx = ctx;
      if (FirstTime) {
        LOG.info("calling " + fn + " for the first time");
      }

      try {
        Invoker.invokeFunction(fn, key, value, this);
      }
      catch (Throwable t) {
        LOG.warn(fn + " generated exception");
        throw new RuntimeException(t);
      }

      if (FirstTime) {
        FirstTime = false;
        LOG.info(fn + " completed successfully on the first call");
      }
    }

    public final void emit(Object key, Object value) {
      // emit() is concrete, because Jython didn't seem to pick up on the
      // override, and therefore called the parent function (is this part of
      // the scripting extension??). Making it concrete and then having it
      // call an override works fine. Hence, output().
      try {
        // nice to have these on separate lines for exception line # reporting
        OutKey.set(key);
        OutValue.set(value);
        Ctx.write(OutKey.getWritable(), OutValue.getWritable());
      }
      catch (IOException e) {
        LOG.error("Writing output generated an exception");
        throw new RuntimeException(e);
      }
      catch (InterruptedException e) {
        LOG.error("Writing output generated an exception");
        throw new RuntimeException(e);
      }

      Ctx.getCounter(Counters.OUTPUT_RECORDS).increment(1);
      Ctx.progress();
    }

    public static <IK,IV,OK extends Writable,OV extends Writable> PyEngine<IK,IV,OK,OV> setup(TaskInputOutputContext<IK,IV,OK,OV> context, String task) {
      final Configuration conf = context.getConfiguration();
      final ByteArrayInputStream byteStream = new ByteArrayInputStream(
        Base64.decode(conf.get("com.lbt.script." + task))
      );

      final PyEngine<IK,IV,OK,OV> py = new PyEngine<IK,IV,OK,OV>();

      Reader in = null;
      try {
        in = new InputStreamReader(byteStream);
        py.eval(in, conf.get("com.lbt.scriptName." + task));
        in.close();
      }
      catch (IOException ignore) {
      }
      finally {
        IOUtils.closeQuietly(in);
      }

      return py;
    }
  }

  public static class PythonMapper
                     extends Mapper<Text,FsEntry,WritableComparable,Writable> {

    private PyEngine<Text,FsEntry,WritableComparable,Writable> Python;

    protected void setup(Context context) {
      Python = PyEngine.setup(context, "map");
    }

    public void map(Text key, FsEntry value, Context context) throws IOException, InterruptedException {
      context.getCounter(Counters.INPUT_RECORDS).increment(1);
      context.progress();
      Python.call("mapper", key.toString(), value, context);
    }
  }

  public static class PythonReducer
     extends Reducer<WritableComparable,Writable,WritableComparable,Writable> {

    // my kingdom for a typedef
    private PyEngine<WritableComparable,Writable,WritableComparable,Writable> Python;

    @Override
    protected void setup(Context context) {
      Python = PyEngine.setup(context, "reduce");
    }

    @Override
    public void reduce(WritableComparable key, Iterable<Writable> values, Context context) throws IOException, InterruptedException {
      context.progress();
      Python.call("reducer", Python.getOutKey().unbox(key), values, context);
    }
  }

  static String convertScanToString(Scan scan) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dos = null;
    try {
      dos = new DataOutputStream(out);
      scan.write(dos);
      dos.close();
      return Base64.encodeBytes(out.toByteArray());
    }
    finally {
      IOUtils.closeQuietly(dos);
    }
  }

  static void configPyTask(Job job, PyEngine py, String task, String script)
                                                             throws Exception {
    final Configuration conf = job.getConfiguration();
    conf.set("com.lbt.scriptName." + task, script);
    conf.set("com.lbt.script." + task, Base64.encodeFromFile(script));

    Reader in = null;
    try {
      in = new BufferedReader(new FileReader(script));
      py.eval(in, script);
      in.close();
    }
    finally {
      IOUtils.closeQuietly(in);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 4) {
      System.err.println("Usage: PythonJob <table> <outpath> <python_mapper> <python_reducer> [SequenceFileOutputFormat]");
      System.exit(2);
    }
    Job job = new Job(conf, "PythonJob");
    job.setJarByClass(PythonJob.class);

    String mapper = otherArgs[2];
    job.setMapperClass(PythonMapper.class);
    PyEngine py = new PyEngine();
    configPyTask(job, py, "map", mapper);
    job.setMapOutputKeyClass(py.getKeyClass());
    job.setMapOutputValueClass(py.getValueClass());

    String reducer = otherArgs[3];
    int numReduces = 1;
    job.setOutputKeyClass(py.getKeyClass());
    job.setOutputValueClass(py.getValueClass());
    if (reducer.equals("none")) {
      numReduces = 0;
    }
    else if (reducer.equals("identity")) {
      job.setReducerClass(Reducer.class);
      job.setOutputKeyClass(py.getKeyClass());
      job.setOutputValueClass(py.getValueClass());
    }
    else if (reducer.equals("LongSumReducer")) {
      job.setReducerClass(LongSumReducer.class);
      job.setCombinerClass(LongSumReducer.class);
    }
    else {
      job.setReducerClass(PythonReducer.class);
      configPyTask(job, py, "reduce", reducer);
      job.setOutputKeyClass(py.getKeyClass());
      job.setOutputValueClass(py.getValueClass());
    }
    job.setNumReduceTasks(numReduces);

    String input = otherArgs[0];
    if (input.endsWith(".json") == true) {
      job.setInputFormatClass(FsEntryJsonInputFormat.class);
      FsEntryJsonInputFormat.addInputPath(job, new Path(input));
    }
    else {
      Scan scan = new Scan();
      scan.addFamily(Bytes.toBytes("core"));
      FsEntryHBaseInputFormat.setupConf(conf);
      job.getConfiguration().set(TableInputFormat.INPUT_TABLE, otherArgs[0]);
      job.getConfiguration().set(TableInputFormat.SCAN, convertScanToString(scan));
      job.setInputFormatClass(FsEntryHBaseInputFormat.class);
    }

    if (otherArgs.length == 5 && otherArgs[4].equals("SequenceFileOutputFormat")) {
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
    }
    else {
      job.setOutputFormatClass(TextOutputFormat.class);
    }
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
