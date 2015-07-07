package com.martinkl;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * To run this:
 * 
 * <pre>
 * mvn package
 * hadoop jar target/elastic-test-0.0.1-mapreduce.jar com.martinkl.BulkLoad
 * bin/hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /tmp/hbase-bulkload docs
 * </pre>
 */
public class BulkLoad {

    public static final String INPUT_PATH = "/Users/martin/gutenberg/*.txt";
    public static final String STAGING_PATH = "/tmp/hbase-staging";
    public static final String OUTPUT_PATH = "/tmp/hbase-bulkload";
    public static final String HBASE_TABLE_NAME = "docs";
    public static final byte[] HBASE_COL_FAMILY = "doc".getBytes();
    public static final byte[] HBASE_COL_NAME = "text".getBytes();
    public static final int TARGET_DOC_SIZE = 10000;

    public static class BulkLoadMapper
        extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

        private StringBuilder buf = new StringBuilder();
        private Random random = new Random();

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            buf.append(value);
            buf.append("\n");
            if (buf.length() >= TARGET_DOC_SIZE) emit(context);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            emit(context);
        }

        private void emit(Context context) throws IOException, InterruptedException {
            ImmutableBytesWritable key = new ImmutableBytesWritable();
            key.set(new BigInteger(128, random).toString(16).getBytes("UTF-8"));
            byte[] value = buf.toString().getBytes("UTF-8");
            KeyValue kv = new KeyValue(key.get(), HBASE_COL_FAMILY, HBASE_COL_NAME, value);
            context.write(key, kv);
            buf = new StringBuilder();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.table.name", HBASE_TABLE_NAME);
        conf.set("hbase.fs.tmp.dir", STAGING_PATH);
        conf.set("hbase.bulkload.staging.dir", STAGING_PATH);
        HBaseConfiguration.addHbaseResources(conf);

        Job job = new Job(conf);
        job.setJarByClass(BulkLoad.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(BulkLoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        HTable table = new HTable(conf, HBASE_TABLE_NAME);
        HFileOutputFormat.configureIncrementalLoad(job, table);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.waitForCompletion(true);
    }
}
