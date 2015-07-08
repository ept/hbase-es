package com.martinkl;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * To run this:
 * 
 * <pre>
 * mvn package
 * hadoop jar target/elastic-test-0.0.1-mapreduce.jar com.martinkl.BulkIndex
 * </pre>
 */
public class BulkIndex {

    public static final String ES_NODES = "localhost:9200";
    public static final String ES_RESOURCE = "docs/doc";

    public static class HBaseTableMapper extends TableMapper<NullWritable, Text> {
        private static final String ID_FIELD = "id";
        private static final String TEXT_FIELD = "text";
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context)
                throws IOException, InterruptedException {
            HashMap<String, String> json = new HashMap<>();
            json.put(ID_FIELD, new String(key.get()));
            json.put(TEXT_FIELD, new String(value.getValue(BulkLoad.HBASE_COL_FAMILY, BulkLoad.HBASE_COL_NAME)));
            context.write(NullWritable.get(), new Text(mapper.writeValueAsBytes(json)));
        }
    }

    public static void main(String[] args) throws Exception {
        new JobConf().setSpeculativeExecution(false);
        Configuration conf = new Configuration();
        conf.set("es.nodes", ES_NODES);
        conf.set("es.resource", ES_RESOURCE);
        conf.set("es.mapping.id", HBaseTableMapper.ID_FIELD.toString());
        conf.set("es.batch.size.bytes", "10mb");
        conf.set("es.batch.size.entries", "10000");
        conf.set("es.batch.write.refresh", "false");

        Job job = new Job(conf);
        job.setJarByClass(BulkIndex.class);
        job.setMapperClass(HBaseTableMapper.class);
        job.setNumReduceTasks(0);
        job.setSpeculativeExecution(false);
        job.setOutputFormatClass(BulkProcessorOutputFormat.class);
        job.setMapOutputValueClass(Text.class); 

        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(BulkLoad.HBASE_TABLE_NAME, scan,
            HBaseTableMapper.class, NullWritable.class, MapWritable.class, job);

        job.waitForCompletion(true);
    }
}
