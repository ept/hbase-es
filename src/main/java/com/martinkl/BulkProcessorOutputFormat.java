package com.martinkl;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class BulkProcessorOutputFormat extends OutputFormat<NullWritable, Text> {

    @Override
    public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new BulkProcessorRecordWriter(context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new BulkProcessorOutputCommitter();
    }


    public enum Counters {
        DOCUMENTS_ENQUEUED, DOCUMENTS_INDEXED, BULK_REQUEST_STARTED, BULK_REQUEST_SUCCESS, BULK_REQUEST_FAILURE,
        REQUEST_TIME_MS
    }

    static class BulkProcessorOutputCommitter extends OutputCommitter {
        @Override
        public void setupJob(JobContext jobContext) throws IOException {}

        @Override
        public void setupTask(TaskAttemptContext taskContext) throws IOException {}

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
            return false;
        }

        @Override
        public void commitTask(TaskAttemptContext taskContext) throws IOException {}

        @Override
        public void abortTask(TaskAttemptContext taskContext) throws IOException {
        }
    }


    static class BulkProcessorRecordWriter extends RecordWriter<NullWritable, Text> implements Listener {
        private final TaskAttemptContext context;
        private final Node node;
        private final BulkProcessor processor;

        public BulkProcessorRecordWriter(TaskAttemptContext context) {
            this.context = context;
            this.node = NodeBuilder.nodeBuilder().clusterName("arachnys").node();
            this.processor = BulkProcessor
                    .builder(node.client(), this)
                    .setBulkActions(10000)
                    .setBulkSize(new ByteSizeValue(100, ByteSizeUnit.MB))
                    .setConcurrentRequests(1)
                    .build();
        }

        @Override
        public void write(NullWritable key, Text value) throws IOException, InterruptedException {
            IndexRequest request = new IndexRequest("docs", "doc").source(value.getBytes());
            processor.add(request);
            context.getCounter(Counters.DOCUMENTS_ENQUEUED).increment(1);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            processor.close();
            node.close();
        }

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            context.getCounter(Counters.BULK_REQUEST_STARTED).increment(1);
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            context.getCounter(Counters.BULK_REQUEST_SUCCESS).increment(1);
            context.getCounter(Counters.REQUEST_TIME_MS).increment(response.getTookInMillis());
            context.getCounter(Counters.DOCUMENTS_INDEXED).increment(request.numberOfActions());
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            context.getCounter(Counters.BULK_REQUEST_FAILURE).increment(1);
        }
    }
}
