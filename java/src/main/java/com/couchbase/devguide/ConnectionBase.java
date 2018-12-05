package com.couchbase.devguide;

import java.util.Arrays;
import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import org.apache.log4j.Logger;

import com.couchbase.client.core.tracing.ThresholdLogReporter;
import com.couchbase.client.core.tracing.ThresholdLogTracer;
import io.opentracing.Tracer;
import java.util.concurrent.TimeUnit;

public class ConnectionBase {

    protected static final Logger LOGGER = Logger.getLogger("devguide");

    protected final Cluster cluster;
    protected final Bucket bucket;

    //=== EDIT THESE TO ADAPT TO YOUR COUCHBASE INSTALLATION ===
    public static final String bucketName = "default";
    public static final String bucketPassword = "";
    public static final List<String> nodes = Arrays.asList("127.0.0.1");
    
    // for more about tracing
    // https://docs.couchbase.com/java-sdk/2.7/tracing-from-the-sdk.html
    // https://docs.couchbase.com/java-sdk/2.7/start-using-sdk.html
    // https://blog.couchbase.com/response-time-observability-with-the-java-sdk/
    // We shall set them so low that almost everything gets logged - not something you should do in production!
    
    public static Tracer tracer = ThresholdLogTracer.create(ThresholdLogReporter.builder()
              .kvThreshold(1000, TimeUnit.MICROSECONDS) // 1 microsecond
              .logInterval(5, TimeUnit.SECONDS) // log every second
              .sampleSize(Integer.MAX_VALUE)
              .pretty(true) // pretty print the json output in the logs
              .build());

    public static CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.builder()
            .tracer(tracer)
            .build();
    // public static CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.create();

    public ConnectionBase() {
        //connect to the cluster by hitting one of the given nodes
        cluster = CouchbaseCluster.create(environment, nodes);
        //get a Bucket reference from the cluster to the configured bucket
        bucket = cluster.openBucket(bucketName, bucketPassword);
    }

    private void disconnect() {
        //release shared resources and close all open buckets
        cluster.disconnect();
    }

    public void execute() {
        //connection has been done in the constructor
        doWork();
        disconnect();
    }

    /**
     * Override this method to showcase specific examples.
     * Make them executable by adding a main method calling new ExampleClass().execute();
     */
    protected void doWork() {
        //this one just showcases connection methods, see constructor and shutdown()
        LOGGER.info("Connected to the cluster, opened bucket " + bucketName);
    }

    public static void main(String[] args) {
        new ConnectionBase().execute();
    }
}
