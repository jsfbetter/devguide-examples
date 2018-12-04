package com.couchbase.devguide;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.x;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.JsonLongDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.couchbase.client.java.query.dsl.functions.MetaFunctions;

/**
 * Example of N1QL Query Consistency in Java for the Couchbase Developer Guide.
 */
public class BenchQuery extends ConnectionBase {

    private int loopTimes;
    private String index;
    private int bulkSize;
    private ScanConsistency consistency;
    private int timeout;

    public static final MetricRegistry registry = new MetricRegistry();
    public static final ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

    public BenchQuery(int loopTimes, int bulkSize, int timeout, String index, String consistency) {
        super();
        this.loopTimes = loopTimes;
        this.timeout = timeout;
        this.bulkSize = bulkSize;
        this.index = index;
        switch(consistency) {
            case "NOT_BOUNDED":
                this.consistency = ScanConsistency.REQUEST_PLUS;
                break;
            case "STATEMENT_PLUS":
                this.consistency = ScanConsistency.STATEMENT_PLUS;
                break;
            default:
                this.consistency = ScanConsistency.NOT_BOUNDED;
        }
        registry.timer("timer");
        registry.counter("success");
        registry.counter("total");
        registry.counter("timeout");

        reporter.start(30, TimeUnit.SECONDS);
    }

    @Override
    protected void doWork() {
        // String key = "Brass Doorknob";

        LOGGER.info("Please ensure there is a primary index on the default bucket!");
        // Random random = new Random();
        // int randomNumber = random.nextInt(10000000);

        ////prepare the random user
        //JsonObject user = JsonObject.create()
        //        .put("name", JsonArray.from("Brass", "Doorknob"))
        //        .put("email", "brass.doorknob@juno.com")
        //        .put("random", randomNumber);
        ////upsert it with the corresponding random key
        //JsonDocument doc = JsonDocument.create(key, user); //the same document will be updated with a random internal value
        //bucket.upsert(doc);

        //immediately query N1QL (note we imported relevant static methods)
        //prepare the statement
        Statement statement = select("name", "email", "random", "META().id")
                .from("`n1ql-test`")
                .useIndex(this.index)
                .where(x("$1").in("name"));

        //configure the query
        N1qlParams params = N1qlParams.build()
                //If this line is removed, the latest 'random' field might not be present
                .consistency(this.consistency)
                .serverSideTimeout(this.timeout,TimeUnit.MILLISECONDS);

        final Timer timer = this.registry.timer("timer");

        this.registry.counter("total").inc(this.bulkSize*loopTimes);

        for (int loop = 0; loop < this.loopTimes; loop++) {
            N1qlQuery query = N1qlQuery.parameterized(statement, JsonArray.from("Brass"), params);

            // LOGGER.info("Expecting random: " + randomNumber);
            N1qlQueryResult result;
            try (@SuppressWarnings("unused") Timer.Context context = timer.time()) {
                result = bucket.query(query);
            }
            if (!result.finalSuccess() || result.allRows().isEmpty()) {
                if (result.errors().toString().contains("Timeout")) {
                    System.out.println(result.errors().toString());
                    this.registry.counter("timeout").inc(this.bulkSize);
                    continue;
                }
            }

            for (N1qlQueryRow queryRow : result) {
                JsonObject row = queryRow.value();
                String rowRandom = row.getString("random");
                String rowName = row.getArray("name").toString().replace("[\"","").replace("\",\"","").replace("\"]","");
                String rowId = row.getString("id");

                // LOGGER.info("Doc Id: " + rowId  + ", Name: " + rowName + ", Email: " + row.getString("email")
                //     + ", Random: " + rowRandom);

                if (rowRandom.equals(rowName)) {
                    // LOGGER.info("!!! Found our newly inserted document !!!");
                    this.registry.counter("success").inc();
                } else {
                    LOGGER.warn("Found a different random value : " + rowRandom);
                }

                // if (System.getProperty("REMOVE_DOORKNOBS") != null || System.getenv("REMOVE_DOORKNOBS") != null) {
                //     bucket.remove(rowId);
                // }
            }
        }
    }

    public static void main(String[] args) {
        final int loopTimes = Integer.valueOf(args[0]);
        final int bulkSize = Integer.valueOf(args[1]);
        final int timeout = Integer.valueOf(args[2]);
        final String index = args[3];
        final String consistency = args[4];
        new BenchQuery(loopTimes, bulkSize, timeout, index, consistency).execute();
    }
}
