package com.couchbase.devguide;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;

/**
 * Example of Delete in Java for the Couchbase Developer Guide.
 */
public class Delete extends ConnectionBase {

    @Override
    protected void doWork() {
        String key = "javaDevguideExampleExpiration";
        //create content
        JsonObject content = JsonObject.create().put("some", "value");

        bucket.upsert(JsonDocument.create(key, content));

        LOGGER.info("Getting item back immediately");
        LOGGER.info(bucket.get(key).content());

        bucket.remove(key);
        LOGGER.info("Getting key again...");

        //get returns null if the key doesn't exist
        if (bucket.get(key) == null) {
            LOGGER.info("Get failed because item has been deleted");
        }

        bucket.upsert(JsonDocument.create(key, content));

        LOGGER.info("Getting item back immediately");
        JsonDocument rv = bucket.getAndTouch(key, 0);
        LOGGER.info("Value is:" + rv.content());

        bucket.remove(JsonDocument.create(key, content));

        LOGGER.info("Getting key again...");
        if (bucket.get(key) == null) {
            LOGGER.info("Failed (not found)");
        }

    }

    public static void main(String[] args) {
        new Delete().execute();
    }
}
