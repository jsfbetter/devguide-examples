package com.couchbase.devguide;

import java.util.concurrent.TimeUnit;
import java.util.Random;

import javax.sound.midi.Soundbank;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.JsonLongDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.document.json.JsonArray;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

/**
 * Example of Bulk Insert in Java for the Couchbase Developer Guide.
 */
public class BenchData extends ConnectionBase {

    private int loopTimes;
    private int bulkSize;

    public BenchData(int loopTimes, int bulkSize) {
        this.loopTimes = loopTimes;
        this.bulkSize = bulkSize;
    }
      public String genRandomNum(){
          int  maxNum = 36;
          int i;
          int count = 0;
          char[] str = { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
            'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
            'X', 'Y', 'Z','a','b','c','d','e','f','g','h','i','j',
            'k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
          StringBuffer pwd = new StringBuffer("");
          Random r = new Random();
          while(count < 16){
           i = Math.abs(r.nextInt(maxNum));
           if (i >= 0 && i < str.length) {
            pwd.append(str[i]);
            count ++;
           }
           if (count==8)
           {
               pwd.append(" ");
           }
          }
          return pwd.toString();
    }

    @Override
    protected void doWork() {
        for (int i = 0; i < this.loopTimes; i++) {
            final String key;
            if (i ==0) {
                key = "Brass Doorknob";
            } else {
                key = genRandomNum();
            }

            // Create a JSON document content
            final JsonObject content = JsonObject.create()
                            .put("name", JsonArray.from(key.split(" ")))
                            .put("email","brass.doorknob@juno.com")
                            .put("random",key.replace(" ",""));

            // Describe what we want to do asynchronously using RxJava Observables:

            Observable<JsonDocument> asyncProcessing = Observable
                    // Use RxJava range + map to generate 10 keys. One could also use "from" with a pre-existing collection of keys.
                    .range(0, this.bulkSize)
                    .map(new Func1<Integer, String>() {
                        public String call(Integer i) {
                            return key + "_" + i;
                        }
                    })
                    //then create a JsonDocument out each one of these keys
                    .map(new Func1<String, JsonDocument>() {
                        public JsonDocument call(String s) {
                            // return JsonDocument.create(s, getUnixEpochInSeconds(3600*24*2*1000), content);
                            return JsonDocument.create(s, content);
                        }
                    })
                    //now use flatMap to asynchronously call the SDK upsert operation on each
                    .flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
                        public Observable<JsonDocument> call(JsonDocument doc) {
                            // if (doc.id().endsWith("3"))
                            //     return bucket.async().upsert(doc).delay(3, TimeUnit.SECONDS); //artificial delay for item 3
                            return bucket.async().upsert(doc);
                        }
                    });

            // So far we've described and not triggered the processing, let's subscribe
            /*
             *  Note: since our app is not fully asynchronous, we want to revert back to blocking at the end,
             *  so we subscribe using toBlocking().
             *
             *  toBlocking will throw any exception that was propagated through the Observer's onError method.
             *
             *  The SDK is doing its own parallelisation so the blocking is just waiting for the last item,
             *  notice how our artificial delay doesn't impact printout of the other values, that come in the order
             *  in which the server answered...
             */
            try {
                asyncProcessing.toBlocking()
                    // we'll still printout each inserted document (with CAS gotten from the server)
                    // toBlocking() also offers several ways of getting one of the emitted values (first(), single(), last())
                    .forEach(new Action1<JsonDocument>() {
                        public void call(JsonDocument jsonDocument) {
                            // LOGGER.info("Inserted " + jsonDocument);
                        }
                    });
            } catch (Exception e) {
                LOGGER.error("Error during bulk insert", e);
            }
        }
    }

    public static void main(String[] args) {
        int loopTimes = Integer.valueOf(args[0]);
        int bulkSize = Integer.valueOf(args[1]);
        new BenchData(loopTimes, bulkSize).execute();
    }
}
