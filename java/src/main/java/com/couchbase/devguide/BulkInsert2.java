package com.couchbase.devguide;

import java.util.concurrent.TimeUnit;

import javax.sound.midi.Soundbank;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.couchbase.client.java.error.DocumentDoesNotExistException;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ConsoleReporter;

import com.couchbase.client.java.document.StringDocument;

/**
 * Example of Bulk Insert in Java for the Couchbase Developer Guide.
 */
public class BulkInsert2 extends ConnectionBase {

    @Override
    protected void doWork() {

        final MetricRegistry registry = new MetricRegistry();
        registry.counter("success");
        registry.counter("total");
        registry.counter("timeout");

        final ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
        reporter.start(30, TimeUnit.SECONDS);

        final String prefix = "cloudcl500";
        final String value = "78b6crtqefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец58foxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец58foxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец58foxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец58foxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец58foxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец58foxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<BлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшыисвртчой09йоцшущпыивруцтофчвтфыаовуралорпаг13655ке7134е5к784епув87йеа73пй37кугвцфорвч7гуряыш27164793еу78цвеа978уцесп78уцуеавпс7нцупвыфв7па78йуепывнфчгяспам8фынгавс78феп78цевп78йец78веый87цевы8пч6йцефыпавч678b6crtqefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшыисвртчой09йоцшущпыивруцтофчвтфыаовуралорпаг13655ке7134е5к784епув87йеа73пй37кугвцфорвч7гуряыш27164793еу78цвеа978уцесп78уцуеавпс7нцупвыфв7па78йуепывнфчгяспам8фынгавс78феп78цевп78йец78веый87цевы8пч6йцефыпавч678b6crtqefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшыисвртчой09йоцшущпыивруцтофчвтфыаовуралорпаг13655ке7134е5к784епув87йеа73пй37кугвцфорвч7гуряыш27164793еу78цвеа978уцесп78уцуеавпс7нцупвыфв7па78йуепывнфчгяспам8фынгавс78феп78цевп78йец78веый87цевы8пч6йцефыпавраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакраоыпвфывмтосрчгнвакфвыйацныгукец589йргыщшцторвуырфалдоофвзйшefoxgausH<Bлоывивафмпайвимвпафровераоыпвфывмтосрчгнвакч6";

        final int bulkSize = 10;
        final int loopTimes = 100;

        final int keystart = 0;
        final int keyend = 5000000;

        final int sleeptime = 4;
        final long timeout = 7;


        // Describe what we want to do asynchronously using RxJava Observables:
        registry.counter("total").inc(loopTimes * ((keyend - keystart) + bulkSize - (keyend - keystart) % bulkSize));

        for (int loop = 0; loop < loopTimes; loop++) {
            for (int i = keystart; i < keyend; i += bulkSize) {
                try {
                    Thread.sleep(sleeptime);

                    // Describe what we want to do asynchronously using RxJava Observables:

                    Observable<StringDocument> asyncProcessing = Observable
                            // Use RxJava range + map to generate 10 keys. One could also use "from" with a pre-existing collection of keys.
                            .range(i, i + bulkSize)
                            .map(new Func1<Integer, String>() {
                                public String call(Integer i) {
                                    return prefix + i;
                                }
                            })
                            //then create a StringDocument out each one of these keys
                            .map(new Func1<String, StringDocument>() {
                                public StringDocument call(String s) {
                                    return StringDocument.create(s, 3600, value);
                                }
                            })
                            //now use flatMap to asynchronously call the SDK upsert operation on each
                            .flatMap(new Func1<StringDocument, Observable<StringDocument>>() {
                                public Observable<StringDocument> call(StringDocument doc) {
                                    return bucket.async().upsert(doc);
                                }
                            })
                            .timeout(timeout, TimeUnit.MILLISECONDS)
                            .onErrorReturn(new Func1<Throwable, StringDocument>() {
                                @Override
                                public StringDocument call(Throwable throwable) {
                                    if (throwable instanceof TimeoutException) {
                                        return StringDocument.create("timeout", 3600, "timeout");
                                    }
                                    return StringDocument.create("exception", 3600, throwable.getMessage());
                                }
                            });

                    asyncProcessing.toBlocking()
                        // we'll still printout each inserted document (with CAS gotten from the server)
                        // toBlocking() also offers several ways of getting one of the emitted values (first(), single(), last())
                        .forEach(new Action1<StringDocument>() {
                            public void call(StringDocument stringDocument) {
                                if (stringDocument.id().equals("timeout")) {
                                    registry.counter("timeout").inc();
                                } else if (stringDocument.id().equals("exception") == false) {
                                    if (stringDocument.content().equals(value)) {
                                        System.out.println(stringDocument.content());
                                        registry.counter("success").inc();
                                    }
                                }
                            }
                        });
                } catch (Exception e) {
                    System.out.println("Error during bulk insert" + e);
                }
            }
        }
    }

    public static void main(String[] args) {
        new BulkInsert().execute();
    }
}
