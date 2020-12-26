import cache.IdledSinksCache;
import cache.KeywordSinksCache;
import emitter.KeywordSourceUpdateEmitter;
import github.live.tracker.payload.ServicePayload;
import github.live.tracker.payload.metadata.UnitPayloadMetadata;
import implementations.mssql.MssqlKeywordSourceStorage;
import io.r2dbc.mssql.MssqlConnection;
import io.r2dbc.mssql.MssqlConnectionConfiguration;
import io.r2dbc.mssql.MssqlConnectionFactory;
import io.rsocket.Payload;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import model.KeywordSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

public class Service {

    public static void main(String... args) {
        new Service().run();
    }

    private void run() {
        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .build();

        MssqlConnectionFactory factory = new MssqlConnectionFactory(configuration);

        MssqlConnection mssqlConnection = factory
            .create()
            .block();

        KeywordSourceUpdateEmitter updateEmitter = new KeywordSourceUpdateEmitter(
            new MssqlKeywordSourceStorage(mssqlConnection),
            Duration.ofSeconds(10)
        );

        RSocketServer
            .create(SocketAcceptor.forRequestChannel(this::onRequestChannel))
            .bind(TcpServerTransport.create("localhost", 7000))
            .subscribe();

        Flux
            .merge(
                updateEmitter
                    .addedKeywordSource()
                    .doOnNext(this::onSubscribeKeywordSource),
                updateEmitter
                    .removedKeywordSource()
                    .doOnNext(this::onUnsubscribeKeywordSource)
            )
            .publishOn(Schedulers.parallel())
            .doOnSubscribe(subscription -> updateEmitter.startListenUpdates())
            .then()
            .block();
    }

    private Sinks.Many<Payload> createKeywordSink() {
        return Sinks.many().unicast().onBackpressureBuffer();
    }

    private final IdledSinksCache idledKeywordSinks = new IdledSinksCache();

    private final KeywordSinksCache keywordSinks = new KeywordSinksCache();

    private Flux<Payload> onRequestChannel(Publisher<Payload> payloadPublisher) {
        return Flux
            .from(payloadPublisher)
            .flatMap(payload -> {
                if (UnitPayloadMetadata.UnitInit.equals(payload.getMetadataUtf8())) {
                    return idledKeywordSinks.addIdledSink(createKeywordSink()).asFlux();
                }

                return Flux.empty();
            })
            .publishOn(Schedulers.parallel());
    }

    private void onSubscribeKeywordSource(KeywordSource record) {
        Sinks.Many<Payload> keywordSink;
        KeywordSinksCache.Record cacheRecord = keywordSinks.getKeywordSink(record.word);

        if (cacheRecord == null) {
            keywordSink = idledKeywordSinks.pollIdledSink();

            if (keywordSink == null) {
                return;
            }

            cacheRecord = keywordSinks.putKeywordSink(record.word, keywordSink);
            keywordSink.emitNext(
                ServicePayload.createStartTrackKeywordPayload(record.word),
                Sinks.EmitFailureHandler.FAIL_FAST
            );
        } else {
            keywordSink = cacheRecord.sink;
        }

        if (!cacheRecord.sources.contains(record.source)) {
            cacheRecord.sources.add(record.source);
            keywordSink.emitNext(
                ServicePayload.createStartTrackSourcePayload(record.source),
                Sinks.EmitFailureHandler.FAIL_FAST
            );
        }
    }

    private void onUnsubscribeKeywordSource(KeywordSource record) {
        Sinks.Many<Payload> keywordSink;
        KeywordSinksCache.Record cacheRecord = keywordSinks.getKeywordSink(record.word);

        if (cacheRecord == null) {
            return;
        }

        keywordSink = cacheRecord.sink;

        if (cacheRecord.sources.contains(record.source)) {
            cacheRecord.sources.remove(record.source);

            keywordSink.emitNext(
                ServicePayload.createStopTrackSourcePayload(record.source),
                Sinks.EmitFailureHandler.FAIL_FAST
            );
        }

        if (cacheRecord.sources.isEmpty()) {
            keywordSinks.removeKeywordSink(record.word);
            idledKeywordSinks.addIdledSink(keywordSink);

            keywordSink.emitNext(
                ServicePayload.createStopTrackKeywordPayload(record.word),
                Sinks.EmitFailureHandler.FAIL_FAST
            );
        }
    }

}
