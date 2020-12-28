import cache.IdledSinksCache;
import cache.KeywordSinksCache;
import emitter.KeywordSourceUpdateEmitter;
import implementations.mssql.MssqlKeywordSourceStorage;
import io.r2dbc.mssql.MssqlConnection;
import io.r2dbc.mssql.MssqlConnectionConfiguration;
import io.r2dbc.mssql.MssqlConnectionFactory;
import io.rsocket.Payload;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import limits.LimitsHolder;
import metadata.UnitPayloadMetadata;
import model.KeywordSource;
import org.reactivestreams.Publisher;
import payload.ServicePayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;

public class Service {

    public static void main(String... args) {
        String host = "localhost";
        int port = 7000;

        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration
            .builder()
            .build();

        new Service(host, port, configuration).run();
    }

    private final RetryBackoffSpec retrySpec = Retry
        .backoff(Long.MAX_VALUE, Duration.ofMillis(1))
        .maxBackoff(Duration.ofSeconds(10));

    public Service(String host, int port, MssqlConnectionConfiguration mssqlConfiguration) {
        serverTransport = TcpServerTransport.create(host, port);

        MssqlConnection mssqlConnection = new MssqlConnectionFactory(mssqlConfiguration)
            .create()
            .retryWhen(retrySpec)
            .block();

        updateEmitter = new KeywordSourceUpdateEmitter(
            new MssqlKeywordSourceStorage(mssqlConnection),
            Duration.ofSeconds(10)
        );
    }

    private final Logger logger = Loggers.getLogger(Service.class);

    private final LimitsHolder limits = new LimitsHolder();

    private final TcpServerTransport serverTransport;

    private final KeywordSourceUpdateEmitter updateEmitter;

    public void run() {
        logger.info("Service running...");

        RSocketServer
            .create(SocketAcceptor.forRequestChannel(this::onRequestChannel))
            .bind(serverTransport)
            .retryWhen(retrySpec)
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
            .then()
            .block();
    }

    private Sinks.Many<Payload> createKeywordSink() {
        return Sinks.many().unicast().onBackpressureBuffer();
    }

    private final IdledSinksCache idledKeywordSinks = new IdledSinksCache();

    private Flux<Payload> onRequestChannel(Publisher<Payload> payloadPublisher) {
        return Flux
            .from(payloadPublisher)
            .flatMap(payload -> {
                if (UnitPayloadMetadata.UnitInit.equals(payload.getMetadataUtf8())) {
                    logger.info("Unit init");
                    return idledKeywordSinks
                        .addIdledSink(createKeywordSink())
                        .asFlux();
                }

                if (UnitPayloadMetadata.UnitError.equals(payload.getMetadataUtf8())) {
                    KeywordSource keywordSource = KeywordSource.fromJSON(payload.getDataUtf8());
                    logger.info("Unit error " + keywordSource);
                    onUnsubscribeKeywordSource(keywordSource);
                }

                return Flux.empty();
            })
            .publishOn(Schedulers.parallel())
            .doFirst(updateEmitter::startListenUpdates);
    }

    private final KeywordSinksCache keywordSinks = new KeywordSinksCache();

    private void onSubscribeKeywordSource(KeywordSource record) {
        logger.info("Request on subscribe to " + record);

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

            logger.info("Start tracking of " + record);

            keywordSink.emitNext(
                ServicePayload.createUpdateLimitsPayload(limits.increment().toJSON()),
                Sinks.EmitFailureHandler.FAIL_FAST
            );

            logger.info("Limits increased");
        } else {
            keywordSink = cacheRecord.sink;
        }

        if (!cacheRecord.sources.contains(record.source)) {
            cacheRecord.sources.add(record.source);
            keywordSink.emitNext(
                ServicePayload.createStartTrackSourcePayload(record.source),
                Sinks.EmitFailureHandler.FAIL_FAST
            );

            logger.info("Subscribed to " + record);
        }
    }

    private void onUnsubscribeKeywordSource(KeywordSource record) {
        logger.info("Request on unsubscribe from " + record);

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

            logger.info("Unsubscribed from " + record);
        }

        if (cacheRecord.sources.isEmpty()) {
            keywordSinks.removeKeywordSink(record.word);
            idledKeywordSinks.addIdledSink(keywordSink);

            keywordSink.emitNext(
                ServicePayload.createStopTrackKeywordPayload(record.word),
                Sinks.EmitFailureHandler.FAIL_FAST
            );

            logger.info("Stop tracking of " + record.word);

            keywordSink.emitNext(
                ServicePayload.createUpdateLimitsPayload(limits.decrement().toJSON()),
                Sinks.EmitFailureHandler.FAIL_FAST
            );

            logger.info("Limits decreased");
        }
    }

}
