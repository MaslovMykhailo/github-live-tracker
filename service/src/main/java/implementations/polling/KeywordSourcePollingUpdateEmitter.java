package implementations.polling;

import interfaces.KeywordSourceStorage;
import interfaces.KeywordSourceUpdateEmitter;
import model.KeywordSource;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public class KeywordSourcePollingUpdateEmitter implements KeywordSourceUpdateEmitter {

    private final KeywordSourceStorage storage;

    private final Set<KeywordSource> recordsCache = new HashSet<>();

    private final Duration pollingInterval;

    public KeywordSourcePollingUpdateEmitter(
        KeywordSourceStorage storage,
        Duration pollingInterval
    ) {
        this.storage = storage;
        this.pollingInterval = pollingInterval;
    }

    private Disposable updatesDisposable = Disposables.disposed();

    public void startListenUpdates() {
        updatesDisposable = Flux
            .interval(pollingInterval)
            .onBackpressureDrop()
            .flatMap(interval -> storage
                .getKeywordRecords()
                .collectList()
            )
            .map(HashSet::new)
            .doOnNext(records -> {
                difference(records, recordsCache)
                    .stream()
                    .parallel()
                    .forEach(record -> addedRecordsSink
                        .emitNext(record, Sinks.EmitFailureHandler.FAIL_FAST)
                    );

                difference(recordsCache, records)
                    .stream()
                    .parallel()
                    .forEach(record -> removedRecordsSink
                        .emitNext(record, Sinks.EmitFailureHandler.FAIL_FAST)
                    );

                recordsCache.clear();
                recordsCache.addAll(records);
            })
            .publishOn(Schedulers.parallel())
            .subscribe();
    }

    public void stopListenUpdates() {
        if (!updatesDisposable.isDisposed()) {
            updatesDisposable.dispose();
        }
    }

    private Sinks.Many<KeywordSource> createRecordsSink() {
        return Sinks
            .many()
            .multicast()
            .onBackpressureBuffer();
    }

    private final Sinks.Many<KeywordSource> addedRecordsSink = createRecordsSink();

    public Flux<KeywordSource> addedKeywordSource() {
        return addedRecordsSink.asFlux();
    }

    private final Sinks.Many<KeywordSource> removedRecordsSink = createRecordsSink();

    public Flux<KeywordSource> removedKeywordSource() {
        return removedRecordsSink.asFlux();
    }

    private static <T> Set<T> difference(Set<T> left, Set<T> right) {
        Set<T> difference = new HashSet<>(left);
        difference.addAll(right);
        difference.removeAll(right);
        return difference;
    }

}
