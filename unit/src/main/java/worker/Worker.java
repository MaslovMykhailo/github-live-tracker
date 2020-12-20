package worker;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import interfaces.WorkerData;
import interfaces.WorkerStorage;
import interfaces.WorkerTarget;

import java.time.Duration;
import java.util.List;

public class Worker<T extends WorkerTarget> {

    private final WorkerData<T> data;

    private final WorkerStorage<T> storage;

    private final WorkerConfiguration configuration;

    public Worker(
        WorkerData<T> data,
        WorkerStorage<T> storage,
        WorkerConfiguration configuration
    ) {
        this.data = data;
        this.storage = storage;
        this.configuration = configuration;

        persist();
    }

    private boolean execution = false;

    private boolean cancelled = false;

    public Flux<T> execute() {
        return loadLatestDataIdentityList()
            .flatMapMany(latest -> data
                .request(configuration)
                .filter(target -> !latest.contains(target.getIdentity()))
            )
            .doOnNext(target -> {
                storageSink.emitNext(
                    target,
                    Sinks.EmitFailureHandler.FAIL_FAST
                );
            })
            .concatWith(!execution ?
                storage.getLatest(configuration) :
                Flux.empty()
            )
            .doOnNext(target -> {
                execution = true;
            });
    }

    public void cancel() {
        cancelled = true;
    }

    private Mono<List<String>> loadLatestDataIdentityList() {
        return storage
            .getLatest(configuration)
            .map(WorkerTarget::getIdentity)
            .collectList();
    }

    private Disposable storageDisposable = Disposables.disposed();

    private final Sinks.Many<T> storageSink = Sinks
        .many()
        .unicast()
        .onBackpressureBuffer();

    private void persist() {
        storageDisposable = storageSink
            .asFlux()
            .windowTimeout(configuration.getPageSize(), Duration.ofSeconds(10))
            .onBackpressureBuffer()
            .flatMap(target -> storage.store(target, configuration))
            .doOnNext(next -> {
                if (cancelled) {
                    storageDisposable.dispose();
                }
            })
            .subscribe();
    }

}
