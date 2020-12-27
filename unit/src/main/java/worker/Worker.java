package worker;

import interfaces.WorkerData;
import interfaces.WorkerStorage;
import interfaces.WorkerTarget;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import worker.configuration.WorkerConfiguration;

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
    }

    public Mono<Void> execute() {
        Flux<T> targets = loadLatestDataIdentityList()
            .flatMapMany(latest -> data
                .request(configuration)
                .filter(target -> !latest.contains(target.getIdentity()))
            )
            .onBackpressureBuffer();

        return persist(targets);
    }

    private Mono<List<String>> loadLatestDataIdentityList() {
        return storage
            .getLatestIdentity(configuration)
            .collectList();
    }

    private Mono<Void> persist(Flux<T> targets) {
        return targets
            .windowTimeout(configuration.getPageSize(), Duration.ofSeconds(10))
            .onBackpressureBuffer()
            .flatMap(target -> storage.store(target, configuration))
            .then();
    }

}
