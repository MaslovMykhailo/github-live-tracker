package interfaces;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import worker.configuration.WorkerConfiguration;

public interface WorkerStorage<T> {

    Mono<Void> store(Flux<T> target, WorkerConfiguration configuration);

    Flux<T> getLatest(WorkerConfiguration configuration);

    Flux<String> getLatestIdentity(WorkerConfiguration configuration);

}
