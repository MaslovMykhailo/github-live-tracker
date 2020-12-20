package interfaces;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import worker.WorkerConfiguration;

public interface WorkerStorage<T> {

    Mono<Void> store(Flux<T> target, WorkerConfiguration configuration);

    Flux<T> getLatest(WorkerConfiguration configuration);

}
