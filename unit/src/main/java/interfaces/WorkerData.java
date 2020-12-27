package interfaces;

import reactor.core.publisher.Flux;
import worker.configuration.WorkerConfiguration;

public interface WorkerData<T> {

    Flux<T> request(WorkerConfiguration configuration);

}
