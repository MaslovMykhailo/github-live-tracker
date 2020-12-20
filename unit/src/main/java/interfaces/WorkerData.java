package interfaces;

import reactor.core.publisher.Flux;
import worker.WorkerConfiguration;

public interface WorkerData<T> {

    Flux<T> request(WorkerConfiguration configuration);

}
