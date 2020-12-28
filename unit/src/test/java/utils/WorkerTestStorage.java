package utils;

import interfaces.WorkerStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import worker.configuration.WorkerConfiguration;

import java.util.ArrayList;
import java.util.List;

public class WorkerTestStorage {

    public List<WorkerConfiguration> storeConfigurationHistory = new ArrayList<>();

    public List<List<WorkerTestTarget>> storeTargetHistory = new ArrayList<>();

    public WorkerStorage<WorkerTestTarget> create() {
        return new WorkerStorage<>() {

            public Mono<Void> store(Flux<WorkerTestTarget> target, WorkerConfiguration configuration) {
                storeConfigurationHistory.add(configuration);
                List<WorkerTestTarget> targetHistory = new ArrayList<>();
                return target
                    .doOnNext(targetHistory::add)
                    .doFinally(signal -> storeTargetHistory.add(targetHistory))
                    .then();
            }

            public Flux<String> getLatestIdentity(WorkerConfiguration configuration) {
                return Flux.empty();
            }

        };
    }

}