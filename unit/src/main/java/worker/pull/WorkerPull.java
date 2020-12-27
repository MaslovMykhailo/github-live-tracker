package worker.pull;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import worker.*;
import interfaces.WorkerData;
import interfaces.WorkerStorage;
import interfaces.WorkerTarget;
import worker.configuration.WorkerConfiguration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class WorkerPull<T extends WorkerTarget> {

    private int currentWorkerIndex = 0;

    private final Map<String, Worker<T>> workers = new HashMap<>();

    private final Sinks.Many<Duration> executionRate = Sinks
        .many()
        .multicast()
        .onBackpressureBuffer();

    private WorkerPullLimits limits;

    public void setLimits(WorkerPullLimits limits) {
        this.limits = limits;
        emitExecutionRate();
    }

    public WorkerPull(WorkerPullLimits limits) {
        this.limits = limits;
    }

    private String keyword;

    public String getKeyword() {
        return keyword;
    }

    public Flux<Void> start(String keyword) {
        this.keyword = keyword;

        return executionRate
            .asFlux()
            .doOnSubscribe(subscription -> emitExecutionRate())
            .switchMap(duration -> Flux
                .interval(
                    workers.size() == 0 ? Duration.ZERO : duration,
                    duration
                )
            )
            .flatMap(interval -> {
                if (workers.isEmpty()) {
                    return Flux.empty();
                }

                return nextWorker().execute();
            });
    }

    public void stop() {
        keyword = null;
        currentWorkerIndex = 0;
        workers.clear();
    }

    public synchronized void addTrackingSource(
        String source,
        WorkerData<T> data,
        WorkerStorage<T> storage
    ) {
        if (workers.containsKey(source)) {
            return;
        }

        workers.put(source, new Worker<T>(
            data,
            storage,
            new WorkerConfiguration(keyword, source, 100))
        );
    }

    public synchronized void removeTrackingSource(String source) {
        if (!workers.containsKey(source)) {
            return;
        }

        workers.remove(source);
    }

    private Worker<T> nextWorker() {
        @SuppressWarnings("unchecked")
        Worker<T> worker = workers.values().toArray(Worker[]::new)[currentWorkerIndex];

        if (currentWorkerIndex == workers.values().size() - 1) {
            currentWorkerIndex = 0;
        } else {
            currentWorkerIndex++;
        }

        return worker;
    }

    private void emitExecutionRate() {
        executionRate.emitNext(
            limits.getExecutionRate(),
            Sinks.EmitFailureHandler.FAIL_FAST
        );
    }

}
