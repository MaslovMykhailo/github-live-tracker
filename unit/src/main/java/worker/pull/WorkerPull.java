package worker.pull;

import interfaces.WorkerData;
import interfaces.WorkerStorage;
import interfaces.WorkerTarget;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import worker.Worker;
import worker.configuration.WorkerConfiguration;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Function;

public class WorkerPull<T extends WorkerTarget> {

    private final WorkersQueue queue = new WorkersQueue();

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
            .switchMap((Function<Duration, Publisher<? extends Long>>) Flux::interval)
            .flatMap(interval -> {
                if (queue.isEmpty()) {
                    return Flux.empty();
                }

                return queue.nextSourceWorker().execute();
            });
    }

    public void stop() {
        keyword = null;
        queue.clear();
    }

    public void addTrackingSource(
        String source,
        WorkerData<T> data,
        WorkerStorage<T> storage
    ) {
        WorkerConfiguration configuration = new WorkerConfiguration(
            keyword,
            source,
            100
        );

        queue.addSourceWorker(
            source,
            new Worker<>(data, storage, configuration)
        );
    }

    public void removeTrackingSource(String source) {
        queue.removeSourceWorker(source);
    }

    private void emitExecutionRate() {
        executionRate.emitNext(
            limits.getExecutionRate(),
            Sinks.EmitFailureHandler.FAIL_FAST
        );
    }

    private class WorkersQueue {

        private final Map<String, Worker<T>> workers = new HashMap<>();

        private final LinkedList<String> sourceQueue = new LinkedList<>();

        public synchronized void addSourceWorker(String source, Worker<T> worker) {
            if (workers.containsKey(source)) {
                return;
            }

            workers.put(source, worker);
            sourceQueue.add(source);
        }

        public synchronized void removeSourceWorker(String source) {
            if (!workers.containsKey(source)) {
                return;
            }

            workers.remove(source);
            sourceQueue.remove(source);
        }

        public synchronized Worker<T> nextSourceWorker() {
            String nextSource = sourceQueue.poll();
            sourceQueue.add(nextSource);
            return workers.get(nextSource);
        }

        public synchronized void clear() {
            workers.clear();
            sourceQueue.clear();
        }

        public synchronized boolean isEmpty() {
            return workers.isEmpty();
        }

    }

}
