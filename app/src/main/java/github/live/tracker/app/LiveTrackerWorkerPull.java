package github.live.tracker.app;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.LinkedList;

public class LiveTrackerWorkerPull {

    private int currentWorkerIndex = 0;

    private final LinkedList workers = new LinkedList<Object>();

    private WorkerPullConfiguration configuration;

    public WorkerPullConfiguration getConfiguration() {
        return configuration;
    }

    private final Sinks.Many<Duration> executionRate = Sinks
        .many()
        .multicast()
        .onBackpressureBuffer();

    public Flux<Object> start(String keyword, WorkerPullLimits limits) {
        this.configuration = new WorkerPullConfiguration(keyword, limits);

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
                    return Mono.empty();
                }

//                return nextWorker().execute()

                return Flux.just();
            });
    }

    public synchronized void addTrackingSource(String source) {
//        workers.add()
    }

    public synchronized void removeTrackingSource() {

    }

    private void emitExecutionRate() {
        executionRate.emitNext(
            configuration.getLimits().getExecutionRate(),
            Sinks.EmitFailureHandler.FAIL_FAST
        );
    }

    public void updateLimits(WorkerPullLimits limits) {
        configuration.setLimits(limits);
        emitExecutionRate();
    }

    private Object nextWorker() {
        Object worker = workers.get(currentWorkerIndex);

        if (currentWorkerIndex == workers.size() - 1) {
            currentWorkerIndex = 0;
        } else {
            currentWorkerIndex++;
        }

        return worker;
    }

}
