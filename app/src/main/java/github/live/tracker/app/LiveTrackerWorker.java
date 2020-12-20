package github.live.tracker.app;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;

public class LiveTrackerWorker {

    private final Sinks.Many<Long> pollingIntervalSink = Sinks
        .many()
        .replay()
        .latestOrDefault(1000 * 30L);

    private final HttpClient client;

    public LiveTrackerWorker(HttpClient client) {
        this.client = client;
    }

    public Disposable start() {

        return pollingIntervalSink
            .asFlux()
            .switchMap(millis -> Flux.interval(Duration.ofMillis(0), Duration.ofMillis(millis)))
            .onBackpressureDrop()
            .map(interval -> client
                .get()
                .uri("https://api.github.com/search/code?q=addClass+org:jquery+s:indexed+o:desc")
                .responseContent()
                .aggregate()
                .asString()
            )
            .doOnNext(data -> {
                System.out.println(System.currentTimeMillis());
            })
            .subscribeOn(Schedulers.parallel())
            .subscribe();

        // start polling
        // when data retrieved write it into storage in non blocking manner
        // retrieved data should be filtered in order to store and sink only new data

    }

    public void updateLimits() {
        // update api limits and change polling interval

        new Thread(() -> {
            Sinks.EmitResult emitResult;
            do {
                emitResult = pollingIntervalSink.tryEmitNext(1000 * 60L);
            } while (emitResult.isFailure());
        }).start();

    }

}
