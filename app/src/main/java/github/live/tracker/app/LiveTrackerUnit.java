package github.live.tracker.app;

import github.live.tracker.payload.UnitPayload;
import github.live.tracker.payload.metadata.ServicePayloadMetadata;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Sinks;

import java.time.Duration;

public class LiveTrackerUnit {

    public void main() {

        // connect to the master server
        // wait for keyword and source for polling
        // if limits are not exceeded delegate new worker creation
        // update api limits before new worker creation
        // do not create new worker if keyword already have been polling by existed worker
        // send new data to master server
        // remove worker

        String host = "localhost";
        int port = 7000;

        TcpClientTransport transport = TcpClientTransport.create(host, port);
        RSocket rSocket = RSocketConnector.connectWith(transport).block();

        rSocket
            .requestChannel(unitLifeCycle.asFlux())
            .doOnSubscribe(this::init)
            .doOnNext(this::handleChannelResponse)
            .doFinally(signal -> rSocket.dispose())
            .subscribe();

    }

    private final Sinks.Many<Payload> unitLifeCycle = Sinks
        .many()
        .multicast()
        .onBackpressureBuffer();

    LiveTrackerWorkerPull workerPull = new LiveTrackerWorkerPull();

    private void handleChannelResponse(Payload payload) {
        switch (payload.getMetadataUtf8()) {
            case ServicePayloadMetadata.StartTrackKeyword -> {
                startTrackKeyword(payload.getDataUtf8());
            }
            case ServicePayloadMetadata.StopTrackKeyword -> {
                stopTrackKeyword();
            }
            case ServicePayloadMetadata.StartTrackSource -> {
                startTrackSource(payload.getDataUtf8());
            }
            case ServicePayloadMetadata.StopTrackSource -> {
                stopTrackSource();
            }
        }
    }

    private void init(Subscription subscription) {
        unitLifeCycle.emitNext(
            UnitPayload.createUnitInitPayload(),
            Sinks.EmitFailureHandler.FAIL_FAST
        );
    }

    private Disposable workerPullDisposable = Disposables.disposed();

    private void startTrackKeyword(String keyword) {
        workerPullDisposable = workerPull
            .start(keyword, new WorkerPullLimits(Duration.ofMinutes(1), 10))
            .doOnNext(update -> {
//                unitLifeCycle.emitNext(update, Sinks.EmitFailureHandler.FAIL_FAST);
            })
            .subscribe();
    }

    private void stopTrackKeyword() {
        if (!workerPullDisposable.isDisposed()) {
            workerPullDisposable.dispose();
        }
    }

    private void startTrackSource(String source) {

    }

    private void stopTrackSource() {

    }

}
