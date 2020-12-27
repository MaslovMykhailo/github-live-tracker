import metadata.ServicePayloadMetadata;
import implementations.github.GithubSearchWorkerData;
import implementations.github.GithubSearchWorkerTarget;
import implementations.mssql.MssqlWorkerStorage;
import interfaces.WorkerData;
import interfaces.WorkerStorage;
import io.r2dbc.mssql.MssqlConnection;
import io.r2dbc.mssql.MssqlConnectionConfiguration;
import io.r2dbc.mssql.MssqlConnectionFactory;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import org.reactivestreams.Subscription;
import payload.UnitPayload;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Sinks;
import reactor.netty.http.client.HttpClient;
import worker.pull.WorkerPull;
import worker.pull.WorkerPullLimits;

import java.time.Duration;

public class Unit {

    public static void main(String... args) {
        new Unit().run();
    }

    public void run() {

        String host = "localhost";
        int port = 7000;

        TcpClientTransport transport = TcpClientTransport.create(host, port);
        RSocket rSocket = RSocketConnector.connectWith(transport).block();

        workerData = new GithubSearchWorkerData(
            HttpClient.create()
        );

        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .build();

        MssqlConnectionFactory factory = new MssqlConnectionFactory(configuration);

        MssqlConnection mssqlConnection = factory
            .create()
            .block();

        workerStorage = new MssqlWorkerStorage(mssqlConnection);

        rSocket
            .requestChannel(unitLifeCycle.asFlux())
            .doOnSubscribe(this::init)
            .doOnNext(this::handleChannelResponse)
            .doFinally(signal -> {
                if (!rSocket.isDisposed()) {
                    rSocket.dispose();
                }
            })
            .then()
            .block();
    }

    private final Sinks.Many<Payload> unitLifeCycle = Sinks
        .many()
        .multicast()
        .onBackpressureBuffer();

    private void handleChannelResponse(Payload payload) {
        switch (payload.getMetadataUtf8()) {
            case ServicePayloadMetadata.StartTrackKeyword -> {
                startTrackKeyword(payload.getDataUtf8());
            }
            case ServicePayloadMetadata.StopTrackKeyword -> {
                stopTrackKeyword(payload.getDataUtf8());
            }
            case ServicePayloadMetadata.StartTrackSource -> {
                startTrackSource(payload.getDataUtf8());
            }
            case ServicePayloadMetadata.StopTrackSource -> {
                stopTrackSource(payload.getDataUtf8());
            }
        }
    }

    private void init(Subscription subscription) {
        unitLifeCycle.emitNext(
            UnitPayload.createUnitInitPayload(),
            Sinks.EmitFailureHandler.FAIL_FAST
        );
    }

    WorkerPull<GithubSearchWorkerTarget> workerPull = new WorkerPull<>(
        new WorkerPullLimits(
            Duration.ofMinutes(1),
            10
        )
    );

    private Disposable workerPullDisposable = Disposables.disposed();

    private void startTrackKeyword(String keyword) {
        workerPullDisposable = workerPull
            .start(keyword)
            .subscribe();
    }

    private void stopTrackKeyword(String keyword) {
        if (!workerPull.getKeyword().equals(keyword)) {
            return;
        }

        if (!workerPullDisposable.isDisposed()) {
            workerPullDisposable.dispose();
        }

        workerPull.stop();
    }

    private WorkerData<GithubSearchWorkerTarget> workerData;

    private WorkerStorage<GithubSearchWorkerTarget> workerStorage;

    private void startTrackSource(String source) {
        workerPull.addTrackingSource(source, workerData, workerStorage);
    }

    private void stopTrackSource(String source) {
        workerPull.removeTrackingSource(source);
    }

}
