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
import metadata.ServicePayloadMetadata;
import model.KeywordSource;
import model.Limits;
import org.reactivestreams.Subscription;
import payload.UnitPayload;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import worker.configuration.WorkerConfiguration;
import worker.exception.InvalidResponseException;
import worker.pull.WorkerPull;
import worker.pull.WorkerPullLimits;

import java.time.Duration;

public class Unit {

    public static void main(String... args) {
        String host = "localhost";
        int port = 7000;

        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration
            .builder()
            .build();

        new Unit(host, port, configuration).run();
    }

    private final RetryBackoffSpec retrySpec = Retry
        .backoff(Long.MAX_VALUE, Duration.ofMillis(1))
        .maxBackoff(Duration.ofSeconds(10));

    public Unit(String host, int port, MssqlConnectionConfiguration mssqlConfiguration) {
        rSocket = RSocketConnector
            .connectWith(TcpClientTransport.create(host, port))
            .retryWhen(retrySpec)
            .block();

        HttpClient httpClient = HttpClient.create();

        MssqlConnection mssqlConnection = new MssqlConnectionFactory(mssqlConfiguration)
            .create()
            .retryWhen(retrySpec)
            .block();

        workerData = new GithubSearchWorkerData(httpClient);
        workerStorage = new MssqlWorkerStorage(mssqlConnection);
    }

    public Unit(
        RSocket rSocket,
        WorkerData<GithubSearchWorkerTarget> workerData,
        WorkerStorage<GithubSearchWorkerTarget> workerStorage
    ) {
        this.rSocket = rSocket;
        this.workerData = workerData;
        this.workerStorage = workerStorage;
    }

    private final Logger logger = Loggers.getLogger(Unit.class);

    private final RSocket rSocket;

    private final WorkerData<GithubSearchWorkerTarget> workerData;

    private final WorkerStorage<GithubSearchWorkerTarget> workerStorage;

    public void run() {
        logger.info("Unit running...");

        rSocket
            .requestChannel(unitLifeCycle.asFlux())
            .doOnSubscribe(this::emitInit)
            .doOnNext(this::handleChannelResponse)
            .publishOn(Schedulers.parallel())
            .retryWhen(retrySpec)
            .doFinally(signal -> rSocket.dispose())
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
            case ServicePayloadMetadata.UpdateLimits -> {
                updateLimits(Limits.fromJSON(payload.getDataUtf8()));
            }
        }
    }

    private void emitInit(Subscription subscription) {
        unitLifeCycle.emitNext(
            UnitPayload.createUnitInitPayload(),
            Sinks.EmitFailureHandler.FAIL_FAST
        );
    }

    private void emitError(Throwable throwable, Object object) {
        if (throwable instanceof InvalidResponseException) {
            WorkerConfiguration configuration = ((InvalidResponseException) throwable).configuration;
            KeywordSource keywordSource = new KeywordSource(
                -1,
                configuration.getKeyword(),
                configuration.getSource()
            );

            unitLifeCycle.emitNext(
                UnitPayload.createUnitErrorPayload(keywordSource.toJSON()),
                Sinks.EmitFailureHandler.FAIL_FAST
            );
        }
    }

    WorkerPull<GithubSearchWorkerTarget> workerPull = new WorkerPull<>(
        new WorkerPullLimits(
            Duration.ofSeconds(90),
            10
        )
    );

    private Disposable workerPullDisposable = Disposables.disposed();

    private void startTrackKeyword(String keyword) {
        workerPullDisposable = workerPull
            .start(keyword)
            .onErrorContinue(InvalidResponseException.class, this::emitError)
            .subscribe();

        logger.info("Start track keyword " + keyword);
    }

    private void stopTrackKeyword(String keyword) {
        if (!workerPull.getKeyword().equals(keyword)) {
            return;
        }

        workerPull.stop();

        if (!workerPullDisposable.isDisposed()) {
            workerPullDisposable.dispose();
        }

        logger.info("Stop track keyword " + keyword);
    }

    private void startTrackSource(String source) {
        workerPull.addTrackingSource(source, workerData, workerStorage);
        logger.info("Start track source " + source + " for keyword " + workerPull.getKeyword());
    }

    private void stopTrackSource(String source) {
        workerPull.removeTrackingSource(source);
        logger.info("Stop track source " + source + " for keyword " + workerPull.getKeyword());
    }

    private void updateLimits(Limits limits) {
        workerPull.setLimits(WorkerPullLimits.fromLimits(limits));
        logger.info("Update limits " + limits);
    }

}
