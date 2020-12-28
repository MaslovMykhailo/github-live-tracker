import implementations.github.GithubSearchWorkerTarget;
import interfaces.WorkerData;
import interfaces.WorkerStorage;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import metadata.UnitPayloadMetadata;
import model.Limits;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import payload.ServicePayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import worker.configuration.WorkerConfiguration;
import worker.exception.InvalidResponseException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class UnitTest {

    private CloseableChannel buildServer(int port) {
        return RSocketServer
            .create(SocketAcceptor.forRequestChannel(requestChannelHandler))
            .bindNow(TcpServerTransport.create("localhost", port));
    }

    private RSocket buildClient(int port) {
        return RSocketConnector
            .connectWith(TcpClientTransport.create("localhost", port))
            .block();
    }

    private Function<Publisher<Payload>, Flux<Payload>> requestChannelHandler;

    @Test
    public void shouldSendInitPayload() {
        requestChannelHandler = payloads -> {
            StepVerifier
                .create(payloads)
                .assertNext(payload -> Assertions
                    .assertEquals(
                        UnitPayloadMetadata.UnitInit,
                        payload.getMetadataUtf8()
                    )
                )
                .thenCancel()
                .verify();

            return Flux.empty();
        };

        CloseableChannel server = buildServer(3000);

        new Unit(
            buildClient(3000),
            Mockito.mock(WorkerData.class),
            Mockito.mock(WorkerStorage.class)
        ).run();

        server.dispose();
    }

    @Test
    public void shouldHandleKeywordSourcesUpdates() {
        // update limits

        // start track keyword1
        // start track source1
        // start track source2
        // start track source3
        // stop track source2
        // stop track keyword1

        // start track keyword2
        // start track source4
        // start track source5
        // stop track keyword2
    }

    @Test
    public void shouldSendErrorPayloadWhenDataRequestFails() {
        WorkerConfiguration configuration = new WorkerConfiguration("word", "source", 10);

        WorkerData<GithubSearchWorkerTarget> mockedData = Mockito.mock(WorkerData.class);
        Mockito
            .when(mockedData.request(Mockito.any()))
            .thenReturn(Flux.error(new InvalidResponseException(configuration)));

        WorkerStorage<GithubSearchWorkerTarget> mockedStorage = Mockito.mock(WorkerStorage.class);
        Mockito
            .when(mockedStorage.store(Mockito.any(), Mockito.any()))
            .thenReturn(Mono.empty());
        Mockito
            .when(mockedStorage.getLatestIdentity(Mockito.any()))
            .thenReturn(Flux.empty());

        List<Payload> payloadHistory = new ArrayList<>();

        requestChannelHandler = payloads -> Flux
            .from(payloads)
            .doOnNext(payloadHistory::add)
            .take(2)
            .filter(payload -> payload.getMetadataUtf8().equals(UnitPayloadMetadata.UnitInit))
            .switchMap(payload -> Flux
                .just(
                    ServicePayload.createUpdateLimitsPayload(new Limits(10, 1).toJSON()),
                    ServicePayload.createStartTrackKeywordPayload(configuration.getKeyword()),
                    ServicePayload.createStartTrackSourcePayload(configuration.getSource())
                )
                .concatWith(
                    Mono
                        .delay(Duration.ofMillis(100))
                        .map(l -> ServicePayload
                            .createStopTrackKeywordPayload(configuration.getKeyword())
                        )
                )
            );

        CloseableChannel server = buildServer(4000);

        new Unit(
            buildClient(4000),
            mockedData,
            mockedStorage
        ).run();

        server.dispose();

        Assertions.assertEquals(
            UnitPayloadMetadata.UnitError,
            payloadHistory.get(1).getMetadataUtf8()
        );
    }

}
