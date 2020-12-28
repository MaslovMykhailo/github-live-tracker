package utils;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import metadata.UnitPayloadMetadata;
import payload.UnitPayload;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class TestClient {

    private RSocket buildClient(int port) {
        return RSocketConnector
            .connectWith(TcpClientTransport.create("localhost", port))
            .block();
    }

    public List<Payload> payloadHistory = new ArrayList<>();

    public Disposable create(int port) {
        return buildClient(port)
            .requestChannel(Mono.just(UnitPayload.createUnitInitPayload()))
            .doOnNext(payloadHistory::add)
            .subscribe();
    }

    public Mono<Void> waitForInit() {
        return Mono
            .delay(Duration.ofMillis(100))
            .map(l -> payloadHistory
                .stream()
                .anyMatch(payload -> payload
                    .getMetadataUtf8()
                    .equals(UnitPayloadMetadata.UnitInit)
                ) ? Mono.empty() : waitForInit()
            )
            .then();
    }

    public Mono<Void> waitForPayload(Payload targetPayload) {
        return Mono
            .delay(Duration.ofMillis(100))
            .map(l -> payloadHistory
                .stream()
                .anyMatch(payload -> isPayloadEquals(payload, targetPayload)) ?
                Mono.empty() :
                waitForPayload(targetPayload)
            )
            .then();
    }

    private boolean isPayloadEquals(Payload p1, Payload p2) {
        return p1.getDataUtf8().equals(p2.getDataUtf8()) &&
            p1.getMetadataUtf8().equals(p2.getMetadataUtf8());
    }

}
