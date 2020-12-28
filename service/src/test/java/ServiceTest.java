import io.rsocket.Payload;
import io.rsocket.transport.netty.server.TcpServerTransport;
import metadata.ServicePayloadMetadata;
import model.KeywordSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import payload.ServicePayload;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import utils.TestClient;
import utils.TestUpdateEmitter;

import java.util.List;

public class ServiceTest {

    @Test
    public void shouldPropagateKeywordSourceMessagesOnUpdates() throws InterruptedException {
        int port = 5000;
        TcpServerTransport transport = TcpServerTransport.create("localhost", port);

        TestUpdateEmitter updateEmitter = new TestUpdateEmitter();

        // Create service
        Service service = new Service(transport, updateEmitter);
        Thread serviceThread = new Thread(service::run);
        serviceThread.start();

        // Create test client 1
        TestClient testClient1 = new TestClient();
        Disposable client1 = testClient1.create(port);
        testClient1.waitForInit().block();

        Assertions.assertTrue(updateEmitter.started);

        // Add new keyword source for client 1
        KeywordSource ks1 = new KeywordSource(1, "w1", "s1");
        updateEmitter.nextAdd(ks1);

        testClient1.waitForPayload(ServicePayload.createStartTrackKeywordPayload(ks1.word));
        testClient1.waitForPayload(ServicePayload.createStartTrackSourcePayload(ks1.source));

        // Add new keyword source for client 1
        KeywordSource ks2 = new KeywordSource(1, "w1", "s2");
        updateEmitter.nextAdd(ks2);

        testClient1.waitForPayload(ServicePayload.createStartTrackSourcePayload(ks2.source));

        // Create test client 2
        TestClient testClient2 = new TestClient();
        Disposable client2 = testClient2.create(port);
        testClient2.waitForInit().block();

        // Add new keyword source for client 2
        KeywordSource ks3 = new KeywordSource(3, "w2", "s3");
        updateEmitter.nextAdd(ks3);

        testClient2.waitForPayload(ServicePayload.createStartTrackKeywordPayload(ks3.word));
        testClient2.waitForPayload(ServicePayload.createStartTrackSourcePayload(ks3.source));

        // Remove keyword sources for client 1
        updateEmitter.nextRemove(ks1);
        updateEmitter.nextRemove(ks2);

        testClient1.waitForPayload(ServicePayload.createStopTrackSourcePayload(ks1.source));
        testClient1.waitForPayload(ServicePayload.createStopTrackSourcePayload(ks2.source));
        testClient1.waitForPayload(ServicePayload.createStopTrackKeywordPayload(ks1.word));

        // Add new keyword source for client 1
        KeywordSource ks4 = new KeywordSource(4, "w3", "s4");
        updateEmitter.nextAdd(ks4);

        testClient1.waitForPayload(ServicePayload.createStartTrackKeywordPayload(ks4.word));
        testClient1.waitForPayload(ServicePayload.createStartTrackSourcePayload(ks4.source));

        // Remove keyword sources for client 2
        updateEmitter.nextRemove(ks3);

        testClient2.waitForPayload(ServicePayload.createStopTrackSourcePayload(ks3.source));
        testClient2.waitForPayload(ServicePayload.createStopTrackKeywordPayload(ks3.word));

        // Cleanup
        updateEmitter.completeNow();
        serviceThread.join();
        List.of(client1, client2).forEach(Disposable::dispose);

        // Check message sequence for client 1
        StepVerifier
            .create(Flux.fromIterable(testClient1.payloadHistory))
            .assertNext(p -> assertPayloadEquals(p, ServicePayload.createStartTrackKeywordPayload(ks1.word)))
            .assertNext(this::assertUpdateLimitsPayload)
            .assertNext(p -> assertPayloadEquals(p, ServicePayload.createStartTrackSourcePayload(ks1.source)))
            .assertNext(p -> assertPayloadEquals(p, ServicePayload.createStartTrackSourcePayload(ks2.source)))
            .assertNext(p -> assertPayloadEquals(p, ServicePayload.createStopTrackSourcePayload(ks1.source)))
            .assertNext(p -> assertPayloadEquals(p, ServicePayload.createStopTrackSourcePayload(ks2.source)))
            .assertNext(p -> assertPayloadEquals(p, ServicePayload.createStopTrackKeywordPayload(ks1.word)))
            .assertNext(this::assertUpdateLimitsPayload)
            .assertNext(p -> assertPayloadEquals(p, ServicePayload.createStartTrackKeywordPayload(ks4.word)))
            .assertNext(this::assertUpdateLimitsPayload)
            .assertNext(p -> assertPayloadEquals(p, ServicePayload.createStartTrackSourcePayload(ks4.source)))
            .verifyComplete();

        // Check message sequence for client 2
        StepVerifier
            .create(Flux.fromIterable(testClient2.payloadHistory))
            .assertNext(p -> assertPayloadEquals(p, ServicePayload.createStartTrackKeywordPayload(ks3.word)))
            .assertNext(this::assertUpdateLimitsPayload)
            .assertNext(p -> assertPayloadEquals(p, ServicePayload.createStartTrackSourcePayload(ks3.source)))
            .assertNext(p -> assertPayloadEquals(p, ServicePayload.createStopTrackSourcePayload(ks3.source)))
            .assertNext(p -> assertPayloadEquals(p, ServicePayload.createStopTrackKeywordPayload(ks3.word)))
            .assertNext(this::assertUpdateLimitsPayload)
            .verifyComplete();
    }

    private void assertPayloadEquals(Payload p1, Payload p2) {
        Assertions.assertEquals(p1.getDataUtf8(), p2.getDataUtf8());
        Assertions.assertEquals(p1.getMetadataUtf8(), p2.getMetadataUtf8());
    }

    private void assertUpdateLimitsPayload(Payload p) {
        Assertions.assertEquals(ServicePayloadMetadata.UpdateLimits, p.getMetadataUtf8());
    }

}
