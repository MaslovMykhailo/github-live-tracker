import interfaces.WorkerData;
import interfaces.WorkerStorage;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import utils.WorkerTestTarget;
import worker.Worker;
import worker.configuration.WorkerConfiguration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class WorkerTest {

    @Test
    public void shouldRequestRecordsOnExecute() {
        WorkerData<WorkerTestTarget> mockedData = Mockito.mock(WorkerData.class);
        WorkerStorage<WorkerTestTarget> mockedStorage = Mockito.mock(WorkerStorage.class);

        WorkerConfiguration workerConfiguration = new WorkerConfiguration("k", "s", 1);

        Mockito.when(mockedData.request(Mockito.any())).thenReturn(Flux.empty());
        Mockito.when(mockedStorage.getLatestIdentity(Mockito.any())).thenReturn(Flux.empty());
        Mockito.when(mockedStorage.store(Mockito.any(), Mockito.any())).thenReturn(Mono.empty().then());

        Worker<WorkerTestTarget> worker = new Worker<>(
            mockedData,
            mockedStorage,
            workerConfiguration
        );

        StepVerifier
            .create(worker.execute())
            .verifyComplete();

        Mockito.verify(mockedData).request(workerConfiguration);
    }

    @Test
    public void shouldStoreOnlyNewestRecords() {
        WorkerData<WorkerTestTarget> mockedData = Mockito.mock(WorkerData.class);

        List<String> existedIdentities = Arrays.asList("a", "b");
        List<String> newIdentities = Collections.singletonList("c");

        Mockito.when(mockedData.request(Mockito.any())).thenReturn(
            Flux.merge(
                Flux.fromIterable(existedIdentities),
                Flux.fromIterable(newIdentities)
            ).map(WorkerTestTarget::new)
        );

        WorkerStorage<WorkerTestTarget> mockedStorage = new WorkerStorage<>() {
            public Mono<Void> store(Flux<WorkerTestTarget> target, WorkerConfiguration configuration) {
                return target
                    .flatMap(t -> !t.getIdentity().equals(newIdentities.get(0)) ?
                        Mono.error(new RuntimeException()) :
                        Mono.empty()
                    )
                    .then();
            }

            public Flux<String> getLatestIdentity(WorkerConfiguration configuration) {
                return Flux.fromIterable(existedIdentities);
            }
        };

        WorkerConfiguration workerConfiguration = new WorkerConfiguration("k", "s", 1);
        Worker<WorkerTestTarget> worker = new Worker<>(
            mockedData,
            mockedStorage,
            workerConfiguration
        );

        StepVerifier
            .create(worker.execute())
            .verifyComplete();
    }

}
