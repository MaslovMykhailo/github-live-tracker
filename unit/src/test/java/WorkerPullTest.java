import interfaces.WorkerData;
import interfaces.WorkerStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import utils.WorkerTestStorage;
import utils.WorkerTestTarget;
import worker.configuration.WorkerConfiguration;
import worker.pull.WorkerPull;
import worker.pull.WorkerPullLimits;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

public class WorkerPullTest {

    @Test
    public void shouldExecuteWorkersAccordingLimits() {
        WorkerPullLimits limits = new WorkerPullLimits(Duration.ofSeconds(1), 1);
        WorkerPull<WorkerTestTarget> workerPull = new WorkerPull<>(limits);

        WorkerData<WorkerTestTarget> mockedData = Mockito.mock(WorkerData.class);
        Mockito.when(mockedData.request(Mockito.any())).thenReturn(Flux.empty());

        WorkerTestStorage testStorage = new WorkerTestStorage();
        workerPull.addTrackingSource("source", mockedData, testStorage.create());

        StepVerifier
            .withVirtualTime(() -> workerPull.start("word"))
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(1))
            .expectNext()
            .expectNoEvent(Duration.ofSeconds(1))
            .expectNext()
            .thenCancel()
            .verify();
    }

    @Test
    public void shouldUpdateExecutionRateAccordingLimits() {
        WorkerPullLimits limits = new WorkerPullLimits(Duration.ofSeconds(1), 1);
        WorkerPull<WorkerTestTarget> workerPull = new WorkerPull<>(limits);

        WorkerData<WorkerTestTarget> mockedData = Mockito.mock(WorkerData.class);
        Mockito.when(mockedData.request(Mockito.any())).thenReturn(Flux.empty());

        WorkerTestStorage testStorage = new WorkerTestStorage();
        workerPull.addTrackingSource("source", mockedData, testStorage.create());

        StepVerifier
            .withVirtualTime(() -> workerPull.start("word"))
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(1))
            .expectNext()
            .then(() -> workerPull.setLimits(new WorkerPullLimits(Duration.ofSeconds(5), 1)))
            .expectNoEvent(Duration.ofSeconds(5))
            .expectNext()
            .expectNoEvent(Duration.ofSeconds(5))
            .expectNext()
            .then(() -> workerPull.setLimits(new WorkerPullLimits(Duration.ofSeconds(3), 1)))
            .expectNoEvent(Duration.ofSeconds(3))
            .expectNext()
            .expectNoEvent(Duration.ofSeconds(3))
            .expectNext()
            .thenCancel()
            .verify();
    }

    @Test
    public void shouldFollowQueueOfWorkersExecution() {
        WorkerPullLimits limits = new WorkerPullLimits(Duration.ofSeconds(1), 1);
        WorkerPull<WorkerTestTarget> workerPull = new WorkerPull<>(limits);

        WorkerTestStorage testStorage = new WorkerTestStorage();
        WorkerStorage<WorkerTestTarget> mockedStorage = testStorage.create();

        WorkerData<WorkerTestTarget> mockedData = Mockito.mock(WorkerData.class);
        Mockito.when(mockedData.request(Mockito.any())).thenReturn(Flux.empty());

        List<String> sources = List.of("s1", "s2", "s3");

        StepVerifier
            .withVirtualTime(() -> workerPull.start("word"))
            .expectSubscription()
            .then(() -> sources
                .forEach(source -> workerPull
                    .addTrackingSource(source, mockedData, mockedStorage)
                )
            )
            .expectNoEvent(Duration.ofSeconds(1))
            .expectNext()
            .expectNoEvent(Duration.ofSeconds(1))
            .expectNext()
            .expectNoEvent(Duration.ofSeconds(1))
            .expectNext()
            .thenCancel()
            .verify();

        Assertions.assertIterableEquals(
            sources,
            testStorage
                .storeConfigurationHistory
                .stream()
                .map(WorkerConfiguration::getSource)
                .collect(Collectors.toList())
        );
    }

}
