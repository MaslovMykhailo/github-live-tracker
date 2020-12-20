package implementations.h2;

import github.GithubSearchCodeItemResponse;
import implementations.github.GithubSearchWorkerTarget;
import interfaces.WorkerStorage;
import io.r2dbc.h2.H2Connection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import worker.WorkerConfiguration;

import java.time.Duration;

public class H2WorkerStorage implements WorkerStorage<GithubSearchWorkerTarget> {

    private final H2Connection connection;

    public H2WorkerStorage(H2Connection connection) {
        this.connection = connection;
    }

    public Mono<Void> store(
        Flux<GithubSearchWorkerTarget> target,
        WorkerConfiguration configuration
    ) {
        return target
            .windowTimeout(configuration.getPageSize(), Duration.ofSeconds(5))
            .flatMap(batch -> storeBatch(batch, configuration))
            .then();
    }

    private Mono<Void> storeBatch(
        Flux<GithubSearchWorkerTarget> target,
        WorkerConfiguration configuration
    ) {
        return target
            .collectList()
            .map(batch -> {
                String partialStatement =
                    "insert into keyword_record " +
                        "('keyword', 'source', 'sha', 'record') " +
                        "values ";

                String[] updates = batch
                    .stream()
                    .map(item -> "($1, $2, " + item.sha + ", " + item.toJSON() + ")")
                    .toArray(String[]::new);

                String sqlStatement = partialStatement + String.join(", ", updates);

                return connection
                    .createStatement(sqlStatement)
                    .bind("$1", configuration.getKeyword())
                    .bind("$2", configuration.getSource())
                    .execute();
            })
            .then();
    }

    public Flux<GithubSearchWorkerTarget> getLatest(WorkerConfiguration configuration) {
        return Flux.just();
    }

}
