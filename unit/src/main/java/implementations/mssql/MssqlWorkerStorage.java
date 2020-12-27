package implementations.mssql;

import implementations.github.GithubSearchWorkerTarget;
import interfaces.WorkerStorage;
import io.r2dbc.mssql.MssqlConnection;
import io.r2dbc.mssql.MssqlResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import worker.configuration.WorkerConfiguration;

import java.time.Duration;

public class MssqlWorkerStorage implements WorkerStorage<GithubSearchWorkerTarget> {

    private final MssqlConnection connection;

    public MssqlWorkerStorage(MssqlConnection connection) {
        this.connection = connection;
    }

    public Mono<Void> store(
        Flux<GithubSearchWorkerTarget> target,
        WorkerConfiguration configuration
    ) {
        return target
            .windowTimeout(configuration.getPageSize(), Duration.ofSeconds(5))
            .onBackpressureBuffer()
            .flatMap(batch -> storeBatch(batch, configuration))
            .then();
    }

    private Flux<Integer> getDistinctKeywordSourceIds(
        WorkerConfiguration configuration
    ) {
        return connection
            .createStatement(
                "SELECT DISTINCT Id FROM Keywords " +
                    "WHERE Word = @word AND Source = @source"
            )
            .bind("word", configuration.getKeyword())
            .bind("source", configuration.getSource())
            .execute()
            .flatMap(result -> result.map(
                (row, metadata) -> row.get("Id", Integer.class)
            ));
    }

    private Mono<Void> storeBatch(
        Flux<GithubSearchWorkerTarget> target,
        WorkerConfiguration configuration
    ) {
        return getDistinctKeywordSourceIds(configuration)
            .flatMap(keywordId -> target
                .flatMap(data -> connection
                    .createStatement(
                        "INSERT INTO KeywordInfos " +
                            "(Word, Source, FileName, RelativePath, FileUrl, RepositoryUrl, ShaHash, KeywordId) " +
                            "VALUES " +
                            "(@word, @source, @fileName, @relativePath, @fileUrl, @repositoryUrl, @shaHash, @keywordId)"
                    )
                    .bind("word", configuration.getKeyword())
                    .bind("source", configuration.getSource())
                    .bind("fileName", data.name)
                    .bind("relativePath", data.path)
                    .bind("fileUrl", data.html_url)
                    .bind("repositoryUrl", data.repository.html_url)
                    .bind("shaHash", data.sha)
                    .bind("keywordId", keywordId)
                    .execute()
                )
                .flatMap(MssqlResult::getRowsUpdated)
            )
            .then();
    }

    public Flux<String> getLatestIdentity(WorkerConfiguration configuration) {
        return connection
            .createStatement(
                "SELECT DISTINCT TOP 100 Id, ShaHash FROM KeywordInfos " +
                    "WHERE Word = @word AND Source = @source " +
                    "ORDER BY Id DESC"
            )
            .bind("word", configuration.getKeyword())
            .bind("source", configuration.getSource())
            .execute()
            .flatMap(result -> result.map(
                (row, metadata) -> row.get("ShaHash", String.class)
            ));
    }
}
