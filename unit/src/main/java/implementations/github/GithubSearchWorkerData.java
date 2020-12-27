package implementations.github;

import github.GithubSearchCodeResponse;
import interfaces.WorkerData;
import reactor.core.publisher.Flux;
import reactor.netty.http.client.HttpClient;
import worker.configuration.WorkerConfiguration;

public class GithubSearchWorkerData implements WorkerData<GithubSearchWorkerTarget> {

    private final HttpClient client;

    public GithubSearchWorkerData(HttpClient client) {
        this.client = client;
    }

    public Flux<GithubSearchWorkerTarget> request(WorkerConfiguration configuration) {
        return client
            .get()
            .uri(buildUri(configuration.getKeyword(), configuration.getSource()))
            .responseContent()
            .aggregate()
            .asString()
            .flatMapMany(response -> Flux
                .fromArray(GithubSearchCodeResponse
                    .fromJSON(response)
                    .items
                )
                .map(GithubSearchWorkerTarget::fromSearchResponse)
            );
    }

    private String buildUri(String searchBy, String searchIn) {
        String protocol = "https";
        String baseUrl = "api.github.com";
        String searchPath = "/search/code";

        return protocol + "://" + baseUrl + searchPath + "?" + String
            .join("+", new String[]{"q=" + searchBy, searchIn, "s:indexed", "o:desc"});
    }

}
