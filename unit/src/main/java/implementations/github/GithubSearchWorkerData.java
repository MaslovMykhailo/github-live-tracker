package implementations.github;

import github.GithubSearchCodeResponse;
import interfaces.WorkerData;
import reactor.core.publisher.Flux;
import reactor.netty.http.client.HttpClient;
import reactor.util.Logger;
import reactor.util.Loggers;
import worker.configuration.WorkerConfiguration;
import worker.exception.InvalidResponseException;

public class GithubSearchWorkerData implements WorkerData<GithubSearchWorkerTarget> {

    private final Logger logger = Loggers.getLogger(GithubSearchWorkerData.class);

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
            .flatMapMany(response -> {
                GithubSearchCodeResponse searchResponse = GithubSearchCodeResponse.fromJSON(response);

                if (searchResponse.items == null) {
                    logger.info("Request data fail\n" + response);
                    return Flux.error(new InvalidResponseException(configuration));
                }

                return Flux.fromArray(searchResponse.items);
            })
            .map(GithubSearchWorkerTarget::fromSearchResponse);
    }

    private String buildUri(String searchBy, String searchIn) {
        String protocol = "https";
        String baseUrl = "api.github.com";
        String searchPath = "/search/code";

        return protocol + "://" + baseUrl + searchPath + "?" + String
            .join("+", new String[]{"q=" + searchBy, searchIn, "s:indexed", "o:desc"});
    }

}
