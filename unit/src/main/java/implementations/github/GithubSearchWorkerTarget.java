package implementations.github;

import github.GithubSearchCodeItemResponse;
import interfaces.WorkerTarget;

public class GithubSearchWorkerTarget extends GithubSearchCodeItemResponse implements WorkerTarget {

    public String getIdentity() {
        return sha;
    }

    public static GithubSearchWorkerTarget fromSearchResponse(GithubSearchCodeItemResponse response) {
        GithubSearchWorkerTarget target = new GithubSearchWorkerTarget();

        target.sha = response.sha;
        target.name = response.name;
        target.path = response.path;
        target.html_url = response.html_url;
        target.repository = response.repository;

        return target;
    }

}
