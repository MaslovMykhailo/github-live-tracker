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
        target.repository = response.repository;
        return target;
    }

}
