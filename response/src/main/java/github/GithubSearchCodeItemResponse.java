package github;

import interfaces.JSONSerializable;

public class GithubSearchCodeItemResponse implements JSONSerializable {

    public String sha;

    public GithubRepositoryResponse repository;

    public static GithubSearchCodeItemResponse fromJSON(String json) {
        return gson.fromJson(json, GithubSearchCodeItemResponse.class);
    }

}
