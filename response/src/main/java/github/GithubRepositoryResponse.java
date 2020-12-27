package github;

import interfaces.JSONSerializable;

public class GithubRepositoryResponse implements JSONSerializable {

    public String name;

    public String html_url;

    public GithubRepositoryOwnerResponse owner;

    public static GithubRepositoryResponse fromJSON(String json) {
        return gson.fromJson(json, GithubRepositoryResponse.class);
    }

}
