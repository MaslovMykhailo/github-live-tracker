package github;

import interfaces.JSONSerializable;

public class GithubRepositoryOwnerResponse implements JSONSerializable {

    public String login;

    public static GithubRepositoryOwnerResponse fromJSON(String json) {
        return gson.fromJson(json, GithubRepositoryOwnerResponse.class);
    }

}
