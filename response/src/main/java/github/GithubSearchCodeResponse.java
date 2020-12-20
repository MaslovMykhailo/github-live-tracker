package github;

import interfaces.JSONSerializable;

public class GithubSearchCodeResponse implements JSONSerializable {

    public int total_count;

    public boolean incomplete_results;

    public GithubSearchCodeItemResponse[] items;

    public static GithubSearchCodeResponse fromJSON(String json) {
        return gson.fromJson(json, GithubSearchCodeResponse.class);
    }

}
