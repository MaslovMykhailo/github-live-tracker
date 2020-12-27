package model;

import com.google.gson.Gson;

import java.util.Objects;

public class KeywordSource {

    public int id;

    public String word;

    public String source;

    public KeywordSource(int id, String word, String source) {
        this.id = id;
        this.word = word;
        this.source = source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeywordSource keywordSource = (KeywordSource) o;
        return id == keywordSource.id && Objects.equals(word, keywordSource.word) && Objects.equals(source, keywordSource.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, word, source);
    }

    @Override
    public String toString() {
        return "KeywordSource{" +
            "id=" + id +
            ", word='" + word + '\'' +
            ", source='" + source + '\'' +
            '}';
    }

    public String toJSON() {
        return new Gson().toJson(this);
    }

    public static KeywordSource fromJSON(String json) {
        return new Gson().fromJson(json, KeywordSource.class);
    }

}
