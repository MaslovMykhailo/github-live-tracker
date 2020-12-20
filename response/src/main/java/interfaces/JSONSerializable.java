package interfaces;

import com.google.gson.Gson;

public interface JSONSerializable {

    Gson gson = new Gson();

    default String toJSON() {
        return gson.toJson(this);
    }

    static JSONSerializable fromJSON(String json) {
        return gson.fromJson(json, JSONSerializable.class);
    }

}
