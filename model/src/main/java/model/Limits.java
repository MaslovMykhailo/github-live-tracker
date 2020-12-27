package model;

import com.google.gson.Gson;

public class Limits {

    public long timeRangeMillis;

    public long executionCount;

    public Limits(long timeRangeMillis, long executionCount) {
        this.timeRangeMillis = timeRangeMillis;
        this.executionCount = executionCount;
    }

    public String toJSON() {
        return new Gson().toJson(this);
    }

    public static Limits fromJSON(String json) {
        return new Gson().fromJson(json, Limits.class);
    }

    @Override
    public String toString() {
        return "Limits{" +
            "timeRangeMillis=" + timeRangeMillis +
            ", executionCount=" + executionCount +
            '}';
    }
}
