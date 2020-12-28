package utils;

import interfaces.WorkerTarget;

public class WorkerTestTarget implements WorkerTarget {

    private final String identity;

    public WorkerTestTarget(String identity) {
        this.identity = identity;
    }

    public String getIdentity() {
        return identity;
    }

}
