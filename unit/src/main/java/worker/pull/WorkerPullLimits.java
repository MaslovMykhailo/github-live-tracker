package worker.pull;

import java.time.Duration;

public class WorkerPullLimits {

    private Duration timeRange;

    private long executionCount;

    public WorkerPullLimits(Duration timeRange, long executionCount) {
        this.timeRange = timeRange;
        this.executionCount = executionCount;
    }

    public Duration getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(Duration timeRange) {
        this.timeRange = timeRange;
    }

    public long getExecutionCount() {
        return executionCount;
    }

    public void setExecutionCount(int executionCount) {
        this.executionCount = executionCount;
    }

    public Duration getExecutionRate() {
        return timeRange.dividedBy(executionCount);
    }

}
