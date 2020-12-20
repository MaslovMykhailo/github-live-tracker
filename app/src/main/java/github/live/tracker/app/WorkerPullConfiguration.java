package github.live.tracker.app;

public class WorkerPullConfiguration {

    private String keyword;

    private WorkerPullLimits limits;

    public WorkerPullConfiguration(String keyword, WorkerPullLimits limits) {
        this.keyword = keyword;
        this.limits = limits;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public WorkerPullLimits getLimits() {
        return limits;
    }

    public void setLimits(WorkerPullLimits limits) {
        this.limits = limits;
    }
}
