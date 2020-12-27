package worker.configuration;

public class WorkerConfiguration {

    private final String keyword;

    private final String source;

    private final int pageSize;

    public WorkerConfiguration(String keyword, String source, int pageSize) {
        this.keyword = keyword;
        this.source = source;
        this.pageSize = pageSize;
    }

    public String getKeyword() {
        return keyword;
    }

    public String getSource() {
        return source;
    }

    public int getPageSize() {
        return pageSize;
    }

}
