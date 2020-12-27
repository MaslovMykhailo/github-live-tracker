package worker.exception;

import worker.configuration.WorkerConfiguration;

public class InvalidResponseException extends Exception {

    public final WorkerConfiguration configuration;

    public InvalidResponseException(WorkerConfiguration configuration) {
        super(String.format(
            "Invalid response for [keyword]=%s and [source]=%s",
            configuration.getKeyword(),
            configuration.getSource()
        ));
        this.configuration = configuration;
    }

}
