package implementations.mssql;

import interfaces.KeywordSourceStorage;
import io.r2dbc.mssql.MssqlConnection;
import model.KeywordSource;
import reactor.core.publisher.Flux;

public class MssqlKeywordSourceStorage implements KeywordSourceStorage {

    private final MssqlConnection connection;

    public MssqlKeywordSourceStorage(MssqlConnection connection) {
        this.connection = connection;
    }

    public Flux<KeywordSource> getKeywordRecords() {
        return connection
            .createStatement("SELECT * FROM Keywords")
            .execute()
            .flatMap(MssqlKeywordSourceMapper::map);
    }

}
