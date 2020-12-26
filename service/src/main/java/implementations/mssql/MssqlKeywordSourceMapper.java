package implementations.mssql;

import io.r2dbc.mssql.MssqlResult;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import model.KeywordSource;
import reactor.core.publisher.Flux;

public class MssqlKeywordSourceMapper {

    public static KeywordSource map(Row row, RowMetadata metadata) {
        return new KeywordSource(
            row.get("Id", Integer.class),
            row.get("Word", String.class),
            row.get("Source", String.class)
        );
    }

    public static Flux<KeywordSource> map(MssqlResult result) {
        return result.map(MssqlKeywordSourceMapper::map);
    }

}
