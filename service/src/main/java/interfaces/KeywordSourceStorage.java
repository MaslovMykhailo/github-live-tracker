package interfaces;

import model.KeywordSource;
import reactor.core.publisher.Flux;

public interface KeywordSourceStorage {

    Flux<KeywordSource> getKeywordRecords();

}
