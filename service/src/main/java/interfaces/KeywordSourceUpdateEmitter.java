package interfaces;

import model.KeywordSource;
import reactor.core.publisher.Flux;

public interface KeywordSourceUpdateEmitter {

    void startListenUpdates();

    void stopListenUpdates();

    Flux<KeywordSource> addedKeywordSource();

    Flux<KeywordSource> removedKeywordSource();

}
