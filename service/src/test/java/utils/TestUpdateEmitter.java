package utils;

import interfaces.KeywordSourceUpdateEmitter;
import model.KeywordSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;

public class TestUpdateEmitter implements KeywordSourceUpdateEmitter {

    public boolean started = false;

    public boolean stopped = false;

    public void startListenUpdates() {
        started = true;
        stopped = false;
    }

    public void stopListenUpdates() {
        started = false;
        stopped = true;
    }

    private final Sinks.Many<KeywordSource> addKeywordSourceSink = Sinks
        .many()
        .unicast()
        .onBackpressureBuffer();

    public void nextAdd(KeywordSource keywordSource) {
        addKeywordSourceSink.emitNext(
            keywordSource,
            Sinks.EmitFailureHandler.FAIL_FAST
        );
    }

    public Flux<KeywordSource> addedKeywordSource() {
        return addKeywordSourceSink.asFlux();
    }

    private final Sinks.Many<KeywordSource> removeKeywordSourceSink = Sinks
        .many()
        .unicast()
        .onBackpressureBuffer();

    public void nextRemove(KeywordSource keywordSource) {
        removeKeywordSourceSink.emitNext(
            keywordSource,
            Sinks.EmitFailureHandler.FAIL_FAST
        );
    }

    public Flux<KeywordSource> removedKeywordSource() {
        return removeKeywordSourceSink.asFlux();
    }

    public void completeNow() {
        addKeywordSourceSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
        removeKeywordSourceSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
    }

    public void complete(Duration delay) {
        Mono
            .delay(delay)
            .doFinally(s -> completeNow())
            .block();
    }

}
