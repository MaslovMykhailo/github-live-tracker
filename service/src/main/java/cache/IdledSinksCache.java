package cache;

import io.rsocket.Payload;
import reactor.core.publisher.Sinks;

import java.util.ArrayDeque;
import java.util.Queue;

public class IdledSinksCache {

    private final Queue<Sinks.Many<Payload>> idledKeywordSinks = new ArrayDeque<>();

    public synchronized Sinks.Many<Payload> addIdledSink(Sinks.Many<Payload> sink) {
        idledKeywordSinks.add(sink);
        return sink;
    }

    public synchronized Sinks.Many<Payload> pollIdledSink() {
        return idledKeywordSinks.poll();
    }

}
