package cache;

import io.rsocket.Payload;
import reactor.core.publisher.Sinks;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KeywordSinksCache {

    private final Map<String, Record> keywordSinks = new HashMap<>();

    public synchronized Record putKeywordSink(String keyword, Sinks.Many<Payload> sink) {
        Record record = new Record(sink);
        keywordSinks.put(keyword, record);
        return record;
    }

    public synchronized Record removeKeywordSink(String keyword) {
        return keywordSinks.remove(keyword);
    }

    public synchronized Record getKeywordSink(String keyword) {
        return keywordSinks.get(keyword);
    }

    public synchronized int getSize() {
        return keywordSinks.size();
    }

    public static class Record {

        public final Sinks.Many<Payload> sink;

        public final Set<String> sources = new HashSet<>();

        private Record(Sinks.Many<Payload> sink) {
            this.sink = sink;
        }

    }

}
