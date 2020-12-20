package github.live.tracker.payload.metadata;

import java.nio.ByteBuffer;

public interface BaseMetadata {

    static ByteBuffer getMetadataBytes(String metadata) {
        return ByteBuffer
            .allocate(metadata.length())
            .put(metadata.getBytes())
            .flip();
    }

}
