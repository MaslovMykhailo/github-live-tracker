package github.live.tracker.payload;

import github.live.tracker.payload.metadata.BaseMetadata;
import github.live.tracker.payload.metadata.UnitPayloadMetadata;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;

public class UnitPayload extends BasePayload {

    public UnitPayload(ByteBuffer data, @Nullable ByteBuffer metadata) {
        super(data, metadata);
    }

    public static UnitPayload create(ByteBuffer data, String metadata) {
        return new UnitPayload(data, BaseMetadata.getMetadataBytes(metadata));
    }

    public static UnitPayload create(String dataString, String metadata) {
        ByteBuffer data = ByteBuffer.allocate(dataString.length()).put(dataString.getBytes()).flip();
        return new UnitPayload(data, BaseMetadata.getMetadataBytes(metadata));
    }

    public static UnitPayload createUnitInitPayload() {
        return create(BasePayload.EmptyBuffer, UnitPayloadMetadata.UnitInit);
    }

    public static UnitPayload createUnitUpdatePayload(String update) {
        return create(update, UnitPayloadMetadata.UnitUpdate);
    }

}
