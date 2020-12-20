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

    public static UnitPayload createUnitInitPayload() {
        return create(BasePayload.EmptyBuffer, UnitPayloadMetadata.UnitInit);
    }

    public static UnitPayload createUnitUpdatePayload(String update) {
        ByteBuffer data = ByteBuffer
            .allocate(update.length())
            .put(update.getBytes());

        return create(data, UnitPayloadMetadata.UnitUpdate);
    }

}
