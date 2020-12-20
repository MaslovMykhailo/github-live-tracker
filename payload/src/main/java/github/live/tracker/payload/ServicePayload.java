package github.live.tracker.payload;

import github.live.tracker.payload.metadata.BaseMetadata;
import github.live.tracker.payload.metadata.ServicePayloadMetadata;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;

public class ServicePayload extends BasePayload {

    public ServicePayload(ByteBuffer data, @Nullable ByteBuffer metadata) {
        super(data, metadata);
    }

    public static ServicePayload create(ByteBuffer data, String metadata) {
        return new ServicePayload(data, BaseMetadata.getMetadataBytes(metadata));
    }

    public static ServicePayload createStartTrackKeywordPayload(String keyword) {
        ByteBuffer data = ByteBuffer
            .allocate(keyword.length())
            .put(keyword.getBytes());

        return create(data, ServicePayloadMetadata.StartTrackKeyword);
    }

    public static ServicePayload createStopTrackKeywordPayload() {
        return create(BasePayload.EmptyBuffer, ServicePayloadMetadata.StopTrackKeyword);
    }

    public static ServicePayload createStartTrackSourcePayload(String source) {
        ByteBuffer data = ByteBuffer
            .allocate(source.length())
            .put(source.getBytes());

        return create(data, ServicePayloadMetadata.StartTrackSource);
    }

    public static ServicePayload createStopTrackSourcePayload() {
        return create(BasePayload.EmptyBuffer, ServicePayloadMetadata.StopTrackSource);
    }

}
