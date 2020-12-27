package payload;

import metadata.BaseMetadata;
import metadata.ServicePayloadMetadata;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;

public class ServicePayload extends BasePayload {

    public ServicePayload(ByteBuffer data, @Nullable ByteBuffer metadata) {
        super(data, metadata);
    }

    public static ServicePayload create(ByteBuffer data, String metadata) {
        return new ServicePayload(data, BaseMetadata.getMetadataBytes(metadata));
    }

    public static ServicePayload create(String dataString, String metadata) {
        ByteBuffer data = ByteBuffer.allocate(dataString.length()).put(dataString.getBytes()).flip();
        return new ServicePayload(data, BaseMetadata.getMetadataBytes(metadata));
    }

    public static ServicePayload createStartTrackKeywordPayload(String keyword) {
        return create(keyword, ServicePayloadMetadata.StartTrackKeyword);
    }

    public static ServicePayload createStopTrackKeywordPayload(String keyword) {
        return create(keyword, ServicePayloadMetadata.StopTrackKeyword);
    }

    public static ServicePayload createStartTrackSourcePayload(String source) {
        return create(source, ServicePayloadMetadata.StartTrackSource);
    }

    public static ServicePayload createStopTrackSourcePayload(String source) {
        return create(source, ServicePayloadMetadata.StopTrackSource);
    }

    public static ServicePayload createUpdateLimitsPayload(String limits) {
        return create(limits, ServicePayloadMetadata.UpdateLimits);
    }

}
