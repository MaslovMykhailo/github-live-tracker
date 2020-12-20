package github.live.tracker.payload;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import reactor.util.annotation.Nullable;

import java.nio.ByteBuffer;

public class BasePayload implements Payload {

    public static final ByteBuffer EmptyBuffer = ByteBuffer.allocateDirect(0);

    private final ByteBuffer data;

    private final ByteBuffer metadata;

    public BasePayload(ByteBuffer data, @Nullable ByteBuffer metadata) {
        this.data = data;
        this.metadata = metadata;
    }

    public static BasePayload create(ByteBuffer data, @Nullable ByteBuffer metadata) {
        return new BasePayload(data, metadata);
    }

    @Override
    public boolean hasMetadata() {
        return metadata != null;
    }

    @Override
    public ByteBuf sliceMetadata() {
        return metadata == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(metadata);
    }

    @Override
    public ByteBuf sliceData() {
        return Unpooled.wrappedBuffer(data);
    }

    @Override
    public ByteBuffer getMetadata() {
        return metadata == null ? DefaultPayload.EMPTY_BUFFER : metadata.duplicate();
    }

    @Override
    public ByteBuffer getData() {
        return data.duplicate();
    }

    @Override
    public ByteBuf data() {
        return sliceData();
    }

    @Override
    public ByteBuf metadata() {
        return sliceMetadata();
    }

    @Override
    public int refCnt() {
        return 1;
    }

    @Override
    public BasePayload retain() {
        return this;
    }

    @Override
    public BasePayload retain(int increment) {
        return this;
    }

    @Override
    public BasePayload touch() {
        return this;
    }

    @Override
    public BasePayload touch(Object hint) {
        return this;
    }

    @Override
    public boolean release() {
        return false;
    }

    @Override
    public boolean release(int decrement) {
        return false;
    }

}
