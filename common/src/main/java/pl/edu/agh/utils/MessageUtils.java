package pl.edu.agh.utils;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.SerializationUtils;

public class MessageUtils {
    public static Object toObject(ByteBuf byteBuf) {
        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.getBytes(0, bytes);
        return SerializationUtils.deserialize(bytes);
    }
}
