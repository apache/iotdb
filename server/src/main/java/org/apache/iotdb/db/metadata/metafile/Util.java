package org.apache.iotdb.db.metadata.metafile;

import java.nio.ByteBuffer;

public class Util {

    public static String readString(ByteBuffer buffer) {
        int length = buffer.get();
        if (length == 0) {
            return null;
        }
        ByteBuffer nameBuffer = ByteBuffer.allocate(length);
        buffer.get(nameBuffer.array());
        return new String(nameBuffer.array());
    }

    public static void writeString(ByteBuffer buffer, String content) {
        if (content == null || content.length() == 0) {
            buffer.put((byte) 0);
            return;
        }
        byte[] bytes = content.getBytes();
        buffer.put((byte) bytes.length);
        buffer.put(bytes);
    }

}
