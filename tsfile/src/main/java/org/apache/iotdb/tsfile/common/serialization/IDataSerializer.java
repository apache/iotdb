package org.apache.iotdb.tsfile.common.serialization;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface IDataSerializer<T, V> {
    int serializeTo(T data, OutputStream outputStream) throws IOException;
    int serializeTo(T data, ByteBuffer buffer);
    T deserializeFrom(ByteBuffer buffer, V options);
    T deserializeFrom(InputStream inputStream, V options) throws IOException;
}
