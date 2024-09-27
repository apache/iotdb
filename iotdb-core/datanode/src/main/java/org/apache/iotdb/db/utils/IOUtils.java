package org.apache.iotdb.db.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class IOUtils {
  public interface BufferSerializable {
    void serialize(ByteBuffer buffer);

    void deserialize(ByteBuffer buffer);
  }

  public interface StreamSerializable {
    void serialize(OutputStream stream) throws IOException;

    void deserialize(InputStream stream) throws IOException;
  }
}
