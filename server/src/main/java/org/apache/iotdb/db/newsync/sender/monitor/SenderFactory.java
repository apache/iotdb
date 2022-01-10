package org.apache.iotdb.db.newsync.sender.monitor;

import org.apache.iotdb.db.newsync.sender.pipe.IoTDBPipeSink;
import org.apache.iotdb.db.newsync.sender.pipe.Pipe;
import org.apache.iotdb.db.newsync.sender.pipe.PipeSink;

public class SenderFactory {
  public static PipeSink createPipeSink(PipeSink.Type type, String name) {
    if (type == PipeSink.Type.IoTDB) {
      return new IoTDBPipeSink(name);
    }
    throw new UnsupportedOperationException("do not support for " + type + " pipeSink");
  }

  public static Pipe createPipe(String type) {
    return null;
  }
}
