package org.apache.iotdb.commons.exception.pipe;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public enum PipeRuntimeExceptionType {
  NON_CRITICAL_EXCEPTION((short) 1),
  CRITICAL_EXCEPTION((short) 2),
  CONNECTOR_CRITICAL_EXCEPTION((short) 3),
  ;

  private final short type;

  PipeRuntimeExceptionType(short type) {
    this.type = type;
  }

  public short getType() {
    return type;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(type, byteBuffer);
  }

  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(type, stream);
  }

  public static PipeRuntimeException deserializeFrom(ByteBuffer byteBuffer) {
    final short type = ReadWriteIOUtils.readShort(byteBuffer);
    switch (type) {
      case 1:
        return PipeRuntimeNonCriticalException.deserializeFrom(byteBuffer);
      case 2:
        return PipeRuntimeCriticalException.deserializeFrom(byteBuffer);
      case 3:
        return PipeRuntimeConnectorCriticalException.deserializeFrom(byteBuffer);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported PipeRuntimeException type %s.", type));
    }
  }

  public static PipeRuntimeException deserializeFrom(InputStream stream) throws IOException {
    final short type = ReadWriteIOUtils.readShort(stream);
    switch (type) {
      case 1:
        return PipeRuntimeNonCriticalException.deserializeFrom(stream);
      case 2:
        return PipeRuntimeCriticalException.deserializeFrom(stream);
      case 3:
        return PipeRuntimeConnectorCriticalException.deserializeFrom(stream);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported PipeRuntimeException type %s.", type));
    }
  }
}
