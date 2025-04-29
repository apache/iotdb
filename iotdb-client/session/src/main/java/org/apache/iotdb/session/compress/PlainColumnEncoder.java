package org.apache.iotdb.session.compress;

import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.PlainEncoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;

import java.io.IOException;
import java.util.List;

public class PlainColumnEncoder implements ColumnEncoder {
  private final Encoder encoder;
  private final TSDataType dataType;
  private static final int DEFAULT_MAX_STRING_LENGTH = 128;

  public PlainColumnEncoder(TSDataType dataType) {
    this.dataType = dataType;
    this.encoder = new PlainEncoder(dataType, DEFAULT_MAX_STRING_LENGTH);
  }

  @Override
  public byte[] encode(List<?> data) throws IOException {
    if (data == null || data.isEmpty()) {
      return new byte[0];
    }

    PublicBAOS outputStream = new PublicBAOS();
    try {
      switch (dataType) {
        case BOOLEAN:
          for (Object value : data) {
            if (value != null) {
              encoder.encode((Boolean) value, outputStream);
            }
          }
          break;
        case INT32:
        case DATE:
          for (Object value : data) {
            if (value != null) {
              encoder.encode((Integer) value, outputStream);
            }
          }
          break;
        case INT64:
        case TIMESTAMP:
          for (Object value : data) {
            if (value != null) {
              encoder.encode((Long) value, outputStream);
            }
          }
          break;
        case FLOAT:
          for (Object value : data) {
            if (value != null) {
              encoder.encode((Float) value, outputStream);
            }
          }
          break;
        case DOUBLE:
          for (Object value : data) {
            if (value != null) {
              encoder.encode((Double) value, outputStream);
            }
          }
          break;
        case TEXT:
        case STRING:
        case BLOB:
          for (Object value : data) {
            if (value != null) {
              if (value instanceof String) {
                encoder.encode(new Binary((byte[]) value), outputStream);
              } else if (value instanceof Binary) {
                encoder.encode((Binary) value, outputStream);
              } else {
                throw new IOException(
                    "Unsupported value type for TEXT/STRING/BLOB: " + value.getClass());
              }
            }
          }
          break;
        default:
          throw new UnsupportedOperationException("PLAIN doesn't support data type: " + dataType);
      }
      encoder.flush(outputStream);
      return outputStream.toByteArray();
    } finally {
      outputStream.close();
    }
  }

  //  @Override
  //  public List<?> decode(byte[] data) throws IOException {
  //    if (data == null || data.length == 0) {
  //      return new ArrayList<>();
  //    }
  //
  //    List<Object> result = new ArrayList<>();
  //    ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
  //    try {
  //      switch (dataType) {
  //        case BOOLEAN:
  //          while (inputStream.available() > 0) {
  //            result.add(inputStream.read() != 0);
  //          }
  //          break;
  //        case INT32:
  //        case DATE:
  //          while (inputStream.available() > 0) {
  //            result.add(ReadWriteForEncodingUtils.readVarInt(inputStream));
  //          }
  //          break;
  //        case INT64:
  //        case TIMESTAMP:
  //          while (inputStream.available() > 0) {
  //            long value = 0;
  //            for (int i = 0; i < 8; i++) {
  //              value = (value << 8) | (inputStream.read() & 0xFF);
  //            }
  //            result.add(value);
  //          }
  //          break;
  //        case FLOAT:
  //          while (inputStream.available() > 0) {
  //            int bits = 0;
  //            for (int i = 0; i < 4; i++) {
  //              bits = (bits << 8) | (inputStream.read() & 0xFF);
  //            }
  //            result.add(Float.intBitsToFloat(bits));
  //          }
  //          break;
  //        case DOUBLE:
  //          while (inputStream.available() > 0) {
  //            long bits = 0;
  //            for (int i = 0; i < 8; i++) {
  //              bits = (bits << 8) | (inputStream.read() & 0xFF);
  //            }
  //            result.add(Double.longBitsToDouble(bits));
  //          }
  //          break;
  //        case TEXT:
  //        case STRING:
  //        case BLOB:
  //          while (inputStream.available() > 0) {
  //            int length = ReadWriteForEncodingUtils.readVarInt(inputStream);
  //            byte[] bytes = new byte[length];
  //            inputStream.read(bytes);
  //            result.add(new Binary(bytes));
  //          }
  //          break;
  //        default:
  //          throw new UnsupportedOperationException("PLAIN doesn't support data type: " +
  // dataType);
  //      }
  //      return result;
  //    } finally {
  //      inputStream.close();
  //    }
  //  }

  @Override
  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  public TSEncoding getEncodingType() {
    return TSEncoding.PLAIN;
  }

  @Override
  public Encoder getEncoder(TSDataType type, TSEncoding encodingType) {
    return new PlainEncoder(type, DEFAULT_MAX_STRING_LENGTH);
  }
}
