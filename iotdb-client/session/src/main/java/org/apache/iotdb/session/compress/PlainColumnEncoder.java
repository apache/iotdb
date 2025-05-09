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
  private ColumnEntry columnEntry;
  private static final int DEFAULT_MAX_STRING_LENGTH = 128;

  public PlainColumnEncoder(TSDataType dataType) {
    this.dataType = dataType;
    this.encoder = new PlainEncoder(dataType, DEFAULT_MAX_STRING_LENGTH);
  }

  @Override
  public byte[] encode(List<?> data) {
    if (data == null || data.isEmpty()) {
      return new byte[0];
    }

    // Calculate the original data size
    int originalSize = 0;
    for (Object value : data) {
      if (value != null) {
        switch (dataType) {
          case BOOLEAN:
            originalSize += 1; // boolean 占用 1 字节
            break;
          case INT32:
          case DATE:
            originalSize += 4; // int32 占用 4 字节
            break;
          case INT64:
          case TIMESTAMP:
            originalSize += 8; // int64 占用 8 字节
            break;
          case FLOAT:
            originalSize += 4; // float 占用 4 字节
            break;
          case DOUBLE:
            originalSize += 8; // double 占用 8 字节
            break;
          case TEXT:
          case STRING:
          case BLOB:
            if (value instanceof String) {
              originalSize += ((String) value).getBytes().length;
            } else if (value instanceof Binary) {
              originalSize += ((Binary) value).getLength();
            }
            break;
        }
      }
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
              }
            }
          }
          break;
        default:
          throw new UnsupportedOperationException("PLAIN doesn't support data type: " + dataType);
      }
      encoder.flush(outputStream);
      byte[] encodedData = outputStream.toByteArray();

      ColumnEntry entry = new ColumnEntry();
      entry.setCompressedSize(encodedData.length);
      entry.setUnCompressedSize(originalSize);
      entry.setDataType(dataType);
      entry.setEncodingType(TSEncoding.PLAIN);

      return encodedData;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        outputStream.close();
      } catch (IOException e) {
      }
    }
  }

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

  @Override
  public ColumnEntry getColumnEntry() {
    return columnEntry;
  }
}
