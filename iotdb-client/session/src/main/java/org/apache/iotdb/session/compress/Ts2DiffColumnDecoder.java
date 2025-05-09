package org.apache.iotdb.session.compress;

import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Ts2DiffColumnDecoder implements ColumnDecoder {
  private final Decoder decoder;
  private final TSDataType dataType;

  public Ts2DiffColumnDecoder(TSDataType dataType) {
    this.dataType = dataType;
    this.decoder = getDecoder(dataType, TSEncoding.TS_2DIFF);
  }

  @Override
  public List<?> decode(ByteBuffer buffer, ColumnEntry columnEntry) {
    int count = columnEntry.getSize();
    switch (dataType) {
      case BOOLEAN:
        {
          List<Boolean> result = new ArrayList<>(count);
          for (int i = 0; i < count; i++) {
            result.add(decoder.readBoolean(buffer));
          }
          return result;
        }
      case INT32:
      case DATE:
        {
          List<Integer> result = new ArrayList<>(count);
          for (int i = 0; i < count; i++) {
            result.add(decoder.readInt(buffer));
          }
          return result;
        }
      case INT64:
      case TIMESTAMP:
        {
          List<Long> result = new ArrayList<>(count);
          for (int i = 0; i < count; i++) {
            result.add(decoder.readLong(buffer));
          }
          return result;
        }
      case FLOAT:
        {
          List<Float> result = new ArrayList<>(count);
          for (int i = 0; i < count; i++) {
            result.add(decoder.readFloat(buffer));
          }
          return result;
        }
      case DOUBLE:
        {
          List<Double> result = new ArrayList<>(count);
          for (int i = 0; i < count; i++) {
            result.add(decoder.readDouble(buffer));
          }
          return result;
        }
      case TEXT:
      case STRING:
      case BLOB:
        {
          List<Binary> result = new ArrayList<>(count);
          for (int i = 0; i < count; i++) {
            Binary binary = decoder.readBinary(buffer);
            result.add(binary);
          }
          return result;
        }
      default:
        throw new UnsupportedOperationException("PLAIN doesn't support data type: " + dataType);
    }
  }

  @Override
  public Decoder getDecoder(TSDataType type, TSEncoding encodingType) {
    return null;
  }
}
