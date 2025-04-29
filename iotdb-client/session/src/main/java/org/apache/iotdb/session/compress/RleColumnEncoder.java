package org.apache.iotdb.session.compress;

import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.PublicBAOS;

import java.io.IOException;
import java.util.List;

public class RleColumnEncoder implements ColumnEncoder {
  private final Encoder encoder;
  private final TSDataType dataType;

  public RleColumnEncoder(TSDataType dataType) {
    this.dataType = dataType;
    this.encoder = getEncoder(dataType, TSEncoding.RLE);
  }

  @Override
  public byte[] encode(List<?> data) throws IOException {
    if (data == null || data.isEmpty()) {
      return new byte[0];
    }
    PublicBAOS outputStream = new PublicBAOS();
    try {
      switch (dataType) {
        case INT32:
        case DATE:
        case BOOLEAN:
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
              encoder.encode((long) value, outputStream);
            }
          }
          break;
        case FLOAT:
          for (Object value : data) {
            if (value != null) {
              encoder.encode((float) value, outputStream);
            }
          }
          break;
        case DOUBLE:
          for (Object value : data) {
            if (value != null) {
              encoder.encode((double) value, outputStream);
            }
          }
          break;
        default:
          throw new UnsupportedOperationException("RLE doesn't support data type: " + dataType);
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
  //      //......
  //    } finally {
  //      inputStream.close();
  //    }
  //    return List.of();
  //  }

  @Override
  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  public TSEncoding getEncodingType() {
    return TSEncoding.RLE;
  }

  @Override
  public Encoder getEncoder(TSDataType type, TSEncoding encodingType) {
    return TSEncodingBuilder.getEncodingBuilder(encodingType).getEncoder(type);
  }
}
