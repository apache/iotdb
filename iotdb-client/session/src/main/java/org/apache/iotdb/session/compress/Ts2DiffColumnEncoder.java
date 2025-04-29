package org.apache.iotdb.session.compress;

import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.util.List;

public class Ts2DiffColumnEncoder implements ColumnEncoder {
  private final Encoder encoder;
  private final TSDataType dataType;

  public Ts2DiffColumnEncoder(TSDataType dataType) {
    this.dataType = dataType;
    this.encoder = getEncoder(dataType, TSEncoding.RLE);
  }

  @Override
  public byte[] encode(List<?> data) throws IOException {
    return new byte[0];
  }

  //  @Override
  //  public List<?> decode(byte[] data) throws IOException {
  //    return List.of();
  //  }

  @Override
  public TSDataType getDataType() {
    return null;
  }

  @Override
  public TSEncoding getEncodingType() {
    return null;
  }

  @Override
  public Encoder getEncoder(TSDataType type, TSEncoding encodingType) {
    return null;
  }
}
