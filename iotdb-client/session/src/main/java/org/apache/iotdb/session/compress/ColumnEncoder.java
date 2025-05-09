package org.apache.iotdb.session.compress;

import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.util.List;

/** Column encoder interface, which defines the encoding and decoding operations of column data */
public interface ColumnEncoder {

  /**
   * Encode a column of data
   *
   * @param data The data column to be encoded
   * @return Encoded data
   */
  byte[] encode(List<?> data);

  TSDataType getDataType();

  TSEncoding getEncodingType();

  Encoder getEncoder(TSDataType type, TSEncoding encodingType);

  ColumnEntry getColumnEntry();
}

/** Encoding type not supported exception */
class EncodingTypeNotSupportedException extends RuntimeException {
  public EncodingTypeNotSupportedException(String message) {
    super("Encoding type " + message + " is not supported.");
  }
}
