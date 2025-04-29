package org.apache.iotdb.session.compress;

import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.util.List;

/** 列式编码器接口，定义了列数据的编码和解码操作 */
public interface ColumnEncoder {

  /**
   * 对一列数据进行编码
   *
   * @param data 待编码的数据列
   * @return 编码后的数据
   * @throws IOException 如果编码过程中发生IO错误
   */
  byte[] encode(List<?> data) throws IOException;

  /**
   * 对编码后的数据进行解码(未实现)
   *
   * @param data 待解码的数据
   * @return 解码后的数据列
   * @throws IOException 如果解码过程中发生IO错误
   */
  //    List<?> decode(byte[] data) throws IOException;

  /**
   * 获取数据类型
   *
   * @return 数据类型
   */
  TSDataType getDataType();

  /**
   * 获取编码类型
   *
   * @return 编码类型
   */
  TSEncoding getEncodingType();

  /**
   * 获取底层编码器实例
   *
   * @return 编码器实例
   */
  Encoder getEncoder(TSDataType type, TSEncoding encodingType);
}

/** 编码类型不支持异常 */
class EncodingTypeNotSupportedException extends RuntimeException {
  public EncodingTypeNotSupportedException(String message) {
    super("Encoding type " + message + " is not supported.");
  }
}
