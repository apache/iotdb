package org.apache.iotdb.session.rpccompress.decoder;

import org.apache.iotdb.session.rpccompress.ColumnEntry;

import org.apache.tsfile.encoding.decoder.*;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;

public class SprintzColumnDecoder implements ColumnDecoder {
  private final Decoder decoder;
  private final TSDataType dataType;

  public SprintzColumnDecoder(TSDataType dataType) {
    this.dataType = dataType;
    this.decoder = getDecoder(dataType, TSEncoding.SPRINTZ);
  }

  @Override
  public Object decode(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    switch (dataType) {
      case INT32:
      case DATE:
        return decodeIntColumn(buffer, columnEntry, rowCount);
      case INT64:
      case TIMESTAMP:
        return decodeLongColumn(buffer, columnEntry, rowCount);
      case FLOAT:
        return decodeFloatColumn(buffer, columnEntry, rowCount);
      case DOUBLE:
        return decodeDoubleColumn(buffer, columnEntry, rowCount);
      default:
        throw new TsFileDecodingException("Sprintz doesn't support data type: " + dataType);
    }
  }

  @Override
  public boolean[] decodeBooleanColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    throw new UnSupportedDataTypeException("Sprintz doesn't support data type: " + dataType);
  }

  @Override
  public int[] decodeIntColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    int[] result = new int[rowCount];
    for (int i = 0; i < rowCount; i++) {
      result[i] = decoder.readInt(buffer);
    }
    return result;
  }

  @Override
  public long[] decodeLongColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    long[] result = new long[rowCount];
    for (int i = 0; i < rowCount; i++) {
      result[i] = decoder.readLong(buffer);
    }
    return result;
  }

  @Override
  public float[] decodeFloatColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    float[] result = new float[rowCount];
    for (int i = 0; i < rowCount; i++) {
      result[i] = decoder.readFloat(buffer);
    }
    return result;
  }

  @Override
  public double[] decodeDoubleColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    double[] result = new double[rowCount];
    for (int i = 0; i < rowCount; i++) {
      result[i] = decoder.readDouble(buffer);
    }
    return result;
  }

  @Override
  public Binary[] decodeBinaryColumn(ByteBuffer buffer, ColumnEntry columnEntry, int rowCount) {
    throw new UnSupportedDataTypeException("Sprintz doesn't support data type: " + dataType);
  }

  @Override
  public Decoder getDecoder(TSDataType type, TSEncoding encodingType) {
    return Decoder.getDecoderByType(encodingType, type);
  }
}
