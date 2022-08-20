package org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils;

import org.apache.iotdb.commons.udf.utils.UDFBinaryTransformer;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.type.Binary;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;
import java.util.Objects;

public class CodegenSimpleRow implements Row {

  Object[] values;
  long timestamp;
  TSDataType[] tsDataTypes;

  public CodegenSimpleRow(TSDataType[] tsDataTypes) {
    this.tsDataTypes = tsDataTypes;
  }

  public void setData(long timestamp, Object... values) {
    this.timestamp = timestamp;
    this.values = values;
  }

  @Override
  public long getTime() throws IOException {
    return timestamp;
  }

  @Override
  public int getInt(int columnIndex) throws IOException {
    return (int) values[columnIndex];
  }

  @Override
  public long getLong(int columnIndex) throws IOException {
    return (long) values[columnIndex];
  }

  @Override
  public float getFloat(int columnIndex) throws IOException {
    return (float) values[columnIndex];
  }

  @Override
  public double getDouble(int columnIndex) throws IOException {
    return (double) values[columnIndex];
  }

  @Override
  public boolean getBoolean(int columnIndex) throws IOException {
    return (boolean) values[columnIndex];
  }

  @Override
  public Binary getBinary(int columnIndex) throws IOException {
    return UDFBinaryTransformer.transformToUDFBinary(
        (org.apache.iotdb.tsfile.utils.Binary) values[columnIndex]);
  }

  @Override
  public String getString(int columnIndex) throws IOException {
    return ((org.apache.iotdb.tsfile.utils.Binary) values[columnIndex]).getStringValue();
  }

  @Override
  public Type getDataType(int columnIndex) {
    return UDFDataTypeTransformer.transformToUDFDataType(tsDataTypes[columnIndex]);
  }

  @Override
  public boolean isNull(int columnIndex) {
    return Objects.isNull(values[columnIndex]);
  }

  @Override
  public int size() {
    return this.tsDataTypes.length;
  }
}
