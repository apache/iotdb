package org.apache.iotdb.db.queryengine.transformation.datastructure.util;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.*;
import org.apache.iotdb.tsfile.utils.Binary;

public class RowColumnConverter {
  public static TimeColumnBuilder constructTimeColumnBuilder(int expectedEntries) {
    return new TimeColumnBuilder(null, expectedEntries);
  }

  public static ColumnBuilder constructValueColumnBuilder(
      TSDataType dataType, int expectedEntries) {
    switch (dataType) {
      case INT32:
        return new IntColumnBuilder(null, expectedEntries);
      case INT64:
        return new LongColumnBuilder(null, expectedEntries);
      case FLOAT:
        return new FloatColumnBuilder(null, expectedEntries);
      case DOUBLE:
        return new DoubleColumnBuilder(null, expectedEntries);
      case BOOLEAN:
        return new BooleanColumnBuilder(null, expectedEntries);
      case TEXT:
        return new BinaryColumnBuilder(null, expectedEntries);
      default:
        throw new UnSupportedDataTypeException(dataType.toString());
    }
  }

  public static ColumnBuilder[] constructColumnBuilders(
      TSDataType[] dataTypes, int expectedEntries) {
    ColumnBuilder[] builders = new ColumnBuilder[dataTypes.length + 1];
    // Value column builders
    for (int i = 0; i < dataTypes.length; i++) {
      builders[i] = constructValueColumnBuilder(dataTypes[i], expectedEntries);
    }
    // Time column builder
    builders[dataTypes.length] = new TimeColumnBuilder(null, expectedEntries);

    return builders;
  }

  public static void appendRowInColumnBuilders(
      TSDataType[] dataTypes, Object[] row, ColumnBuilder[] builders) {
    // Write value field
    for (int i = 0; i < dataTypes.length; i++) {
      Object field = row[i];
      switch (dataTypes[i]) {
        case INT32:
          builders[i].writeInt((int) field);
          break;
        case INT64:
          builders[i].writeLong((long) field);
          break;
        case FLOAT:
          builders[i].writeFloat((float) field);
          break;
        case DOUBLE:
          builders[i].writeDouble((double) field);
          break;
        case BOOLEAN:
          builders[i].writeBoolean((boolean) field);
          break;
        case TEXT:
          builders[i].writeBinary((Binary) field);
          break;
        default:
          throw new UnSupportedDataTypeException(dataTypes[i].toString());
      }
    }
    // Write time field
    builders[dataTypes.length].writeLong((long) row[dataTypes.length]);
  }

  public static Column[] buildColumnsByBuilders(TSDataType[] dataTypes, ColumnBuilder[] builders) {
    Column[] columns = new Column[dataTypes.length + 1];
    for (int i = 0; i < dataTypes.length + 1; i++) {
      columns[i] = builders[i].build();
    }

    return columns;
  }
}
