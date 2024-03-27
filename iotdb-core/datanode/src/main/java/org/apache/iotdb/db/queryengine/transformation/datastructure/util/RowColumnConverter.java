package org.apache.iotdb.db.queryengine.transformation.datastructure.util;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.*;
import org.apache.iotdb.tsfile.utils.Binary;

public class RowColumnConverter {
  public static ColumnBuilder[] constructColumnBuilders(TSDataType[] dataTypes, int expectedEntries) {
    ColumnBuilder[] builders = new ColumnBuilder[dataTypes.length + 1];
    // Value column builders
    for (int i = 0; i < dataTypes.length; i++) {
      switch (dataTypes[i]) {
        case INT32:
          builders[i] = new IntColumnBuilder(null, expectedEntries);
          break;
        case INT64:
          builders[i] = new LongColumnBuilder(null, expectedEntries);
          break;
        case FLOAT:
          builders[i] = new FloatColumnBuilder(null, expectedEntries);
          break;
        case DOUBLE:
          builders[i] = new DoubleColumnBuilder(null, expectedEntries);
          break;
        case BOOLEAN:
          builders[i] = new BooleanColumnBuilder(null, expectedEntries);
          break;
        case TEXT:
          builders[i] = new BinaryColumnBuilder(null, expectedEntries);
          break;
        default:
          throw new UnSupportedDataTypeException(dataTypes[i].toString());
      }
    }
    // Time column builder
    builders[dataTypes.length] = new TimeColumnBuilder(null, expectedEntries);

    return builders;
  }

  public static void appendRowInColumnBuilders(TSDataType[] dataTypes, Object[] row, ColumnBuilder[] builders) {
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
    builders[dataTypes.length].writeLong((long)row[dataTypes.length]);
  }

  public static Column[] buildColumnsByBuilders(TSDataType[] dataTypes, ColumnBuilder[] builders) {
    Column[] columns = new Column[dataTypes.length + 1];
    for (int i = 0; i < dataTypes.length + 1; i++) {
      columns[i] = builders[i].build();
    }

    return columns;
  }
}
