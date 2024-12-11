package org.apache.iotdb.db.queryengine.execution.operator.process.window.utils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.List;

public class RowComparator {
  private final List<TSDataType> dataTypes;

  public RowComparator(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public boolean equal(List<Column> columns, int offset1, int offset2) {
    for (int i = 0; i < dataTypes.size(); i++) {
      Column column = columns.get(i);
      TSDataType dataType = dataTypes.get(i);
      if (!equal(column, dataType, offset1, offset2)) {
        return false;
      }
    }
    return true;
  }

  public boolean equal(Column column, int offset1, int offset2) {
    assert dataTypes.size() == 1;
    return equal(column, dataTypes.get(0), offset1, offset2);
  }

  private boolean equal(Column column, TSDataType dataType, int offset1, int offset2) {
    switch (dataType) {
      case BOOLEAN:
        boolean bool1 = column.getBoolean(offset1);
        boolean bool2 = column.getBoolean(offset2);
        if (bool1 != bool2) {
          return false;
        }
        break;
      case INT32:
        int int1 = column.getInt(offset1);
        int int2 = column.getInt(offset2);
        if (int1 != int2) {
          return false;
        }
        break;
      case INT64:
        long long1 = column.getLong(offset1);
        long long2 = column.getLong(offset2);
        if (long1 != long2) {
          return false;
        }
        break;
      case FLOAT:
        float float1 = column.getFloat(offset1);
        float float2 = column.getFloat(offset2);
        if (float1 != float2) {
          return false;
        }
        break;
      case DOUBLE:
        double double1 = column.getDouble(offset1);
        double double2 = column.getDouble(offset2);
        if (double1 != double2) {
          return false;
        }
        break;
      case TEXT:
        Binary bin1 = column.getBinary(offset1);
        Binary bin2 = column.getBinary(offset2);
        if (!bin1.equals(bin2)) {
          return false;
        }
        break;
      default:
        // Would throw at the first run
        throw new UnSupportedDataTypeException(dataType.toString());
    }
    return true;
  }
}
