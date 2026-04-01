/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

  public boolean equalColumns(List<Column> columns, int offset1, int offset2) {
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
    if (offset1 == offset2) {
      return true;
    }

    if (column.isNull(offset1) || column.isNull(offset2)) {
      return column.isNull(offset1) && column.isNull(offset2);
    }

    switch (dataType) {
      case BOOLEAN:
        boolean bool1 = column.getBoolean(offset1);
        boolean bool2 = column.getBoolean(offset2);
        if (bool1 != bool2) {
          return false;
        }
        break;
      case INT32:
      case DATE:
        int int1 = column.getInt(offset1);
        int int2 = column.getInt(offset2);
        if (int1 != int2) {
          return false;
        }
        break;
      case INT64:
      case TIMESTAMP:
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
      case STRING:
      case TEXT:
      case BLOB:
      case OBJECT:
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

  public boolean equalColumnLists(List<ColumnList> columns, int offset1, int offset2) {
    for (int i = 0; i < dataTypes.size(); i++) {
      ColumnList column = columns.get(i);
      TSDataType dataType = dataTypes.get(i);
      if (!equal(column, dataType, offset1, offset2)) {
        return false;
      }
    }
    return true;
  }

  public boolean equal(ColumnList column, int offset1, int offset2) {
    assert dataTypes.size() == 1;
    return equal(column, dataTypes.get(0), offset1, offset2);
  }

  private boolean equal(ColumnList column, TSDataType dataType, int offset1, int offset2) {
    if (offset1 == offset2) {
      return true;
    }

    if (column.isNull(offset1) || column.isNull(offset2)) {
      return column.isNull(offset1) && column.isNull(offset2);
    }

    switch (dataType) {
      case BOOLEAN:
        boolean bool1 = column.getBoolean(offset1);
        boolean bool2 = column.getBoolean(offset2);
        if (bool1 != bool2) {
          return false;
        }
        break;
      case INT32:
      case DATE:
        int int1 = column.getInt(offset1);
        int int2 = column.getInt(offset2);
        if (int1 != int2) {
          return false;
        }
        break;
      case INT64:
      case TIMESTAMP:
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
      case STRING:
      case BLOB:
      case OBJECT:
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

  public boolean equal(List<Column> columns1, int offset1, List<Column> columns2, int offset2) {
    for (int i = 0; i < dataTypes.size(); i++) {
      TSDataType dataType = dataTypes.get(i);
      Column column1 = columns1.get(i);
      Column column2 = columns2.get(i);

      if (column1.isNull(offset1) || column2.isNull(offset2)) {
        return column1.isNull(offset1) && column2.isNull(offset2);
      }

      switch (dataType) {
        case BOOLEAN:
          boolean bool1 = column1.getBoolean(offset1);
          boolean bool2 = column2.getBoolean(offset2);
          if (bool1 != bool2) {
            return false;
          }
          break;
        case INT32:
        case DATE:
          int int1 = column1.getInt(offset1);
          int int2 = column2.getInt(offset2);
          if (int1 != int2) {
            return false;
          }
          break;
        case INT64:
        case TIMESTAMP:
          long long1 = column1.getLong(offset1);
          long long2 = column2.getLong(offset2);
          if (long1 != long2) {
            return false;
          }
          break;
        case FLOAT:
          float float1 = column1.getFloat(offset1);
          float float2 = column2.getFloat(offset2);
          if (float1 != float2) {
            return false;
          }
          break;
        case DOUBLE:
          double double1 = column1.getDouble(offset1);
          double double2 = column2.getDouble(offset2);
          if (double1 != double2) {
            return false;
          }
          break;
        case TEXT:
        case STRING:
        case BLOB:
        case OBJECT:
          Binary bin1 = column1.getBinary(offset1);
          Binary bin2 = column2.getBinary(offset2);
          if (!bin1.equals(bin2)) {
            return false;
          }
          break;
        default:
          // Would throw at the first run
          throw new UnSupportedDataTypeException(dataType.toString());
      }
    }

    return true;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }
}
