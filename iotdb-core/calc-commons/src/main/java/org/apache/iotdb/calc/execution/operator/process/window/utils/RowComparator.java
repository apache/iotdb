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

package org.apache.iotdb.calc.execution.operator.process.window.utils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;

import java.util.List;

public class RowComparator {
  private final List<TSDataType> dataTypes;

  public RowComparator(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public boolean equalColumns(List<Column> columns, int offset1, int offset2) {
    for (int i = 0; i < dataTypes.size(); i++) {
      Column column = columns.get(i);
      if (!equal(column, offset1, offset2)) {
        return false;
      }
    }
    return true;
  }

  private boolean equal(Column column, int offset1, int offset2) {
    if (offset1 == offset2) {
      return true;
    }

    if (column.isNull(offset1) || column.isNull(offset2)) {
      return column.isNull(offset1) && column.isNull(offset2);
    }

    return column.arePositionsEqual(offset1, offset2);
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

    return column.arePositionsEqual(offset1, offset2);
  }

  public boolean equal(List<Column> columns1, int offset1, List<Column> columns2, int offset2) {
    for (int i = 0; i < dataTypes.size(); i++) {
      Column column1 = columns1.get(i);
      Column column2 = columns2.get(i);

      if (column1.isNull(offset1) || column2.isNull(offset2)) {
        return column1.isNull(offset1) && column2.isNull(offset2);
      }

      if (!column1.arePositionsEqual(offset1, column2, offset2)) {
        return false;
      }
    }

    return true;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }
}
