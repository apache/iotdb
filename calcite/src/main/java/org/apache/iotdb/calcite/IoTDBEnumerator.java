/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.calcite;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public class IoTDBEnumerator implements Enumerator<Object> {

  private ResultSet currentResultSet;
  private Iterator<ResultSet> iterator;
  private List<Integer> indexInResultSet = new ArrayList<>();
  private Row current;
  private List<RelDataTypeField> fieldTypes;

  /**
   * Creates a IoTDBEnumerator.
   *
   * @param resultList   IoTDB result set
   * @param protoRowType The type of protecting rows
   */
  IoTDBEnumerator(List<ResultSet> resultList, RelProtoDataType protoRowType) throws SQLException {
    this.iterator = resultList.iterator();
    if (iterator.hasNext()) {
      this.currentResultSet = iterator.next();
    }
    this.current = null;

    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    this.fieldTypes = protoRowType.apply(typeFactory).getFieldList();

    // get the corresponding index of project columns in result set
    // as we have 'time' and 'device' columns in result that the project columns may don't include
    // e.g. If resultSet includes [time, device, s0, s1], project columns only include [device, s0, s1],
    // then the indexInResultSet is [2, 3, 4].
    ResultSetMetaData metaData = currentResultSet.getMetaData();
    int indexInFieldTypes = 0;
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      if (i <= 2 && !metaData.getColumnName(i).toLowerCase()
          .equals(fieldTypes.get(indexInFieldTypes).getName())) {
        continue;
      } else {
        indexInFieldTypes++;
        indexInResultSet.add(i);
      }
    }
  }

  /**
   * Produce and get the next row from the results
   *
   * @return A new row from the results
   */
  @Override
  public Object current() {
    if (fieldTypes.size() == 1) {
      // If we just have one field, produce it directly
      return currentRowField(indexInResultSet.get(0), fieldTypes.get(0).getType().getSqlTypeName());
    } else {
      // Build an array with all fields in this row
      Object[] row = new Object[fieldTypes.size()];
      for (int i = 0; i < fieldTypes.size(); i++) {
        row[i] = currentRowField(indexInResultSet.get(i),
            fieldTypes.get(i).getType().getSqlTypeName());
      }

      return row;
    }
  }

  /**
   * Get a field for the current row from the underlying object.
   *
   * @param index Index of the field within the Row object
   * @param type  Type of the field in this row
   */
  private Object currentRowField(int index, SqlTypeName type) {
    try {
      if (type == SqlTypeName.VARCHAR) {
        return currentResultSet.getString(index);
      } else if (type == SqlTypeName.INTEGER) {
        return currentResultSet.getInt(index);
      } else if (type == SqlTypeName.BIGINT) {
        return currentResultSet.getLong(index);
      } else if (type == SqlTypeName.DOUBLE) {
        return currentResultSet.getDouble(index);
      } else if (type == SqlTypeName.REAL) {
        return currentResultSet.getFloat(index);
      } else if (type == SqlTypeName.BOOLEAN) {
        return currentResultSet.getBoolean(index);
      } else {
        return null;
      }
    } catch (SQLException e) {
      if (e.getMessage().endsWith("NULL.")) {
        return null;
      } else {
        e.printStackTrace();
        return null;
      }
    }
  }

  /**
   * Advances the enumerator to the next element of the collection.
   *
   * @return whether the resultset has next element
   */
  @Override
  public boolean moveNext() {
    try {
      if (currentResultSet.next()) {
        return true;
      } else if (iterator.hasNext()) {
        currentResultSet = iterator.next();
        return moveNext();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * Sets the enumerator to its initial position, which is before the first element in the
   * collection.
   */
  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }

  /**
   * Closes this enumerable and releases resources.
   */
  @Override
  public void close() {
    // do nothing here
  }
}

// End IoTDBEnumerator.java