/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.session;

import static org.apache.iotdb.rpc.AbstractIoTDBDataSet.START_INDEX;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.rpc.AbstractIoTDBDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.thrift.TException;

public class SessionDataSet {

  private final AbstractIoTDBDataSet abstractIoTDBDataSet;

  public SessionDataSet(String sql, List<String> columnNameList, List<String> columnTypeList,
      Map<String, Integer> columnNameIndex,
      long queryId, TSIService.Iface client, long sessionId, TSQueryDataSet queryDataSet) {
    this.abstractIoTDBDataSet = new AbstractIoTDBDataSet(sql, columnNameList, columnTypeList,
        columnNameIndex, false, queryId, client, sessionId, queryDataSet, 1024);
  }

  public int getFetchSize() {
    return abstractIoTDBDataSet.fetchSize;
  }

  public void setFetchSize(int fetchSize) {
    abstractIoTDBDataSet.fetchSize = fetchSize;
  }

  public List<String> getColumnNames() {
    return abstractIoTDBDataSet.columnNameList;
  }


  public boolean hasNext() throws StatementExecutionException, IoTDBConnectionException {
    return abstractIoTDBDataSet.next();
  }


  private RowRecord constructRowRecordFromValueArray() {
    List<Field> outFields = new ArrayList<>();
    for (int i = 0; i < abstractIoTDBDataSet.columnSize; i++) {
      Field field;

      int loc =
          abstractIoTDBDataSet.columnOrdinalMap.get(abstractIoTDBDataSet.columnNameList.get(i + 1))
              - START_INDEX;
      byte[] valueBytes = abstractIoTDBDataSet.values[loc];

      if (valueBytes != null) {
        TSDataType dataType = abstractIoTDBDataSet.columnTypeDeduplicatedList.get(loc);
        field = new Field(dataType);
        switch (dataType) {
          case BOOLEAN:
            boolean booleanValue = BytesUtils.bytesToBool(valueBytes);
            field.setBoolV(booleanValue);
            break;
          case INT32:
            int intValue = BytesUtils.bytesToInt(valueBytes);
            field.setIntV(intValue);
            break;
          case INT64:
            long longValue = BytesUtils.bytesToLong(valueBytes);
            field.setLongV(longValue);
            break;
          case FLOAT:
            float floatValue = BytesUtils.bytesToFloat(valueBytes);
            field.setFloatV(floatValue);
            break;
          case DOUBLE:
            double doubleValue = BytesUtils.bytesToDouble(valueBytes);
            field.setDoubleV(doubleValue);
            break;
          case TEXT:
            field.setBinaryV(new Binary(valueBytes));
            break;
          default:
            throw new UnSupportedDataTypeException(String
                .format("Data type %s is not supported.",
                    abstractIoTDBDataSet.columnTypeDeduplicatedList.get(i)));
        }
      } else {
        field = new Field(null);
      }
      outFields.add(field);
    }
    return new RowRecord(BytesUtils.bytesToLong(abstractIoTDBDataSet.time), outFields);
  }


  public RowRecord next() throws StatementExecutionException, IoTDBConnectionException {
    if (!abstractIoTDBDataSet.hasCachedRecord) {
      if (!hasNext()) {
        return null;
      }
    }
    abstractIoTDBDataSet.hasCachedRecord = false;

    return constructRowRecordFromValueArray();
  }

  public void closeOperationHandle() throws StatementExecutionException, IoTDBConnectionException {
    try {
      abstractIoTDBDataSet.close();
    } catch (TException e) {
      throw new IoTDBConnectionException(e.getMessage());
    }
  }

  public DataIterator iterator() {
    return new DataIterator();
  }

  public class DataIterator {

    public boolean next() throws StatementExecutionException, IoTDBConnectionException {
      return abstractIoTDBDataSet.next();
    }

    public boolean getBoolean(int columnIndex) throws StatementExecutionException {
      return abstractIoTDBDataSet.getBoolean(columnIndex);
    }

    public boolean getBoolean(String columnName) throws StatementExecutionException {
      return abstractIoTDBDataSet.getBoolean(columnName);
    }

    public double getDouble(int columnIndex) throws StatementExecutionException {
      return abstractIoTDBDataSet.getDouble(columnIndex);
    }

    public double getDouble(String columnName) throws StatementExecutionException {
      return abstractIoTDBDataSet.getDouble(columnName);
    }

    public float getFloat(int columnIndex) throws StatementExecutionException {
      return abstractIoTDBDataSet.getFloat(columnIndex);
    }

    public float getFloat(String columnName) throws StatementExecutionException {
      return abstractIoTDBDataSet.getFloat(columnName);
    }

    public int getInt(int columnIndex) throws StatementExecutionException {
      return abstractIoTDBDataSet.getInt(columnIndex);
    }

    public int getInt(String columnName) throws StatementExecutionException {
      return abstractIoTDBDataSet.getInt(columnName);
    }

    public long getLong(int columnIndex) throws StatementExecutionException {
      return abstractIoTDBDataSet.getLong(columnIndex);
    }

    public long getLong(String columnName) throws StatementExecutionException {
      return abstractIoTDBDataSet.getLong(columnName);
    }

    public Object getObject(int columnIndex) throws StatementExecutionException {
      return abstractIoTDBDataSet.getObject(columnIndex);
    }

    public Object getObject(String columnName) throws StatementExecutionException {
      return abstractIoTDBDataSet.getObject(columnName);
    }

    public String getString(int columnIndex) throws StatementExecutionException {
      return abstractIoTDBDataSet.getString(columnIndex);
    }

    public String getString(String columnName) throws StatementExecutionException {
      return abstractIoTDBDataSet.getString(columnName);
    }

    public Timestamp getTimestamp(int columnIndex) throws StatementExecutionException {
      return abstractIoTDBDataSet.getTimestamp(columnIndex);
    }

    public Timestamp getTimestamp(String columnName) throws StatementExecutionException {
      return abstractIoTDBDataSet.getTimestamp(columnName);
    }

    public int findColumn(String columnName) {
      return abstractIoTDBDataSet.findColumn(columnName);
    }
  }
}
