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

package org.apache.iotdb.session;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.NumericRow;
import org.apache.iotdb.isession.NumericRowPreparedStatement;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSPrepareNumericRowReq;
import org.apache.iotdb.session.i18n.SessionMessages;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TableSession implements ITableSession {

  private final Session session;

  public TableSession(Session session) {
    this.session = session;
  }

  @Override
  public void insert(Tablet tablet) throws StatementExecutionException, IoTDBConnectionException {
    session.insertRelationalTablet(tablet);
  }

  @Override
  public void insert(NumericRow row) throws StatementExecutionException, IoTDBConnectionException {
    session.insertRelationalRecord(row);
  }

  @Override
  public NumericRowPreparedStatement prepareInsertNumericRow(
      String tableName,
      List<String> columnNames,
      List<TSDataType> dataTypes,
      List<ColumnCategory> columnCategories)
      throws StatementExecutionException, IoTDBConnectionException {
    List<String> names = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    List<ColumnCategory> categories = new ArrayList<>();
    for (ColumnCategory category :
        new ColumnCategory[] {ColumnCategory.TAG, ColumnCategory.FIELD}) {
      for (int i = 0; i < columnNames.size(); i++) {
        if (columnCategories.get(i) == category) {
          names.add(columnNames.get(i));
          types.add(dataTypes.get(i));
          categories.add(category);
        }
      }
    }
    TSPrepareNumericRowReq request = new TSPrepareNumericRowReq();
    request
        .setTableName(tableName)
        .setMeasurements(names)
        .setDataTypes(types.stream().map(TSDataType::ordinal).toList())
        .setColumnCategories(categories.stream().map(c -> (byte) c.ordinal()).toList());
    NumericRowPreparedStatement prepared =
        new NumericRowPreparedStatement(tableName, names, types, categories);
    SessionConnection connection = session.getDefaultSessionConnection();
    prepared.bind(
        connection.prepareNumericRow(request),
        connection,
        () -> {
          try {
            connection.closePreparedNumericRow(prepared.getPreparedId());
          } catch (IoTDBConnectionException ignored) {
            // Closing the session releases all server-side prepared rows.
          }
        });
    return prepared;
  }

  @Override
  public void executePreparedInsertNumericRow(
      NumericRowPreparedStatement prepared, long timestamp, long[] values, String[] tagValues)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionConnection connection = session.getDefaultSessionConnection();
    if (prepared == null || prepared.isClosed() || prepared.getOwner() != connection) {
      throw new IoTDBConnectionException(
          SessionMessages
              .EXCEPTION_PREPARED_NUMERIC_ROW_IS_CLOSED_OR_BELONGS_TO_ANOTHER_CONNECTION_PREPARE_IT_AGAIN_055CC7BC);
    }
    int tagCount = prepared.getTagCount();
    int fieldCount = prepared.getDataTypes().size() - tagCount;
    if (values.length != fieldCount || tagValues.length != tagCount) {
      throw new IllegalArgumentException(
          SessionMessages.EXCEPTION_PREPARED_NUMERIC_ROW_VALUE_COUNT_MISMATCH_9ACD3A0C);
    }
    int tagBytes = 0;
    for (String tag : tagValues) {
      tagBytes += ReadWriteIOUtils.sizeToWrite(tag);
    }
    ByteBuffer tagBuffer = prepared.prepareTagBuffer(tagBytes);
    for (String tag : tagValues) {
      ReadWriteIOUtils.write(tag, tagBuffer);
    }
    tagBuffer.flip();
    ByteBuffer valueBuffer = prepared.prepareValueBuffer();
    int valueIndex = 0;
    for (int i = 0; i < prepared.getDataTypes().size(); i++) {
      if (prepared.getColumnCategories().get(i) == ColumnCategory.FIELD) {
        long value = values[valueIndex++];
        switch (prepared.getDataTypes().get(i)) {
          case BOOLEAN -> valueBuffer.put((byte) (value == 0 ? 0 : 1));
          case INT32, DATE, FLOAT -> valueBuffer.putInt((int) value);
          case INT64, TIMESTAMP, DOUBLE -> valueBuffer.putLong(value);
          default ->
              throw new IllegalArgumentException(
                  SessionMessages.EXCEPTION_UNSUPPORTED_NUMERIC_TYPE_06DD3044);
        }
      }
    }
    valueBuffer.flip();
    connection.executePreparedNumericRow(
        prepared.getPreparedId(), timestamp, valueBuffer, tagBuffer);
  }

  @Override
  public void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement(sql);
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    return session.executeQueryStatement(sql);
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws StatementExecutionException, IoTDBConnectionException {
    return session.executeQueryStatement(sql, timeoutInMs);
  }

  @Override
  public void close() throws IoTDBConnectionException {
    session.close();
  }
}
