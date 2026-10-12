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
import org.apache.iotdb.isession.NumericRowPreparedStatement;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSPrepareNumericRowReq;
import org.apache.iotdb.session.i18n.SessionMessages;
import org.apache.iotdb.session.util.SessionUtils;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
  public NumericRowPreparedStatement prepareInsertNumericRow(
      String tableName,
      List<String> columnNames,
      List<TSDataType> dataTypes,
      List<ColumnCategory> columnCategories)
      throws StatementExecutionException, IoTDBConnectionException {
    // Validate before regrouping, which would otherwise discard unsupported categories or
    // surplus metadata entries.
    if (tableName == null
        || columnNames == null
        || dataTypes == null
        || columnCategories == null
        || columnNames.size() != dataTypes.size()
        || columnNames.size() != columnCategories.size()) {
      throw new IllegalArgumentException(
          SessionMessages
              .EXCEPTION_NUMERIC_ROW_ARGUMENTS_MUST_BE_NON_NULL_COLUMN_METADATA_LENGTHS_AND_PER_CATEGORY_VALUE_COUNTS_MUST_MATCH_EB12B233);
    }
    for (int i = 0; i < columnNames.size(); i++) {
      if (columnNames.get(i) == null
          || dataTypes.get(i) == null
          || columnCategories.get(i) == null) {
        throw new IllegalArgumentException(
            SessionMessages
                .EXCEPTION_NUMERIC_ROW_ARGUMENTS_MUST_BE_NON_NULL_COLUMN_METADATA_LENGTHS_AND_PER_CATEGORY_VALUE_COUNTS_MUST_MATCH_EB12B233);
      }
      if ((columnCategories.get(i) != ColumnCategory.TAG
              && columnCategories.get(i) != ColumnCategory.FIELD)
          || (columnCategories.get(i) == ColumnCategory.TAG
              && dataTypes.get(i) != TSDataType.STRING)) {
        throw new IllegalArgumentException(
            SessionMessages
                .EXCEPTION_NUMERIC_ROWS_SUPPORT_NUMERIC_FIELD_COLUMNS_AND_STRING_TAG_COLUMNS_ONLY_87159FFA);
      }
    }
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
        new NumericRowPreparedStatement(tableName, names, types, categories, session.getDatabase());
    SessionConnection connection = session.getDefaultSessionConnection();
    long id = connection.prepareNumericRow(request);
    prepared.bind(
        id,
        connection,
        connection.getSessionId(),
        closePreparedRow(connection, connection.getSessionId(), id));
    return prepared;
  }

  @Override
  public void executePreparedInsertNumericRow(
      NumericRowPreparedStatement prepared, long timestamp, long[] values, String[] tagValues)
      throws StatementExecutionException, IoTDBConnectionException {
    executePreparedInsertNumericRow(prepared, timestamp, values, tagValues, null);
  }

  @Override
  public void executePreparedInsertNumericRow(
      NumericRowPreparedStatement prepared,
      long timestamp,
      long[] values,
      String[] tagValues,
      BitMap nullBitmap)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionConnection connection = session.getDefaultSessionConnection();
    if (prepared == null
        || prepared.isClosed()
        || prepared.getOwner() != connection
        || prepared.getPreparedId(connection, connection.getSessionId()) == null) {
      throw new IoTDBConnectionException(
          SessionMessages
              .EXCEPTION_PREPARED_NUMERIC_ROW_IS_CLOSED_OR_BELONGS_TO_ANOTHER_CONNECTION_PREPARE_IT_AGAIN_055CC7BC);
    }
    int tagCount = prepared.getTagCount();
    int fieldCount = prepared.getDataTypes().size() - tagCount;
    if (values == null
        || tagValues == null
        || values.length != fieldCount
        || tagValues.length != tagCount) {
      throw new IllegalArgumentException(
          SessionMessages.EXCEPTION_PREPARED_NUMERIC_ROW_VALUE_COUNT_MISMATCH_9ACD3A0C);
    }
    SessionUtils.validateNumericRowNullBitmap(nullBitmap, fieldCount);
    // A prepared handle remains bound to its original database after USE. Do not prepare it on
    // a redirected session whose database has changed in the meantime.
    IDeviceID deviceId = null;
    if (session.isEnableRedirection()
        && Objects.equals(prepared.getDatabase(), session.getDatabase())) {
      String[] segments = new String[tagValues.length + 1];
      segments[0] = prepared.getTableName();
      System.arraycopy(tagValues, 0, segments, 1, tagValues.length);
      deviceId = IDeviceID.Factory.DEFAULT_FACTORY.create(segments);
      connection = session.getSessionConnection(deviceId);
    }
    Long id = prepared.getPreparedId(connection, connection.getSessionId());
    if (id == null) {
      TSPrepareNumericRowReq request = new TSPrepareNumericRowReq();
      request
          .setTableName(prepared.getTableName())
          .setMeasurements(prepared.getColumnNames())
          .setDataTypes(prepared.getDataTypes().stream().map(TSDataType::ordinal).toList())
          .setColumnCategories(
              prepared.getColumnCategories().stream().map(c -> (byte) c.ordinal()).toList());
      // Prepare lazily on each destination; IDs must never cross physical sessions.
      id = connection.prepareNumericRow(request);
      prepared.bindConnection(
          connection,
          connection.getSessionId(),
          id,
          closePreparedRow(connection, connection.getSessionId(), id));
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
        if (nullBitmap != null && nullBitmap.isMarked(valueIndex - 1)) {
          continue;
        }
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
    try {
      if (nullBitmap == null) {
        connection.executePreparedNumericRow(id, timestamp, valueBuffer, tagBuffer);
      } else {
        connection.executePreparedNumericRow(
            id,
            timestamp,
            valueBuffer,
            tagBuffer,
            SessionUtils.encodeNumericRowNullBitmap(
                nullBitmap, prepared.prepareNullBitmapBuffer()));
      }
    } catch (RedirectException e) {
      // This row succeeded. Update only the route for subsequent rows, without replaying this one.
      if (deviceId != null && e.getEndPoint() != null) {
        session.handleRedirection(deviceId, e.getEndPoint());
      }
    } catch (StatementExecutionException e) {
      // A server may have invalidated its handle on schema failure. Do not let another cached
      // destination silently revive that logical statement; require an explicit new prepare.
      prepared.close();
      throw e;
    }
  }

  private static Runnable closePreparedRow(SessionConnection connection, long sessionId, long id) {
    return () -> {
      if (connection.getSessionId() != sessionId) {
        return;
      }
      try {
        connection.closePreparedNumericRow(id);
      } catch (IoTDBConnectionException ignored) {
        // Closing the physical session releases any handles left by a failed close RPC.
      }
    };
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
