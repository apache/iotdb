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
package org.apache.iotdb.flink.sql.function;

import org.apache.iotdb.flink.sql.common.Options;
import org.apache.iotdb.flink.sql.common.Utils;
import org.apache.iotdb.flink.sql.exception.IllegalSchemaException;
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class IoTDBLookupFunction extends TableFunction<RowData> {
  private final List<Tuple2<String, DataType>> schema;
  private final int cacheMaxRows;
  private final int cacheTTLSec;
  private final List<String> nodeUrls;
  private final String user;
  private final String password;
  private final String sql;
  private Session session;

  private transient Cache<RowData, RowData> cache;

  public IoTDBLookupFunction(ReadableConfig options, SchemaWrapper schemaWrapper) {
    this.schema = schemaWrapper.getSchema();
    sql = options.get(Options.SQL);
    cacheMaxRows = options.get(Options.LOOKUP_CACHE_MAX_ROWS);
    cacheTTLSec = options.get(Options.LOOKUP_CACHE_TTL_SEC);
    nodeUrls = Arrays.asList(options.get(Options.NODE_URLS).split(","));
    user = options.get(Options.USER);
    password = options.get(Options.PASSWORD);
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    session = new Session.Builder().nodeUrls(nodeUrls).username(user).password(password).build();
    session.open(false);

    if (cacheMaxRows > 0 && cacheTTLSec > 0) {
      cache =
          CacheBuilder.newBuilder()
              .expireAfterAccess(cacheTTLSec, TimeUnit.SECONDS)
              .maximumSize(cacheMaxRows)
              .build();
    }
  }

  @Override
  public void close() throws Exception {
    if (cache != null) {
      cache.invalidateAll();
    }
    if (session != null) {
      session.close();
    }
    super.close();
  }

  public void eval(Object obj) throws IoTDBConnectionException, StatementExecutionException {
    RowData lookupKey = GenericRowData.of(obj);
    if (cache != null) {
      RowData cacheRow = cache.getIfPresent(lookupKey);
      if (cacheRow != null) {
        collect(cacheRow);
        return;
      }
    }

    long timestamp = lookupKey.getLong(0);

    String sql = String.format("%s WHERE TIME=%d", this.sql, timestamp);
    SessionDataSet dataSet = session.executeQueryStatement(sql);
    List<String> columnNames = dataSet.getColumnNames();
    columnNames.remove("Time");
    RowRecord rowRecord = dataSet.next();
    if (rowRecord == null) {
      ArrayList<Object> values = new ArrayList<>();
      values.add(timestamp);
      for (int i = 0; i < schema.size(); i++) {
        values.add(null);
      }
      GenericRowData rowData = GenericRowData.of(values.toArray());
      collect(rowData);
      return;
    }
    List<Field> fields = rowRecord.getFields();

    ArrayList<Object> values = new ArrayList<>();
    values.add(timestamp);
    for (Tuple2<String, DataType> field : schema) {
      if (!columnNames.contains(field.f0)) {
        values.add(null);
        continue;
      }
      int index = columnNames.indexOf(field.f0);
      DataType flinkType = field.f1;
      TSDataType iotdbType = fields.get(index).getDataType();
      if (!Utils.isTypeEqual(iotdbType, flinkType)) {
        throw new IllegalSchemaException(
            String.format(
                "The data type of column `%s` is different in IoTDB and Flink", field.f0));
      }
      values.add(Utils.getValue(fields.get(index), field.f1));
    }

    GenericRowData rowData = GenericRowData.of(values.toArray());
    if (cache != null) {
      cache.put(lookupKey, rowData);
    }
    collect(rowData);
  }
}
