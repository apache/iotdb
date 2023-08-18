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
import org.apache.iotdb.flink.sql.wrapper.SchemaWrapper;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.curator5.com.google.common.cache.Cache;
import org.apache.flink.shaded.curator5.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class IoTDBLookupFunction extends TableFunction<RowData> {
  private final List<Tuple2<String, DataType>> SCHEMA;
  private final int CACHE_MAX_ROWS;
  private final int CACHE_TTL_SEC;
  private final List<String> NODE_URLS;
  private final String USER;
  private final String PASSWORD;
  private final String DEVICE;
  private final List<String> MEASUREMENTS;
  private Session session;

  private transient Cache<RowData, RowData> cache;

  public IoTDBLookupFunction(ReadableConfig options, SchemaWrapper schemaWrapper) {
    this.SCHEMA = schemaWrapper.getSchema();

    CACHE_MAX_ROWS = options.get(Options.LOOKUP_CACHE_MAX_ROWS);

    CACHE_TTL_SEC = options.get(Options.LOOKUP_CACHE_TTL_SEC);

    NODE_URLS = Arrays.asList(options.get(Options.NODE_URLS).split(","));

    USER = options.get(Options.USER);

    PASSWORD = options.get(Options.PASSWORD);

    DEVICE = options.get(Options.DEVICE);

    MEASUREMENTS =
        SCHEMA.stream().map(field -> String.valueOf(field.f0)).collect(Collectors.toList());
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    session = new Session.Builder().nodeUrls(NODE_URLS).username(USER).password(PASSWORD).build();
    session.open(false);

    if (CACHE_MAX_ROWS > 0 && CACHE_TTL_SEC > 0) {
      cache =
          CacheBuilder.newBuilder()
              .expireAfterAccess(CACHE_TTL_SEC, TimeUnit.SECONDS)
              .maximumSize(CACHE_MAX_ROWS)
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

    String sql =
        String.format(
            "SELECT %s FROM %s WHERE TIME=%d",
            StringUtils.join(MEASUREMENTS, ','), DEVICE, timestamp);
    SessionDataSet dataSet = session.executeQueryStatement(sql);
    List<String> columnNames = dataSet.getColumnNames();
    columnNames.remove("Time");
    RowRecord record = dataSet.next();
    if (record == null) {
      ArrayList<Object> values = new ArrayList<>();
      values.add(timestamp);
      for (int i = 0; i < SCHEMA.size(); i++) {
        values.add(null);
      }
      GenericRowData rowData = GenericRowData.of(values.toArray());
      collect(rowData);
      return;
    }
    List<Field> fields = record.getFields();

    ArrayList<Object> values = new ArrayList<>();
    values.add(timestamp);
    for (Tuple2<String, DataType> filed : SCHEMA) {
      values.add(
          Utils.getValue(fields.get(columnNames.indexOf(DEVICE + '.' + filed.f0)), filed.f1));
    }

    GenericRowData rowData = GenericRowData.of(values.toArray());
    if (cache != null) {
      cache.put(lookupKey, rowData);
    }
    collect(rowData);
  }
}
