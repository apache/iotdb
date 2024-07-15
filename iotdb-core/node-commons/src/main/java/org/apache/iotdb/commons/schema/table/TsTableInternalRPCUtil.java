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

package org.apache.iotdb.commons.schema.table;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsTableInternalRPCUtil {

  private TsTableInternalRPCUtil() {
    // do nothing
  }

  public static byte[] serializeBatchTsTable(Map<String, List<TsTable>> tableMap) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(tableMap.size(), outputStream);
      for (Map.Entry<String, List<TsTable>> entry : tableMap.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue().size(), outputStream);
        for (TsTable table : entry.getValue()) {
          table.serialize(outputStream);
        }
      }
    } catch (IOException ignored) {
      // won't happen
    }
    return outputStream.toByteArray();
  }

  public static Map<String, List<TsTable>> deserializeBatchTsTable(byte[] bytes) {
    InputStream inputStream = new ByteArrayInputStream(bytes);
    Map<String, List<TsTable>> result = new HashMap<>();
    try {
      int dbNum = ReadWriteIOUtils.readInt(inputStream);
      String database;
      int tableNum;
      List<TsTable> tableList;
      for (int i = 0; i < dbNum; i++) {
        database = ReadWriteIOUtils.readString(inputStream);
        tableNum = ReadWriteIOUtils.readInt(inputStream);
        tableList = new ArrayList<>(tableNum);
        for (int j = 0; j < tableNum; j++) {
          tableList.add(TsTable.deserialize(inputStream));
        }
        result.put(database, tableList);
      }
    } catch (IOException ignored) {
      // won't happen
    }
    return result;
  }

  public static byte[] serializeSingleTsTable(final String database, final TsTable table) {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(database, outputStream);
      table.serialize(outputStream);
    } catch (final IOException ignored) {
      // ByteArrayOutputStream won't throw IOException
    }
    return outputStream.toByteArray();
  }

  public static Pair<String, TsTable> deserializeSingleTsTable(final byte[] bytes) {
    InputStream inputStream = new ByteArrayInputStream(bytes);
    try {
      String database = ReadWriteIOUtils.readString(inputStream);
      TsTable table = TsTable.deserialize(inputStream);
      return new Pair<>(database, table);
    } catch (IOException ignored) {
      // ByteArrayInputStream won't throw IOException
    }
    throw new IllegalStateException();
  }

  public static byte[] serializeTableInitializationInfo(
      Map<String, List<TsTable>> usingTableMap, Map<String, List<TsTable>> preCreateTableMap) {
    byte[] usingBytes = serializeBatchTsTable(usingTableMap);
    byte[] preCreateBytes = serializeBatchTsTable(preCreateTableMap);
    byte[] result = new byte[usingBytes.length + preCreateBytes.length];
    System.arraycopy(usingBytes, 0, result, 0, usingBytes.length);
    System.arraycopy(preCreateBytes, 0, result, usingBytes.length, preCreateBytes.length);
    return result;
  }

  public static Pair<Map<String, List<TsTable>>, Map<String, List<TsTable>>>
      deserializeTableInitializationInfo(byte[] bytes) {
    return new Pair<>(deserializeBatchTsTable(bytes), deserializeBatchTsTable(bytes));
  }
}
