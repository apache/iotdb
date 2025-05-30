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

package org.apache.iotdb.db.schemaengine.table;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;

import java.util.Arrays;
import java.util.StringJoiner;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.schema.table.TreeViewSchema.TREE_PATH_PATTERN;
import static org.apache.iotdb.commons.schema.table.TreeViewSchema.getPrefixPattern;

public class DataNodeTreeViewSchemaUtils {

  public static void checkTableInWrite(final String database, final TsTable table) {
    if (isTreeViewTable(table)) {
      throw new SemanticException(
          new IoTDBException(
              String.format(
                  "The table %s.%s is a view from tree, cannot be written or deleted from",
                  database, table.getTableName()),
              TSStatusCode.SEMANTIC_ERROR.getStatusCode()));
    }
  }

  // For better performance
  public static boolean isTreeViewTable(final TsTable table) {
    return table.containsPropWithoutLock(TREE_PATH_PATTERN);
  }

  public static String[] getPatternNodes(final TsTable table) {
    final PartialPath path = getPrefixPattern(table);
    return Arrays.copyOf(path.getNodes(), path.getNodeLength() - 1);
  }

  public static String getPrefixPath(final TsTable table) {
    final PartialPath path = getPrefixPattern(table);
    StringJoiner joiner = new StringJoiner(".");
    for (int i = 0; i < path.getNodeLength() - 1; i++) {
      joiner.add(path.getNodes()[i]);
    }
    return joiner.toString();
  }

  public static IDeviceID convertToIDeviceID(final TsTable table, final String[] idValues) {
    return IDeviceID.Factory.DEFAULT_FACTORY.create(
        StringArrayDeviceID.splitDeviceIdString(
            Stream.concat(
                    Arrays.stream(getPatternNodes(table)),
                    Arrays.stream((String[]) DeviceIDFactory.truncateTailingNull(idValues)))
                .toArray(String[]::new)));
  }

  private DataNodeTreeViewSchemaUtils() {
    // Private constructor
  }
}
