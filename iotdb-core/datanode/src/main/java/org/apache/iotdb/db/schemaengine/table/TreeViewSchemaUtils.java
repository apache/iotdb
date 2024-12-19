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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.exception.sql.SemanticException;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;

import java.util.Arrays;
import java.util.stream.Stream;

public class TreeViewSchemaUtils {

  public static void putAlignedToTreeCache() {}

  public static boolean isTreeViewDatabase(final String database) {
    return TreeViewSchema.TREE_VIEW_DATABASE.equals(database);
  }

  public static String getOriginalDatabase(final TsTable table) {
    return table
        .getPropValue(TreeViewSchema.TREE_DATABASE)
        .orElseThrow(
            () ->
                new SemanticException(
                    String.format(
                        "Failed to get the original database, because the %s is null for table %s",
                        TreeViewSchema.TREE_DATABASE, table.getTableName())));
  }

  public static IDeviceID convertToIDeviceID(final String database, final String[] idValues) {
    final String[] databaseNodes;
    try {
      databaseNodes = new PartialPath(database).getNodes();
    } catch (final IllegalPathException e) {
      throw new SemanticException(
          String.format(
              "Failed to parse the tree database %s when convert to IDeviceID", database));
    }
    return IDeviceID.Factory.DEFAULT_FACTORY.create(
        StringArrayDeviceID.splitDeviceIdString(
            Stream.concat(Arrays.stream(databaseNodes), Arrays.stream(idValues))
                .toArray(String[]::new)));
  }

  private TreeViewSchemaUtils() {
    // Private constructor
  }
}
