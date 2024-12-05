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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.IdColumnSchema;
import org.apache.iotdb.db.exception.sql.SemanticException;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

public class InformationSchema {
  public static final String INFORMATION_DATABASE = "information_schema";
  private static final Map<String, TsTable> schemaTables = new HashMap<>();

  static {
    // Show queries
    final TsTable queriesTable = new TsTable("queries");
    queriesTable.addColumnSchema(new IdColumnSchema("time", TSDataType.TIMESTAMP));
    queriesTable.addColumnSchema(new IdColumnSchema("query_id", TSDataType.STRING));
    queriesTable.addColumnSchema(new IdColumnSchema("datanode_id", TSDataType.INT32));
    queriesTable.addColumnSchema(new IdColumnSchema("elapsed_time", TSDataType.FLOAT));
    queriesTable.addColumnSchema(new IdColumnSchema("statement", TSDataType.STRING));
    queriesTable.addColumnSchema(new IdColumnSchema("sql_dialect", TSDataType.STRING));
    schemaTables.put("queries", queriesTable);

    // Show databases
    final TsTable databaseTable = new TsTable("databases");
    databaseTable.addColumnSchema(new IdColumnSchema("database", TSDataType.TIMESTAMP));
    databaseTable.addColumnSchema(new IdColumnSchema("ttl(ms)", TSDataType.STRING));
    databaseTable.addColumnSchema(
        new IdColumnSchema(
            "SchemaReplicationFactor".toLowerCase(Locale.ENGLISH), TSDataType.INT32));
    databaseTable.addColumnSchema(
        new IdColumnSchema("DataReplicationFactor".toLowerCase(Locale.ENGLISH), TSDataType.INT32));
    databaseTable.addColumnSchema(
        new IdColumnSchema("TimePartitionInterval".toLowerCase(Locale.ENGLISH), TSDataType.INT64));
    schemaTables.put("databases", databaseTable);

    // Show databases details
    final TsTable databaseTableDetails = new TsTable("databases");
    databaseTableDetails.addColumnSchema(new IdColumnSchema("database", TSDataType.TIMESTAMP));
    databaseTableDetails.addColumnSchema(new IdColumnSchema("ttl(ms)", TSDataType.STRING));
    databaseTableDetails.addColumnSchema(
        new IdColumnSchema(
            "SchemaReplicationFactor".toLowerCase(Locale.ENGLISH), TSDataType.INT32));
    databaseTableDetails.addColumnSchema(
        new IdColumnSchema("DataReplicationFactor".toLowerCase(Locale.ENGLISH), TSDataType.INT32));
    databaseTableDetails.addColumnSchema(
        new IdColumnSchema("TimePartitionInterval".toLowerCase(Locale.ENGLISH), TSDataType.INT64));
    databaseTableDetails.addColumnSchema(
        new IdColumnSchema("model".toLowerCase(Locale.ENGLISH), TSDataType.STRING));
    schemaTables.put("databases", databaseTableDetails);

    // Show tables

    // Show tables details

    // Desc table

    // Desc table details
  }

  public static void checkDBNameInWrite(final String dbName) {
    if (dbName.equals(INFORMATION_DATABASE)) {
      throw new SemanticException("The database 'information_schema' can only be queried");
    }
  }

  public static void buildDatabaseTsBlock(
      final Predicate<String> canSeenDB, final TsBlockBuilder builder, final boolean details) {
    if (!canSeenDB.test(INFORMATION_DATABASE)) {
      return;
    }
    builder.getTimeColumnBuilder().writeLong(0L);
    builder
        .getColumnBuilder(0)
        .writeBinary(new Binary(INFORMATION_DATABASE, TSFileConfig.STRING_CHARSET));
    builder
        .getColumnBuilder(1)
        .writeBinary(new Binary(IoTDBConstant.TTL_INFINITE, TSFileConfig.STRING_CHARSET));

    builder.getColumnBuilder(2).appendNull();
    builder.getColumnBuilder(3).appendNull();
    builder.getColumnBuilder(4).appendNull();
    if (details) {
      builder.getColumnBuilder(5).writeBinary(new Binary("TABLE", TSFileConfig.STRING_CHARSET));
    }
    builder.declarePosition();
  }

  public static TsTable mayGetTable(final String database, final String tableName) {
    return INFORMATION_DATABASE.equals(database) ? schemaTables.get(tableName) : null;
  }

  private InformationSchema() {
    // Util class
  }
}
