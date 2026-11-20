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

package org.apache.iotdb.postgresql.catalog;

import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Registers the PostgreSQL catalog tables in IoTDB's {@code information_schema}. The first column
 * of each table is its key (a tag column, as in IoTDB's own information-schema tables), the others
 * are attribute columns; there is no time column. The rows come from {@link PgCatalogProvider}.
 */
public final class PgCatalogExtension implements InformationSchema.InformationSchemaExtension {

  @Override
  public void registerAdditionalInformationSchemaTables(BiConsumer<String, TsTable> register) {
    for (Map.Entry<String, List<PgCatalogTables.Column>> table :
        PgCatalogTables.tables().entrySet()) {
      TsTable tsTable = new TsTable(table.getKey());
      boolean key = true;
      for (PgCatalogTables.Column column : table.getValue()) {
        tsTable.addColumnSchema(
            key
                ? new TagColumnSchema(column.name(), column.type())
                : new AttributeColumnSchema(column.name(), column.type()));
        key = false;
      }
      register.accept(table.getKey(), tsTable);
    }
  }
}
