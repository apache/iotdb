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

package org.apache.iotdb.commons.pipe.datastructure.pattern;

import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TablePatternTest {

  @Test
  public void testWritableViewSourcePatternKeepsViewPatternForMetadata() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, Boolean.TRUE.toString());
    attributes.put(PipeSourceConstant.SOURCE_DATABASE_NAME_KEY, "db");
    attributes.put(PipeSourceConstant.SOURCE_TABLE_NAME_KEY, "writable_view");
    attributes.put(PipeSourceConstant.SOURCE_ORIGINAL_DATABASE_KEY, "source_db");
    attributes.put(PipeSourceConstant.SOURCE_ORIGINAL_TABLE_KEY, "source_table");

    final TablePattern tablePattern =
        TablePattern.parsePipePatternFromSourceParameters(new PipeParameters(attributes));

    Assert.assertTrue(tablePattern.matchesDatabase("db"));
    Assert.assertTrue(tablePattern.matchesTable("writable_view"));
    Assert.assertFalse(tablePattern.matchesDatabase("source_db"));
    Assert.assertFalse(tablePattern.matchesTable("source_table"));
  }

  @Test
  public void testWritableViewSourcePatternOverridesViewPatternForData() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, Boolean.TRUE.toString());
    attributes.put(PipeSourceConstant.SOURCE_DATABASE_NAME_KEY, "db");
    attributes.put(PipeSourceConstant.SOURCE_TABLE_NAME_KEY, "writable_view");
    attributes.put(PipeSourceConstant.SOURCE_ORIGINAL_DATABASE_KEY, "source_db");
    attributes.put(PipeSourceConstant.SOURCE_ORIGINAL_TABLE_KEY, "source_table");

    final TablePattern tablePattern =
        TablePattern.parsePipeDataPatternFromSourceParameters(new PipeParameters(attributes));

    Assert.assertTrue(tablePattern.matchesDatabase("source_db"));
    Assert.assertTrue(tablePattern.matchesTable("source_table"));
    Assert.assertFalse(tablePattern.matchesDatabase("db"));
    Assert.assertFalse(tablePattern.matchesTable("writable_view"));
  }

  @Test
  public void testWritableViewSourcePatternIsQuotedForData() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, Boolean.TRUE.toString());
    attributes.put(PipeSourceConstant.SOURCE_DATABASE_NAME_KEY, "db");
    attributes.put(PipeSourceConstant.SOURCE_TABLE_NAME_KEY, "view");
    attributes.put(PipeSourceConstant.SOURCE_ORIGINAL_DATABASE_KEY, "source.db");
    attributes.put(PipeSourceConstant.SOURCE_ORIGINAL_TABLE_KEY, "source.table");

    final TablePattern tablePattern =
        TablePattern.parsePipeDataPatternFromSourceParameters(new PipeParameters(attributes));

    Assert.assertTrue(tablePattern.matchesDatabase("source.db"));
    Assert.assertTrue(tablePattern.matchesTable("source.table"));
    Assert.assertFalse(tablePattern.matchesDatabase("sourceXdb"));
    Assert.assertFalse(tablePattern.matchesTable("sourceXtable"));
  }

  @Test
  public void testWritableViewStaticSourcePatternAddsBaseTablesForData() {
    final Map<String, Set<String>> matchedOriginalDatabaseTables = new HashMap<>();
    final Set<String> sourceTables = new HashSet<>();
    sourceTables.add("source.table(1)");
    matchedOriginalDatabaseTables.put("source.db", sourceTables);

    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, Boolean.TRUE.toString());
    attributes.put(PipeSourceConstant.SOURCE_DATABASE_NAME_KEY, "db");
    attributes.put(PipeSourceConstant.SOURCE_TABLE_NAME_KEY, "view.*");
    attributes.put(
        PipeSourceConstant.SOURCE_ORIGINAL_DATABASE_TABLES_KEY,
        TablePattern.serializeDatabaseTablePairs(matchedOriginalDatabaseTables));

    final TablePattern tablePattern =
        TablePattern.parsePipeDataPatternFromSourceParameters(new PipeParameters(attributes));

    Assert.assertTrue(tablePattern.matchesDatabaseAndTable("db", "view_1"));
    Assert.assertTrue(tablePattern.matchesDatabaseAndTable("source.db", "source.table(1)"));
    Assert.assertFalse(tablePattern.matchesDatabaseAndTable("source.db", "sourceXtable(1)"));
    Assert.assertFalse(tablePattern.matchesDatabase("source.db"));
    Assert.assertFalse(tablePattern.matchesTable("source.table(1)"));
    Assert.assertTrue(tablePattern.mayMatchDatabase("source.db"));
  }

  @Test
  public void testWritableViewStaticSourcePatternEscapesSerializedSpecialCharacters() {
    final Map<String, Set<String>> matchedOriginalDatabaseTables = new HashMap<>();
    final Set<String> sourceTables = new HashSet<>();
    sourceTables.add("source.table(1),;\\[]{}$^+?|");
    matchedOriginalDatabaseTables.put("source.db,;\\[]{}$^+?|", sourceTables);

    final String serializedDatabaseTablePairs =
        TablePattern.serializeDatabaseTablePairs(matchedOriginalDatabaseTables);
    final Map<String, Set<String>> deserializedDatabaseTablePairs =
        TablePattern.deserializeDatabaseTablePairs(serializedDatabaseTablePairs);
    Assert.assertEquals(matchedOriginalDatabaseTables, deserializedDatabaseTablePairs);

    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, Boolean.TRUE.toString());
    attributes.put(PipeSourceConstant.SOURCE_DATABASE_NAME_KEY, "view_db");
    attributes.put(PipeSourceConstant.SOURCE_TABLE_NAME_KEY, "view.*");
    attributes.put(
        PipeSourceConstant.SOURCE_ORIGINAL_DATABASE_TABLES_KEY, serializedDatabaseTablePairs);

    final TablePattern tablePattern =
        TablePattern.parsePipeDataPatternFromSourceParameters(new PipeParameters(attributes));

    Assert.assertTrue(
        tablePattern.matchesDatabaseAndTable(
            "source.db,;\\[]{}$^+?|", "source.table(1),;\\[]{}$^+?|"));
    Assert.assertFalse(
        tablePattern.matchesDatabaseAndTable(
            "sourceXdb,;\\[]{}$^+?|", "source.table(1),;\\[]{}$^+?|"));
  }

  @Test
  public void testWritableViewStaticSourcePatternMatchesDatabaseTablePairs() {
    final Map<String, Set<String>> matchedOriginalDatabaseTables = new HashMap<>();
    final Set<String> sourceTables1 = new HashSet<>();
    sourceTables1.add("source_table_1");
    matchedOriginalDatabaseTables.put("source_db_1", sourceTables1);
    final Set<String> sourceTables2 = new HashSet<>();
    sourceTables2.add("source_table_2");
    matchedOriginalDatabaseTables.put("source_db_2", sourceTables2);

    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY, Boolean.TRUE.toString());
    attributes.put(PipeSourceConstant.SOURCE_DATABASE_NAME_KEY, "view_db");
    attributes.put(PipeSourceConstant.SOURCE_TABLE_NAME_KEY, "view.*");
    attributes.put(
        PipeSourceConstant.SOURCE_ORIGINAL_DATABASE_TABLES_KEY,
        TablePattern.serializeDatabaseTablePairs(matchedOriginalDatabaseTables));

    final TablePattern tablePattern =
        TablePattern.parsePipeDataPatternFromSourceParameters(new PipeParameters(attributes));

    Assert.assertTrue(tablePattern.matchesDatabaseAndTable("source_db_1", "source_table_1"));
    Assert.assertTrue(tablePattern.matchesDatabaseAndTable("source_db_2", "source_table_2"));
    Assert.assertFalse(tablePattern.matchesDatabaseAndTable("source_db_1", "source_table_2"));
    Assert.assertFalse(tablePattern.matchesDatabaseAndTable("source_db_2", "source_table_1"));
    Assert.assertFalse(tablePattern.matchesDatabase("source_db_1"));
    Assert.assertFalse(tablePattern.matchesTable("source_table_1"));
    Assert.assertTrue(tablePattern.mayMatchDatabase("source_db_1"));
  }
}
