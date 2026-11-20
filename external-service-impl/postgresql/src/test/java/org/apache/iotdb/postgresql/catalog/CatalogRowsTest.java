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

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.postgresql.types.PgType;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class CatalogRowsTest {

  private static Map<String, Map<String, TsTable>> tables() {
    TsTable sensors = new TsTable("sensors");
    sensors.addColumnSchema(new TimeColumnSchema("time", TSDataType.TIMESTAMP));
    sensors.addColumnSchema(new TagColumnSchema("device", TSDataType.STRING));
    sensors.addColumnSchema(new AttributeColumnSchema("site", TSDataType.STRING));
    sensors.addColumnSchema(new FieldColumnSchema("blob", TSDataType.OBJECT));
    FieldColumnSchema temp = new FieldColumnSchema("temp", TSDataType.DOUBLE);
    temp.getProps().put(TsTable.COMMENT_KEY, "degrees");
    sensors.addColumnSchema(temp);
    TsTable secret = new TsTable("secret");
    secret.addColumnSchema(new TimeColumnSchema("time", TSDataType.TIMESTAMP));
    Map<String, TsTable> plant = new HashMap<>();
    plant.put("sensors", sensors);
    plant.put("secret", secret);
    Map<String, TsTable> hidden = new HashMap<>();
    hidden.put("t", new TsTable("t"));
    Map<String, Map<String, TsTable>> all = new HashMap<>();
    all.put("plant", plant);
    all.put("other", hidden);
    all.put("pg_catalog", new HashMap<>(hidden));
    return all;
  }

  private static CatalogSnapshot userSnapshot() {
    return CatalogSnapshot.of(
        tables(), db -> db.equals("plant"), (db, table) -> !table.equals("secret"));
  }

  @Test
  public void everyMappedTypeHasABaseAndAnArrayRowWithItsFunctions() {
    CatalogRows rows = new CatalogRows("u", CatalogRowsTest::userSnapshot);
    Map<Long, Map<String, Object>> types = new HashMap<>();
    for (Map<String, Object> row : rows.rows("pg_type")) {
      types.put((Long) row.get("oid"), row);
    }
    Set<Long> procs =
        rows.rows("pg_proc").stream().map(r -> (Long) r.get("oid")).collect(Collectors.toSet());
    for (PgType type : PgType.values()) {
      if (type == PgType.UNKNOWN) {
        assertEquals("p", types.get((long) type.oid()).get("typtype"));
        continue;
      }
      Map<String, Object> base = types.get((long) type.oid());
      assertEquals(type.typeName(), base.get("typname"));
      assertEquals("b", base.get("typtype"));
      assertEquals((long) type.arrayOid(), base.get("typarray"));
      Map<String, Object> array = types.get((long) type.arrayOid());
      assertEquals("_" + type.typeName(), array.get("typname"));
      assertEquals((long) type.oid(), array.get("typelem"));
      assertEquals(2400L, array.get("typreceive"));
    }
    for (Map<String, Object> type : types.values()) {
      for (String function : new String[] {"typinput", "typoutput", "typreceive", "typsend"}) {
        assertTrue(type.get("typname") + "." + function, procs.contains((Long) type.get(function)));
      }
    }
    assertTrue(types.containsKey(2249L)); // record
    assertTrue(types.containsKey(2287L)); // _record
    assertTrue(types.containsKey(2278L)); // void
  }

  @Test
  public void theUserSeesOnlyReadableDatabasesTablesAndExposedColumns() {
    CatalogRows rows = new CatalogRows("u", CatalogRowsTest::userSnapshot);
    List<String> schemas =
        rows.rows("pg_namespace").stream()
            .map(r -> (String) r.get("nspname"))
            .collect(Collectors.toList());
    assertEquals(3, schemas.size());
    assertTrue(
        schemas.containsAll(java.util.Arrays.asList("pg_catalog", "information_schema", "plant")));

    List<Map<String, Object>> userTables =
        rows.rows("pg_is_tables").stream()
            .filter(r -> "plant".equals(r.get("table_schema")))
            .collect(Collectors.toList());
    assertEquals(1, userTables.size());
    assertEquals("sensors", userTables.get(0).get("table_name"));
    assertEquals("BASE TABLE", userTables.get(0).get("table_type"));

    List<Map<String, Object>> columns =
        rows.rows("pg_is_columns").stream()
            .filter(r -> "sensors".equals(r.get("table_name")))
            .collect(Collectors.toList());
    assertEquals("OBJECT columns are hidden", 4, columns.size());
    assertEquals("time", columns.get(0).get("column_name"));
    assertEquals("timestamp with time zone", columns.get(0).get("data_type"));
    assertEquals("NO", columns.get(0).get("is_nullable"));
    assertEquals("NO", columns.get(1).get("is_nullable")); // tag
    assertEquals("YES", columns.get(2).get("is_nullable")); // attribute
    assertEquals("temp", columns.get(3).get("column_name"));
    assertEquals(4, columns.get(3).get("ordinal_position"));
    assertEquals("float8", columns.get(3).get("udt_name"));
  }

  @Test
  public void classesAttributesAndDescriptionsAgree() {
    CatalogRows rows = new CatalogRows("u", CatalogRowsTest::userSnapshot);
    Map<String, Object> sensors =
        rows.rows("pg_class").stream()
            .filter(r -> "sensors".equals(r.get("relname")))
            .findFirst()
            .get();
    long oid = (Long) sensors.get("oid");
    assertTrue(oid >= 16384 && oid < Integer.MAX_VALUE);
    assertEquals("r", sensors.get("relkind"));
    assertEquals(4, sensors.get("relnatts"));
    long plant =
        rows.rows("pg_namespace").stream()
            .filter(r -> "plant".equals(r.get("nspname")))
            .map(r -> (Long) r.get("oid"))
            .findFirst()
            .get();
    assertEquals(plant, sensors.get("relnamespace"));

    List<Map<String, Object>> attributes =
        rows.rows("pg_attribute").stream()
            .filter(r -> r.get("attrelid").equals(oid))
            .collect(Collectors.toList());
    assertEquals(4, attributes.size());
    assertEquals(1184L, attributes.get(0).get("atttypid"));
    assertEquals(true, attributes.get(0).get("attnotnull"));
    assertEquals(701L, attributes.get(3).get("atttypid"));

    Map<Integer, Object> descriptions = new HashMap<>();
    for (Map<String, Object> row : rows.rows("pg_description")) {
      if (row.get("objoid").equals(oid)) {
        descriptions.put((Integer) row.get("objsubid"), row.get("description"));
      }
    }
    assertEquals("TIME", descriptions.get(1));
    assertEquals("TAG", descriptions.get(2));
    assertEquals("FIELD: degrees", descriptions.get(4));

    // catalog relations are listed too, with PostgreSQL's OIDs
    assertTrue(
        rows.rows("pg_class").stream()
            .anyMatch(r -> "pg_type".equals(r.get("relname")) && r.get("oid").equals(1247L)));
  }

  @Test
  public void oidsDoNotDependOnTheUser() {
    CatalogSnapshot all = CatalogSnapshot.of(tables(), db -> true, (db, table) -> true);
    CatalogSnapshot some = userSnapshot();
    long sensorsForAll =
        all.relations().stream().filter(r -> r.name().equals("sensors")).findFirst().get().oid();
    long sensorsForSome =
        some.relations().stream().filter(r -> r.name().equals("sensors")).findFirst().get().oid();
    assertEquals(sensorsForAll, sensorsForSome);
    assertFalse(
        "reserved names are hidden",
        all.namespaces().stream().anyMatch(n -> n.name().equals("pg_catalog")));
  }

  @Test
  public void collisionsTakeTheNextFreeOid() {
    Set<Long> used = new HashSet<>();
    long first = CatalogSnapshot.allocate("x", used);
    long second = CatalogSnapshot.allocate("x", used);
    assertNotEquals(first, second);
    assertEquals(first + 1, second);
  }

  @Test
  public void everyRegisteredTableHasRowsOrIsEmpty() {
    CatalogRows rows = new CatalogRows("u", CatalogRowsTest::userSnapshot);
    for (Map.Entry<String, List<PgCatalogTables.Column>> table :
        PgCatalogTables.tables().entrySet()) {
      Set<String> names =
          table.getValue().stream().map(PgCatalogTables.Column::name).collect(Collectors.toSet());
      for (Map<String, Object> row : rows.rows(table.getKey())) {
        for (String column : row.keySet()) {
          assertTrue(table.getKey() + " has no column " + column, names.contains(column));
        }
        for (PgCatalogTables.Column column : table.getValue()) {
          // every value converts to its column type
          CatalogRows.convert(row.get(column.name()), column.type());
        }
      }
    }
    assertEquals("u", rows.rows("pg_roles").get(0).get("rolname"));
  }

  @Test
  public void alwaysEmptyTablesAreEmptyAndArrayColumnsAreNeverFilled() {
    CatalogRows rows = new CatalogRows("u", CatalogRowsTest::userSnapshot);
    for (Map.Entry<String, List<PgCatalogTables.Column>> table :
        PgCatalogTables.tables().entrySet()) {
      List<Map<String, Object>> content = rows.rows(table.getKey());
      if (PgCatalogTables.alwaysEmpty(table.getKey())) {
        assertTrue(table.getKey(), content.isEmpty());
      }
      for (PgCatalogTables.Column column : table.getValue()) {
        if (column.array()) {
          for (Map<String, Object> row : content) {
            assertEquals(table.getKey() + "." + column.name(), null, row.get(column.name()));
          }
        }
      }
    }
  }
}
