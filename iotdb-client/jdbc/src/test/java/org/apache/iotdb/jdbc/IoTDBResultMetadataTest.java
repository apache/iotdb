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

package org.apache.iotdb.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IoTDBResultMetadataTest {

  private IoTDBResultMetadata metadata;

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void testGetColumnCount() {
    metadata = new IoTDBResultMetadata(false, null, "QUERY", null, null, false);
    boolean flag = false;
    try {
      metadata.getColumnCount();
    } catch (Exception e) {
      flag = true;
    }
    assertFalse(flag);

    flag = false;
    try {
      metadata =
          new IoTDBResultMetadata(false, null, "QUERY", Collections.emptyList(), null, false);
      metadata.getColumnCount();
    } catch (Exception e) {
      flag = true;
    }
    assertFalse(flag);

    List<String> columnInfoList = new ArrayList<>();
    columnInfoList.add("root.a.b.c");
    metadata = new IoTDBResultMetadata(false, null, "QUERY", columnInfoList, null, false);
    assertEquals(1, metadata.getColumnCount());
  }

  @Test
  public void testGetColumnName() throws SQLException {
    metadata = new IoTDBResultMetadata(false, null, "QUERY", null, null, false);
    boolean flag = false;
    try {
      metadata.getColumnName(1);
    } catch (Exception e) {
      flag = true;
    }
    assertTrue(flag);

    List<String> columnInfoList = new ArrayList<>();
    metadata = new IoTDBResultMetadata(false, null, "QUERY", columnInfoList, null, false);
    flag = false;
    try {
      metadata.getColumnName(1);
    } catch (Exception e) {
      flag = true;
    }
    assertTrue(flag);

    String[] colums = {"root.a.b.c1", "root.a.b.c2", "root.a.b.c3"};
    metadata = new IoTDBResultMetadata(false, null, "QUERY", Arrays.asList(colums), null, false);
    flag = false;
    try {
      metadata.getColumnName(colums.length + 1);
    } catch (Exception e) {
      flag = true;
    }
    assertTrue(flag);

    flag = false;
    try {
      metadata.getColumnName(0);
    } catch (Exception e) {
      flag = true;
    }
    assertTrue(flag);

    for (int i = 1; i <= colums.length; i++) {
      assertEquals(metadata.getColumnLabel(i), colums[i - 1]);
    }
  }

  @Test
  public void testGetColumnType() throws SQLException {
    metadata = new IoTDBResultMetadata(false, null, "QUERY", null, null, false);
    boolean flag = false;
    try {
      metadata.getColumnType(1);
    } catch (Exception e) {
      flag = true;
    }
    assertTrue(flag);

    List<String> columnInfoList = new ArrayList<>();
    metadata = new IoTDBResultMetadata(false, null, "QUERY", columnInfoList, null, false);
    flag = false;
    try {
      metadata.getColumnType(1);
    } catch (Exception e) {
      flag = true;
    }
    assertTrue(flag);

    String[] columns = {
      "timestamp",
      "root.a.b.boolean",
      "root.a.b.int32",
      "root.a.b.int64",
      "root.a.b.float",
      "root.a.b.double",
      "root.a.b.text"
    };
    String[] typesString = {"TIMESTAMP", "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"};
    int[] types = {
      Types.BOOLEAN, Types.INTEGER, Types.BIGINT, Types.FLOAT, Types.DOUBLE, Types.VARCHAR
    };
    metadata =
        new IoTDBResultMetadata(
            false, null, "QUERY", Arrays.asList(columns), Arrays.asList(typesString), false);
    flag = false;
    try {
      metadata.getColumnType(columns.length + 1);
    } catch (Exception e) {
      flag = true;
    }
    assertTrue(flag);

    flag = false;
    try {
      metadata.getColumnType(0);
    } catch (Exception e) {
      flag = true;
    }
    assertTrue(flag);

    assertEquals(Types.TIMESTAMP, metadata.getColumnType(1));
    for (int i = 1; i <= types.length; i++) {
      assertEquals(metadata.getColumnType(i + 1), types[i - 1]);
    }
  }

  @Test
  public void getCatalogName() throws SQLException {
    List<String> listColumns = new ArrayList<>();
    listColumns.add("count");
    listColumns.add("b");
    IoTDBResultMetadata metadata1 =
        new IoTDBResultMetadata(true, listColumns, "QUERY", listColumns, null, false);
    String catalogName1 = metadata1.getCatalogName(2);
    assertEquals("b", catalogName1);

    IoTDBResultMetadata metadata2 =
        new IoTDBResultMetadata(true, listColumns, "SHOW", listColumns, null, false);
    String catalogName2 = metadata2.getCatalogName(1);
    assertEquals("_system_database", catalogName2);

    IoTDBResultMetadata metadata3 =
        new IoTDBResultMetadata(true, listColumns, "LIST_USER", listColumns, null, false);
    String catalogName3 = metadata3.getCatalogName(1);
    assertEquals("_system_user", catalogName3);

    IoTDBResultMetadata metadata4 =
        new IoTDBResultMetadata(true, listColumns, "LIST_ROLE", listColumns, null, false);
    String catalogName4 = metadata4.getCatalogName(1);
    assertEquals("_system_role", catalogName4);

    IoTDBResultMetadata metadata5 =
        new IoTDBResultMetadata(true, listColumns, "LIST_USER_PRIVILEGE", listColumns, null, false);
    String catalogName5 = metadata5.getCatalogName(1);
    assertEquals("_system_auths", catalogName5);

    IoTDBResultMetadata metadata6 =
        new IoTDBResultMetadata(true, listColumns, "LIST_USER_ROLES", listColumns, null, false);
    String catalogName6 = metadata6.getCatalogName(1);
    assertEquals("_system_role", catalogName6);

    IoTDBResultMetadata metadata7 =
        new IoTDBResultMetadata(true, listColumns, "LIST_ROLE_PRIVILEGE", listColumns, null, false);
    String catalogName7 = metadata7.getCatalogName(1);
    assertEquals("_system_auths", catalogName7);

    IoTDBResultMetadata metadata8 =
        new IoTDBResultMetadata(true, listColumns, "LIST_ROLE_USERS", listColumns, null, false);
    String catalogName8 = metadata8.getCatalogName(1);
    assertEquals("_system_user", catalogName8);
  }

  @Test
  public void getColumnClassName() throws SQLException {
    List<String> listColumns = new ArrayList<>();
    listColumns.add("Time");
    listColumns.add("count");
    listColumns.add("b");
    listColumns.add("b");
    listColumns.add("b");
    listColumns.add("b");
    listColumns.add("b");
    List<String> typeColumns = new ArrayList<>();
    typeColumns.add("INT64");
    typeColumns.add("TEXT");
    typeColumns.add("INT32");
    typeColumns.add("INT64");
    typeColumns.add("FLOAT");
    typeColumns.add("DOUBLE");
    typeColumns.add("BOOLEAN");
    IoTDBResultMetadata metadata =
        new IoTDBResultMetadata(true, listColumns, "QUERY", listColumns, typeColumns, false);
    String columnClassName1 = metadata.getColumnClassName(2);
    assertEquals(columnClassName1, String.class.getName());
    String columnClassName2 = metadata.getColumnClassName(3);
    assertEquals(columnClassName2, Integer.class.getName());
    String columnClassName3 = metadata.getColumnClassName(4);
    assertEquals(columnClassName3, Long.class.getName());
    String columnClassName4 = metadata.getColumnClassName(5);
    assertEquals(columnClassName4, Float.class.getName());
    String columnClassName5 = metadata.getColumnClassName(6);
    assertEquals(columnClassName5, Double.class.getName());
    String columnClassName6 = metadata.getColumnClassName(7);
    assertEquals(columnClassName6, Boolean.class.getName());
  }

  @Test
  public void getPrecision() throws SQLException {
    List<String> listColumns = new ArrayList<>();
    listColumns.add("Time");
    listColumns.add("count");
    listColumns.add("b");
    listColumns.add("b");
    listColumns.add("b");
    listColumns.add("b");
    listColumns.add("b");
    List<String> typeColumns = new ArrayList<>();
    typeColumns.add("INT64");
    typeColumns.add("TEXT");
    typeColumns.add("INT32");
    typeColumns.add("INT64");
    typeColumns.add("FLOAT");
    typeColumns.add("DOUBLE");
    typeColumns.add("BOOLEAN");
    IoTDBResultMetadata metadata =
        new IoTDBResultMetadata(true, listColumns, "QUERY", listColumns, typeColumns, false);
    int columnClassName0 = metadata.getPrecision(1);
    assertEquals(columnClassName0, 13);
    int columnClassName1 = metadata.getPrecision(2);
    assertEquals(columnClassName1, Integer.MAX_VALUE);
    int columnClassName2 = metadata.getPrecision(3);
    assertEquals(columnClassName2, 10);
    int columnClassName3 = metadata.getPrecision(4);
    assertEquals(columnClassName3, 19);
    int columnClassName4 = metadata.getPrecision(5);
    assertEquals(columnClassName4, 38);
    int columnClassName5 = metadata.getPrecision(6);
    assertEquals(columnClassName5, 308);
    int columnClassName6 = metadata.getPrecision(7);
    assertEquals(columnClassName6, 1);
  }

  @Test
  public void getScale() throws SQLException {
    List<String> listColumns = new ArrayList<>();
    listColumns.add("Time");
    listColumns.add("count");
    listColumns.add("b");
    listColumns.add("b");
    listColumns.add("b");
    listColumns.add("b");
    listColumns.add("b");
    List<String> typeColumns = new ArrayList<>();
    typeColumns.add("INT64");
    typeColumns.add("TEXT");
    typeColumns.add("INT32");
    typeColumns.add("INT64");
    typeColumns.add("FLOAT");
    typeColumns.add("DOUBLE");
    typeColumns.add("BOOLEAN");
    IoTDBResultMetadata metadata =
        new IoTDBResultMetadata(true, listColumns, "QUERY", listColumns, typeColumns, false);
    int columnClassName0 = metadata.getScale(1);
    assertEquals(columnClassName0, 0);
    int columnClassName1 = metadata.getScale(2);
    assertEquals(columnClassName1, 0);
    int columnClassName2 = metadata.getScale(3);
    assertEquals(columnClassName2, 0);
    int columnClassName3 = metadata.getScale(4);
    assertEquals(columnClassName3, 0);
    int columnClassName4 = metadata.getScale(5);
    assertEquals(columnClassName4, 6);
    int columnClassName5 = metadata.getScale(6);
    assertEquals(columnClassName5, 15);
    int columnClassName6 = metadata.getScale(7);
    assertEquals(columnClassName6, 0);
  }
}
