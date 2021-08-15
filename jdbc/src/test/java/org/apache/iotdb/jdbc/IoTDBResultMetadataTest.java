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
}
