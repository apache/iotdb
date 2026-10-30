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

package org.apache.iotdb.tool.data;

import org.apache.iotdb.isession.SessionDataSet;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExportDataTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void exportsInterleavedDevicesWithoutEmptyRecords() throws Exception {
    SessionDataSet dataSet = mock(SessionDataSet.class);
    SessionDataSet.DataIterator iterator = mock(SessionDataSet.DataIterator.class);
    when(dataSet.iterator()).thenReturn(iterator);
    when(iterator.getColumnTypeList())
        .thenReturn(List.of("INT64", "TEXT", "TEXT", "INT32", "INT32"));
    when(iterator.next()).thenReturn(true, true, false);
    when(iterator.getLong(1)).thenReturn(1L, 2L);
    when(iterator.isNull(2)).thenReturn(false, true);
    when(iterator.isNull(3)).thenReturn(true, false);
    when(iterator.isNull(4)).thenReturn(false, true);
    when(iterator.isNull(5)).thenReturn(true);
    when(iterator.getString(2)).thenReturn("O'Reilly");
    when(iterator.getString(3)).thenReturn("a \"quoted\" value");
    when(iterator.getString(4)).thenReturn("7");
    List<String> headers = List.of("Time", "root.d.s1", "root.e.s1", "root.d.s2", "root.e.s2");
    Path output = temporaryFolder.getRoot().toPath().resolve("export");

    ExportData.writeSqlFile(dataSet, output.toString(), headers);

    assertEquals(
        "INSERT INTO root.d(TIMESTAMP,s1,s2) VALUES (1,'O''Reilly',7);\n"
            + "INSERT INTO root.e(TIMESTAMP,s1) VALUES (2,'a \"quoted\" value');\n",
        Files.readString(Path.of(output + "_0.sql")).replace("\r\n", "\n"));
    assertEquals("Time", headers.get(0));
  }

  @Test
  public void alignedExportPreservesNullsAndTypedValues() throws Exception {
    SessionDataSet dataSet = mock(SessionDataSet.class);
    SessionDataSet.DataIterator iterator = mock(SessionDataSet.DataIterator.class);
    when(dataSet.iterator()).thenReturn(iterator);
    when(iterator.getColumnTypeList())
        .thenReturn(List.of("INT64", "TEXT", "TEXT", "STRING", "BLOB", "DATE"));
    when(iterator.next()).thenReturn(true, false);
    when(iterator.getLong(1)).thenReturn(3L);
    when(iterator.getString(2)).thenReturn("root.d");
    when(iterator.isNull(3)).thenReturn(true);
    when(iterator.getString(4)).thenReturn("O'Reilly");
    when(iterator.getString(5)).thenReturn("0xABCD");
    when(iterator.getString(6)).thenReturn("2026-10-22");
    List<String> headers = List.of("Time", "Device", "s1", "s2", "s3", "s4");
    Path output = temporaryFolder.getRoot().toPath().resolve("aligned");

    ExportData.writeSqlFile(dataSet, output.toString(), headers);

    assertEquals(
        "INSERT INTO root.d(TIMESTAMP,s1,s2,s3,s4) ALIGNED VALUES (3,NULL,'O''Reilly',X'ABCD','2026-10-22');\n",
        Files.readString(Path.of(output + "_0.sql")).replace("\r\n", "\n"));
    assertEquals(List.of("Time", "Device", "s1", "s2", "s3", "s4"), headers);
  }
}
