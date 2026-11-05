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

package org.apache.iotdb.tool.schema;

import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tool.common.Constants;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Checks that the import passes the parsed tags and attributes on to the client API. */
public class SchemaTagsImportTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void reset() {
    ImportSchemaTree.sessionPool = null;
    ImportSchemaTree.aligned = false;
  }

  @Test
  public void legacyFileKeepsTheLegacyCreateCall() throws Exception {
    SessionPool pool = mock(SessionPool.class);
    ImportSchemaTree.sessionPool = pool;

    runImport(
        "Timeseries,Alias,DataType,Encoding,Compression\n"
            + "root.sg.d.s1,,INT64,TS_2DIFF,SNAPPY\n"
            + "root.sg.d.s2,aliasB,INT64,TS_2DIFF,SNAPPY\n");

    verify(pool)
        .createMultiTimeseries(
            Arrays.asList("root.sg.d.s1"),
            Arrays.asList(TSDataType.INT64),
            Arrays.asList(TSEncoding.TS_2DIFF),
            Arrays.asList(CompressionType.SNAPPY),
            null,
            null,
            null,
            null);
    verify(pool)
        .createMultiTimeseries(
            Arrays.asList("root.sg.d.s2"),
            Arrays.asList(TSDataType.INT64),
            Arrays.asList(TSEncoding.TS_2DIFF),
            Arrays.asList(CompressionType.SNAPPY),
            null,
            null,
            null,
            Arrays.asList("aliasB"));
  }

  @Test
  public void v2FilePassesTagsAndAttributesToCreateMultiTimeseries() throws Exception {
    SessionPool pool = mock(SessionPool.class);
    ImportSchemaTree.sessionPool = pool;
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("room", "101");
    tags.put("type", "sensor");
    Map<String, String> attributes = new LinkedHashMap<>();
    attributes.put("owner", "team a");

    runImport(
        Constants.HEAD_COLUMNS_V2.stream().collect(Collectors.joining(","))
            + "\nroot.sg.d.s1,,INT64,TS_2DIFF,SNAPPY,\""
            + AbstractSchemaTool.toJsonCell(tags).replace("\"", "\"\"")
            + "\",\""
            + AbstractSchemaTool.toJsonCell(attributes).replace("\"", "\"\"")
            + "\"\n"
            + "root.sg.d.s2,,INT64,TS_2DIFF,SNAPPY,,\n");

    verify(pool)
        .createMultiTimeseries(
            Arrays.asList("root.sg.d.s1", "root.sg.d.s2"),
            Arrays.asList(TSDataType.INT64, TSDataType.INT64),
            Arrays.asList(TSEncoding.TS_2DIFF, TSEncoding.TS_2DIFF),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY),
            null,
            Arrays.asList(new LinkedHashMap<>(tags), null),
            Arrays.asList(new LinkedHashMap<>(attributes), null),
            null);
  }

  @Test
  public void v2AlignedFilePassesTagsAndAttributesToCreateAlignedTimeseries() throws Exception {
    SessionPool pool = mock(SessionPool.class);
    ImportSchemaTree.sessionPool = pool;
    ImportSchemaTree.aligned = true;
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("room", "101");
    Map<String, String> attributes = new LinkedHashMap<>();
    attributes.put("owner", "team a");
    // the aligned writer empties its lists in a finally block, so snapshot the arguments
    List<Object> alignedCall = new ArrayList<>();
    doAnswer(
            call -> {
              for (Object argument : call.getArguments()) {
                alignedCall.add(
                    argument instanceof List ? new ArrayList<>((List<?>) argument) : argument);
              }
              return null;
            })
        .when(pool)
        .createAlignedTimeseries(any(), any(), any(), any(), any(), any(), any(), any());

    runImport(
        Constants.HEAD_COLUMNS_V2.stream().collect(Collectors.joining(","))
            + "\nroot.sg.d.s1,,INT64,TS_2DIFF,SNAPPY,\""
            + AbstractSchemaTool.toJsonCell(tags).replace("\"", "\"\"")
            + "\",\""
            + AbstractSchemaTool.toJsonCell(attributes).replace("\"", "\"\"")
            + "\"\n");

    // the pre-existing aligned path hands over both the measurement and the full path, so the maps
    // must be built with the same shape to stay positionally aligned
    assertEquals(
        Arrays.asList(
            "root.sg.d",
            Arrays.asList("s1", "root.sg.d.s1"),
            Arrays.asList(TSDataType.INT64),
            Arrays.asList(TSEncoding.TS_2DIFF),
            Arrays.asList(CompressionType.SNAPPY),
            new ArrayList<String>(),
            Arrays.asList(new LinkedHashMap<>(tags), new LinkedHashMap<>(tags)),
            Arrays.asList(new LinkedHashMap<>(attributes), new LinkedHashMap<>(attributes))),
        alignedCall);
    verify(pool, never())
        .createMultiTimeseries(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void v2HeaderWithoutAllColumnsCreatesNothing() throws Exception {
    SessionPool pool = mock(SessionPool.class);
    ImportSchemaTree.sessionPool = pool;
    assertFalse(
        ImportSchemaTree.checkHeader(
            Arrays.asList("Timeseries", "Alias", "DataType", "Encoding", "Compression", "Tags")));

    verify(pool, never())
        .createMultiTimeseries(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void brokenJsonCellSkipsTheRecord() throws Exception {
    SessionPool pool = mock(SessionPool.class);
    ImportSchemaTree.sessionPool = pool;

    runImport(
        Constants.HEAD_COLUMNS_V2.stream().collect(Collectors.joining(","))
            + "\nroot.sg.d.s1,,INT64,TS_2DIFF,SNAPPY,\"{not json}\",\n");

    verify(pool, never())
        .createMultiTimeseries(any(), any(), any(), any(), any(), any(), any(), any());
  }

  private void runImport(String csvContent) throws Exception {
    File csv = temporaryFolder.newFile("schema_" + System.nanoTime() + ".csv");
    Files.write(csv.toPath(), csvContent.getBytes(StandardCharsets.UTF_8));
    String failedFilePath = temporaryFolder.getRoot().getAbsolutePath() + "/failed";
    try (Stream<org.apache.commons.csv.CSVRecord> records =
        ImportSchemaTree.readCsvFile(csv.getAbsolutePath()).stream()) {
      List<String> headerNames = ImportSchemaTree.readHeaderNames(csv.getAbsolutePath());
      ImportSchemaTree.writeScheme(csv.getName(), headerNames, records, failedFilePath);
    }
  }
}
