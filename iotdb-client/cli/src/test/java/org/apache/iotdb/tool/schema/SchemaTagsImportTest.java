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
import org.apache.tsfile.external.commons.collections4.CollectionUtils;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
    ImportSchemaTree.batchPointSize = Constants.BATCH_POINT_SIZE;
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

  /**
   * Guards the batch-flush boundary: every aliased list must be emptied together, otherwise the
   * next batch pairs the aliases still buffered with the wrong paths.
   */
  @Test
  public void batchedAliasedRecordsKeepAliasTagsAndPathsPaired() throws Exception {
    SessionPool pool = mock(SessionPool.class);
    ImportSchemaTree.sessionPool = pool;
    ImportSchemaTree.batchPointSize = 2;
    List<List<List<?>>> calls = new ArrayList<>();
    doAnswer(
            call -> {
              List<List<?>> snapshot = new ArrayList<>();
              for (int i = 0; i < call.getArguments().length; i++) {
                List<?> argument = call.getArgument(i);
                snapshot.add(argument == null ? null : new ArrayList<>(argument));
              }
              calls.add(snapshot);
              return null;
            })
        .when(pool)
        .createMultiTimeseries(any(), any(), any(), any(), any(), any(), any(), any());

    File csv = temporaryFolder.newFile("batch.csv");
    AbstractSchemaTool.CSVPrinterWrapper printer =
        new AbstractSchemaTool.CSVPrinterWrapper(csv.getAbsolutePath());
    printer.printRecord(Constants.HEAD_COLUMNS_V2);
    for (int i = 1; i <= 5; i++) {
      printer.print("root.sg.d.s" + i);
      printer.print("alias" + i);
      printer.print("INT64");
      printer.print("TS_2DIFF");
      printer.print("SNAPPY");
      printer.print(AbstractSchemaTool.toJsonCell(Collections.singletonMap("k", "v" + i)));
      printer.print("");
      printer.println();
    }
    printer.close();
    runImportCsv(csv);

    // 3 flushes with batchPointSize 2: [s1,s2], [s3,s4], [s5]
    assertEquals(3, calls.size());
    List<String> sentPaths = new ArrayList<>();
    for (List<List<?>> call : calls) {
      List<?> paths = call.get(0);
      List<?> dataTypes = call.get(1);
      List<?> encodings = call.get(2);
      List<?> compressors = call.get(3);
      List<?> tags = call.get(5);
      List<?> attributes = call.get(6);
      List<?> measurementAlias = call.get(7);
      assertTrue(CollectionUtils.isNotEmpty(paths));
      // every list that is indexed by the path position must have the same length
      for (List<?> parallel :
          Arrays.asList(dataTypes, encodings, compressors, tags, attributes, measurementAlias)) {
        assertEquals("lists must stay positionally aligned", paths.size(), parallel.size());
      }
      for (int i = 0; i < paths.size(); i++) {
        String path = (String) paths.get(i);
        String seq = path.substring(path.length() - 1);
        sentPaths.add(path);
        // s<n> must travel with alias<n> and with the tag built from the same n
        assertEquals(path, "alias" + seq, measurementAlias.get(i));
        assertEquals("v" + seq, ((Map<?, ?>) tags.get(i)).get("k"));
      }
    }
    assertEquals(
        Arrays.asList(
            "root.sg.d.s1", "root.sg.d.s2", "root.sg.d.s3", "root.sg.d.s4", "root.sg.d.s5"),
        sentPaths);
  }

  private void runImport(String csvContent) throws Exception {
    File csv = temporaryFolder.newFile("schema_" + System.nanoTime() + ".csv");
    Files.write(csv.toPath(), csvContent.getBytes(StandardCharsets.UTF_8));
    runImportCsv(csv);
  }

  private void runImportCsv(File csv) throws Exception {
    String failedFilePath = temporaryFolder.getRoot().getAbsolutePath() + "/failed";
    try (Stream<org.apache.commons.csv.CSVRecord> records =
        ImportSchemaTree.readCsvFile(csv.getAbsolutePath()).stream()) {
      List<String> headerNames = ImportSchemaTree.readHeaderNames(csv.getAbsolutePath());
      ImportSchemaTree.writeScheme(csv.getName(), headerNames, records, failedFilePath);
    }
  }
}
