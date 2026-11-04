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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.commons.csv.CSVParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.stubbing.Answer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class ImportDataBatchTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final boolean usePool;
  private final boolean byDevice;
  private final boolean aligned;
  private final List<List<Long>> requests = new ArrayList<>();
  private Boolean originalAligned;
  private int originalBatchSize;
  private Path failed;
  private String csv;

  @Parameterized.Parameters(name = "pool={0}, byDevice={1}, aligned={2}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(
        new Object[][] {
          {false, false, false},
          {false, false, true},
          {false, true, false},
          {false, true, true},
          {true, false, false},
          {true, false, true},
          {true, true, false},
          {true, true, true}
        });
  }

  public ImportDataBatchTest(boolean usePool, boolean byDevice, boolean aligned) {
    this.usePool = usePool;
    this.byDevice = byDevice;
    this.aligned = aligned;
  }

  @Before
  public void setUp() {
    originalAligned = AbstractDataTool.aligned;
    originalBatchSize = AbstractDataTool.batchPointSize;
    AbstractDataTool.aligned = aligned;
    AbstractDataTool.batchPointSize = 100_000;
    AbstractDataTool.resetCurrentImportFailed();
    failed = temporaryFolder.getRoot().toPath().resolve("failed");
    csv = header() + row(1) + row(2) + row(3);
  }

  @After
  public void tearDown() {
    AbstractDataTool.aligned = originalAligned;
    AbstractDataTool.batchPointSize = originalBatchSize;
    AbstractDataTool.resetCurrentImportFailed();
  }

  @Test
  public void connectionFailureRetriesTheBatchOnly() throws Exception {
    writeCsv(
        times -> {
          throw new IoTDBConnectionException("connection failed");
        });

    assertEquals(4, requests.size());
    for (List<Long> request : requests) {
      assertEquals(Arrays.asList(1L, 2L, 3L), request);
    }
    assertTrue(AbstractDataTool.isCurrentImportFailed());
    assertFailedCsv(csv);
  }

  @Test
  public void statementFailureRecoversGoodRows() throws Exception {
    List<Long> imported = new ArrayList<>();
    writeCsv(
        times -> {
          if (times.size() > 1 || times.get(0) == 2L) {
            throw new StatementExecutionException("bad row");
          }
          imported.addAll(times);
        });

    assertEquals(4, requests.size());
    assertEquals(Arrays.asList(1L, 3L), imported);
    assertTrue(AbstractDataTool.isCurrentImportFailed());
    assertFailedCsv(header() + row(2));
  }

  @Test
  public void connectionFailureDuringRowRecoveryStopsRemainingRetries() throws Exception {
    writeCsv(
        times -> {
          if (times.size() > 1) {
            throw new StatementExecutionException("batch failed");
          }
          throw new IoTDBConnectionException("connection lost during row recovery");
        });

    assertEquals(5, requests.size());
    assertEquals(Arrays.asList(1L, 2L, 3L), requests.get(0));
    for (int index = 1; index < requests.size(); index++) {
      assertEquals(Arrays.asList(1L), requests.get(index));
    }
    assertTrue(AbstractDataTool.isCurrentImportFailed());
    assertFailedCsv(csv);
  }

  @Test
  public void transientConnectionFailureKeepsTheWholeBatch() throws Exception {
    writeCsv(
        times -> {
          if (requests.size() == 1) {
            throw new IoTDBConnectionException("temporary outage");
          }
        });

    assertEquals(2, requests.size());
    assertEquals(requests.get(0), requests.get(1));
    assertFalse(AbstractDataTool.isCurrentImportFailed());
    assertFalse(Files.exists(Path.of(failed + "_0")));
  }

  @Test
  public void recoveryUsesTheCurrentBatchAfterFlushing() throws Exception {
    AbstractDataTool.batchPointSize = 2;
    writeCsv(
        times -> {
          if (times.size() > 1 || times.get(0) == 3L) {
            throw new StatementExecutionException("bad batch or last row");
          }
        });

    assertEquals(
        Arrays.asList(
            Arrays.asList(1L, 2L),
            Arrays.asList(1L),
            Arrays.asList(2L),
            Arrays.asList(3L),
            Arrays.asList(3L)),
        requests);
    assertTrue(AbstractDataTool.isCurrentImportFailed());
    assertFailedCsv(header() + row(3));
  }

  private void writeCsv(InsertBehavior behavior) throws Exception {
    String method =
        (aligned ? "insertAlignedRecords" : "insertRecords") + (byDevice ? "OfOneDevice" : "");
    Answer<Object> answer =
        invocation -> {
          if (invocation.getMethod().getName().startsWith("insert")) {
            assertEquals(method, invocation.getMethod().getName());
            List<Long> times = invocation.getArgument(1);
            List<List<Object>> values = invocation.getArgument(4);
            for (int index = 0; index < times.size(); index++) {
              assertEquals(times.get(index).intValue(), values.get(index).get(0));
            }
            requests.add(new ArrayList<>(times));
            behavior.insert(times);
            return null;
          }
          return RETURNS_DEFAULTS.answer(invocation);
        };
    Object client = usePool ? mock(SessionPool.class, answer) : mock(Session.class, answer);
    Path input = temporaryFolder.newFile("input.csv").toPath();
    Files.writeString(input, csv);
    try (CSVParser parser = AbstractDataTool.readCsvFile(input.toString())) {
      if (usePool) {
        if (byDevice) {
          AbstractDataTool.writeDataAlignedByDevice(
              (SessionPool) client, parser.getHeaderNames(), parser.stream(), failed.toString());
        } else {
          AbstractDataTool.writeDataAlignedByTime(
              (SessionPool) client, parser.getHeaderNames(), parser.stream(), failed.toString());
        }
      } else if (byDevice) {
        AbstractDataTool.writeDataAlignedByDevice(
            (Session) client, parser.getHeaderNames(), parser.stream(), failed.toString());
      } else {
        AbstractDataTool.writeDataAlignedByTime(
            (Session) client, parser.getHeaderNames(), parser.stream(), failed.toString());
      }
    }
  }

  private String header() {
    return byDevice ? "Time,Device,s1(INT32)\n" : "Time,root.d.s1(INT32)\n";
  }

  private String row(int timestamp) {
    return timestamp + (byDevice ? ",root.d," : ",") + timestamp + "\n";
  }

  private void assertFailedCsv(String expected) throws Exception {
    assertEquals(expected, Files.readString(Path.of(failed + "_0")).replace("\r\n", "\n"));
  }

  @FunctionalInterface
  private interface InsertBehavior {
    void insert(List<Long> times) throws IoTDBConnectionException, StatementExecutionException;
  }
}
