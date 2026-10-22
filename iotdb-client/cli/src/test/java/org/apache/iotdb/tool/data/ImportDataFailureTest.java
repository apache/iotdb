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

import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.commons.csv.CSVParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class ImportDataFailureTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() {
    AbstractDataTool.aligned = false;
  }

  @After
  public void tearDown() {
    AbstractDataTool.aligned = null;
  }

  @Test
  public void failedBatchWritesEachOriginalCsvRecordOnce() throws Exception {
    SessionPool sessionPool = mock(SessionPool.class);
    doAnswer(
            invocation -> {
              throw new StatementExecutionException("insert failed");
            })
        .when(sessionPool)
        .insertRecords(anyList(), anyList(), anyList(), anyList(), anyList());

    Path input = temporaryFolder.newFile("input.csv").toPath();
    Files.writeString(input, "Time,root.d.s1(INT32),root.e.s1(INT32)\n1,1,2\n");
    Path failed = temporaryFolder.getRoot().toPath().resolve("failed");

    try (CSVParser parser = AbstractDataTool.readCsvFile(input.toString())) {
      AbstractDataTool.writeDataAlignedByTime(
          sessionPool, parser.getHeaderNames(), parser.stream(), failed.toString());
    }

    assertEquals(
        "Time,root.d.s1(INT32),root.e.s1(INT32)\n1,1,2\n",
        Files.readString(Path.of(failed + "_0")).replace("\r\n", "\n"));
  }

  @Test
  public void failedDeviceGroupWritesTheWholeOriginalRecordOnce() throws Exception {
    SessionPool sessionPool = mock(SessionPool.class);
    doAnswer(
            invocation -> {
              if (((java.util.List<?>) invocation.getArgument(0)).size() == 1
                  && "root.e".equals(((java.util.List<?>) invocation.getArgument(0)).get(0))) {
                throw new StatementExecutionException("device insert failed");
              }
              if (((java.util.List<?>) invocation.getArgument(0)).size() > 1) {
                throw new StatementExecutionException("batch insert failed");
              }
              return null;
            })
        .when(sessionPool)
        .insertRecords(anyList(), anyList(), anyList(), anyList(), anyList());

    Path input = temporaryFolder.newFile("input.csv").toPath();
    Files.writeString(input, "Time,root.d.s1(INT32),root.e.s1(INT32)\n1,1,2\n");
    Path failed = temporaryFolder.getRoot().toPath().resolve("failed");

    try (CSVParser parser = AbstractDataTool.readCsvFile(input.toString())) {
      AbstractDataTool.writeDataAlignedByTime(
          sessionPool, parser.getHeaderNames(), parser.stream(), failed.toString());
    }

    assertEquals(
        "Time,root.d.s1(INT32),root.e.s1(INT32)\n1,1,2\n",
        Files.readString(Path.of(failed + "_0")).replace("\r\n", "\n"));
  }
}
