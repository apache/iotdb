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
package org.apache.iotdb.db.query.control;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TracingClearAllManagerTest {

  private TracingManager tracingManager = TracingManager.getInstance();
  private final String tracingDir = IoTDBDescriptor.getInstance().getConfig().getTracingDir();
  private final String sql = "select * from root.sg.device1 where time > 10";
  private final long queryId = 10;

  private Set<TsFileResource> seqResources = new HashSet<>();

  @Before
  public void setUp() {
    System.out.println("do Before");
  }

  @After
  public void tearDown() throws IOException {
    System.out.println("do After");
    FileUtils.deleteDirectory(new File(tracingDir));
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void tracingQueryTest() throws IOException {
    System.out.println("do Test");
    String[] ans = {
      "Query Id: 10 - Query Statement: " + sql,
      "Query Id: 10 - Start time: 2020-12-",
      "Query Id: 10 - Number of series paths: 3",
      "Query Id: 10 - Query Statement: " + sql,
      "Query Id: 10 - Start time: 2020-12-",
      "Query Id: 10 - Number of series paths: 3",
      "Query Id: 10 - Number of sequence files: 1",
      "Query Id: 10 - SeqFile_1-1-0.tsfile root.sg.d1[1, 999], root.sg.d2[2, 998]",
      "Query Id: 10 - Number of unSequence files: 0",
      "Query Id: 10 - Number of chunks: 3",
      "Query Id: 10 - Average size of chunks: 1371",
      "Query Id: 10 - Total cost time: "
    };
    tracingManager.writeQueryInfo(queryId, sql, 1607529600000L);
    tracingManager.writePathsNum(queryId, 3);
    tracingManager.writeQueryInfo(queryId, sql, 1607529600000L, 3);
    tracingManager.writeChunksInfo(queryId, 3, 4113L);
    tracingManager.writeEndTime(queryId);
    tracingManager.close();

    /*tracing clear all UT*/
    tracingManager.clearTracingAllInfo(tracingDir, IoTDBConstant.TRACING_LOG);
    File tracingClearAllTestFile =
        SystemFileFactory.INSTANCE.getFile(tracingDir + File.separator + IoTDBConstant.TRACING_LOG);
    Assert.assertTrue(tracingClearStatues(tracingClearAllTestFile));
  }

  public boolean tracingClearStatues(File tracingClearAllTestFile) throws IOException {
    try {
      BufferedReader tracingClearAllTestBufferedReader =
          new BufferedReader(new FileReader(tracingClearAllTestFile));
      tracingClearAllTestBufferedReader.close();
      return false;
    } catch (FileNotFoundException e) {
      return true;
    }
  }
}
