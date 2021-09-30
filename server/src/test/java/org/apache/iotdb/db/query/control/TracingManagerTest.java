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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.tracing.TracingManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TracingManagerTest {

  private final String tracingDir = IoTDBDescriptor.getInstance().getConfig().getTracingDir();
  private TracingManager tracingManager;
  private final String statement = "tracing select * from root.sg.device1 where time > 10";
  private final long statementId = 10;

  private final Set<TsFileResource> seqResources = new HashSet<>();

  @Before
  public void setUp() {
    tracingManager = TracingManager.getInstance();
    prepareTsFileResources();
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(new File(tracingDir));
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void tracingQueryTest() throws IOException {
    //    if (!tracingManager.getWriterStatus()) {
    //      tracingManager.openTracingWriteStream();
    //    }
    //    TracingInfo tracingInfo = tracingManager.getTracingInfo(statementId);
    //    tracingInfo.setStartTime(System.currentTimeMillis());
    //    String[] ans = {
    //      "Statement Id: 10 - Statement: " + statement,
    //      "Statement Id: 10 - Start time: ",
    //      "Statement Id: 10 - Number of series paths: ",
    //      "Statement Id: 10 - Statement: " + statement,
    //      "Statement Id: 10 - Start time: ",
    //      "Statement Id: 10 - Number of series paths: ",
    //      "Statement Id: 10 - Number of sequence files read: 1",
    //      "Statement Id: 10 - SeqFiles: 1-1-0.tsfile",
    //      "Statement Id: 10 - Number of unSequence files read: 0",
    //      "Statement Id: 10 - Num of sequence chunks: 3, avg points: 1371.0",
    //      "Statement Id: 10 - Num of unsequence chunks: 3, avg points: 1371.0",
    //      "Statement Id: 10 - Num of Pages: 20, overlapped pages: 10 (50.0%)",
    //      "Statement Id: 10 - Total cost time: "
    //    };
    //    tracingManager.writeQueryInfo(statementId, statement);
    //    tracingManager.writePathsNum(statementId, 3);
    //    tracingManager.writeQueryInfo(statementId, statement, 3);
    //    tracingManager.writeTsFileInfo(statementId, seqResources, Collections.EMPTY_SET);
    //    tracingManager.writeSeqChunksInfo(statementId, 3, 4113L);
    //    tracingManager.writeUnSeqChunksInfo(statementId, 3, 4113L);
    //    tracingManager.writeOverlappedPageInfo(statementId, 20, 10);
    //    tracingManager.writeEndTime(statementId, System.currentTimeMillis());
    //    tracingManager.close();
    //
    //    File tracingFile =
    //        SystemFileFactory.INSTANCE.getFile(tracingDir + File.separator +
    // IoTDBConstant.TRACING_LOG);
    //    BufferedReader bufferedReader = new BufferedReader(new FileReader(tracingFile));
    //    String str;
    //    int cnt = 0;
    //    while (cnt < ans.length) {
    //      str = bufferedReader.readLine();
    //      Assert.assertTrue(str.contains(ans[cnt++]));
    //    }
    //    bufferedReader.close();
  }

  void prepareTsFileResources() {
    Map<String, Integer> deviceToIndex = new HashMap<>();
    deviceToIndex.put("root.sg.d1", 0);
    deviceToIndex.put("root.sg.d2", 1);
    long[] startTimes = {1, 2};
    long[] endTimes = {999, 998};
    File file1 = new File(TestConstant.OUTPUT_DATA_DIR.concat("1-1-0.tsfile"));
    TsFileResource tsFileResource1 = new TsFileResource(file1, deviceToIndex, startTimes, endTimes);
    tsFileResource1.setClosed(true);
    seqResources.add(tsFileResource1);
  }
}
