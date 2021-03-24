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

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TracingClearAllManagerTest {

  private final String tracingDir = IoTDBDescriptor.getInstance().getConfig().getTracingDir();
  private final String sql = "select * from root.sg.device1 where time > 10";
  private final long queryId = 10;

  private Set<TsFileResource> seqResources = new HashSet<>();

  @BeforeClass
  public static void setUp() throws ClassNotFoundException {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    System.out.println("do Before");
  }

  @AfterClass
  public static void tearDown() throws IOException, StorageEngineException, ClassNotFoundException {
    System.out.println("do After");
    EnvironmentUtils.cleanEnv();
    // FileUtils.deleteDirectory(new File(tracingDir));
    // EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void tracingClearTest() throws IOException, ClassNotFoundException {
    System.out.println("do Test");
    tracingSwitch("tracing on");
    TracingManager tracingManager = TracingManager.getInstance();
    // tracingManager.reOpenBufferedWriter(tracingDir, IoTDBConstant.TRACING_LOG);
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
    tracingSwitch("tracing off");
    tracingManager.clearTracingAllInfo(tracingDir, IoTDBConstant.TRACING_LOG);
    File tracingClearAllTestFile =
        SystemFileFactory.INSTANCE.getFile(tracingDir + File.separator + IoTDBConstant.TRACING_LOG);

    Assert.assertTrue(tracingClearStatues(tracingClearAllTestFile));
  }

  public boolean tracingClearStatues(File tracingClearAllTestFile) throws IOException {
    BufferedReader tracingClearAllTestBufferedReader = null;
    try {
      tracingClearAllTestBufferedReader =
          new BufferedReader(new FileReader(tracingClearAllTestFile));
      tracingClearAllTestBufferedReader.close();
      return false;
    } catch (FileNotFoundException e) {
      return true;
    } finally {
      if (tracingClearAllTestBufferedReader != null) {
        tracingClearAllTestBufferedReader.close();
      }
    }
  }

  private static void tracingSwitch(String sql) throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      statement.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
