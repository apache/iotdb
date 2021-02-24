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

package org.apache.iotdb.db.writelog;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.MmapUtil;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class IoTDBLogFileSizeTest {

  private boolean skip = true;

  private int groupSize;
  private long runtime = 600000;

  private String[] setUpSqls =
      new String[] {
        "SET STORAGE GROUP TO root.logFileTest.seq",
        "SET STORAGE GROUP TO root.logFileTest.unsequence",
        "CREATE TIMESERIES root.logFileTest.seq.val WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.logFileTest.unsequence.val WITH DATATYPE=INT32, ENCODING=PLAIN",
        // unsequence baseline
        "INSERT INTO root.logFileTest.unsequence(timestamp,val) VALUES (1000000000, 0)"
      };

  private String[] tearDownSqls = new String[] {"DELETE TIMESERIES root.logFileTest.*"};

  @Before
  public void setUp() throws Exception {
    if (skip) {
      return;
    }
    groupSize = TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte();
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(8 * 1024 * 1024);
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(8 * 1024 * 1024);
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    executeSQL(setUpSqls);
  }

  @After
  public void tearDown() throws Exception {
    if (skip) {
      return;
    }
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(groupSize);
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(groupSize);
    executeSQL(tearDownSqls);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSeqFile() throws InterruptedException {
    if (skip) {
      return;
    }
    final long[] maxLength = {0};
    ByteBuffer[] buffers = new ByteBuffer[2];
    buffers[0] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    buffers[1] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    Thread writeThread =
        new Thread(
            () -> {
              int cnt = 0;
              try {
                Class.forName(Config.JDBC_DRIVER_NAME);
              } catch (ClassNotFoundException e) {
                e.printStackTrace();
                return;
              }

              try (Connection connection =
                      DriverManager.getConnection(
                          Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
                  Statement statement = connection.createStatement()) {
                // System.out.println("Exit after " + cnt + " insertion");
                while (!Thread.interrupted()) {
                  String sql =
                      String.format(
                          "INSERT INTO root.logFileTest.seq(timestamp,val) VALUES (%d, %d)",
                          ++cnt, cnt);
                  statement.execute(sql);
                  WriteLogNode logNode =
                      MultiFileLogNodeManager.getInstance()
                          .getNode(
                              "root.logFileTest.seq" + IoTDBConstant.SEQFILE_LOG_NODE_SUFFIX,
                              () -> buffers);
                  File bufferWriteWALFile =
                      new File(
                          logNode.getLogDirectory()
                              + File.separator
                              + ExclusiveWriteLogNode.WAL_FILE_NAME);
                  if (bufferWriteWALFile.exists() && bufferWriteWALFile.length() > maxLength[0]) {
                    maxLength[0] = bufferWriteWALFile.length();
                  }
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
            });
    writeThread.start();
    Thread.sleep(runtime);
    writeThread.interrupt();
    while (writeThread.isAlive()) {}

    for (ByteBuffer byteBuffer : buffers) {
      MmapUtil.clean((MappedByteBuffer) byteBuffer);
    }
  }

  @Test
  public void testUnsequence() throws InterruptedException {
    if (skip) {
      return;
    }
    ByteBuffer[] buffers = new ByteBuffer[2];
    buffers[0] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    buffers[1] =
        ByteBuffer.allocateDirect(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
    final long[] maxLength = {0};
    Thread writeThread =
        new Thread(
            () -> {
              int cnt = 0;
              try {
                Class.forName(Config.JDBC_DRIVER_NAME);
              } catch (ClassNotFoundException e) {
                e.printStackTrace();
                return;
              }
              try (Connection connection =
                      DriverManager.getConnection(
                          Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
                  Statement statement = connection.createStatement()) {
                // System.out.println("Exit after " + cnt + " insertion");
                while (!Thread.interrupted()) {
                  String sql =
                      String.format(
                          "INSERT INTO root.logFileTest.unsequence(timestamp,val) VALUES (%d, %d)",
                          ++cnt, cnt);
                  statement.execute(sql);
                  WriteLogNode logNode =
                      MultiFileLogNodeManager.getInstance()
                          .getNode(
                              "root.logFileTest.unsequence"
                                  + IoTDBConstant.UNSEQFILE_LOG_NODE_SUFFIX,
                              () -> buffers);
                  File WALFile =
                      new File(
                          logNode.getLogDirectory()
                              + File.separator
                              + ExclusiveWriteLogNode.WAL_FILE_NAME);
                  if (WALFile.exists() && WALFile.length() > maxLength[0]) {
                    maxLength[0] = WALFile.length();
                  }
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
            });
    writeThread.start();
    Thread.sleep(runtime);
    writeThread.interrupt();
    while (writeThread.isAlive()) {}

    for (ByteBuffer byteBuffer : buffers) {
      MmapUtil.clean((MappedByteBuffer) byteBuffer);
    }
  }

  private void executeSQL(String[] sqls) throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
