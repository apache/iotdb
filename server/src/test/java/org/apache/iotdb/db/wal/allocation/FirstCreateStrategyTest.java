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
package org.apache.iotdb.db.wal.allocation;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.wal.node.IWALNode;
import org.apache.iotdb.db.wal.utils.WALFileUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FirstCreateStrategyTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final String[] walDirs =
      new String[] {
        TestConstant.BASE_OUTPUT_PATH.concat("wal_test1"),
        TestConstant.BASE_OUTPUT_PATH.concat("wal_test2"),
        TestConstant.BASE_OUTPUT_PATH.concat("wal_test3")
      };
  private String[] prevWalDirs;

  @Before
  public void setUp() throws Exception {
    prevWalDirs = config.getWalDirs();
    config.setWalDirs(walDirs);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    for (String walDir : walDirs) {
      EnvironmentUtils.cleanDir(walDir);
    }
    config.setWalDirs(prevWalDirs);
  }

  @Test
  public void testAllocateWALNode() throws IllegalPathException {
    FirstCreateStrategy roundRobinStrategy = new FirstCreateStrategy();
    IWALNode[] walNodes = new IWALNode[6];
    try {
      for (int i = 0; i < 12; i++) {
        String identifier = String.valueOf(i % 6);
        IWALNode walNode = roundRobinStrategy.applyForWALNode(identifier);
        if (i < 6) {
          walNodes[i] = walNode;
        } else {
          assertEquals(walNodes[i % 6], walNode);
        }
        walNode.log(i, getInsertRowPlan());
      }
      for (String walDir : walDirs) {
        File walDirFile = new File(walDir);
        assertTrue(walDirFile.exists());
        File[] nodeDirs = walDirFile.listFiles(File::isDirectory);
        assertNotNull(nodeDirs);
        assertEquals(2, nodeDirs.length);
        for (File nodeDir : nodeDirs) {
          assertTrue(nodeDir.exists());
          assertNotEquals(0, WALFileUtils.listAllWALFiles(nodeDir).length);
        }
      }
    } finally {
      for (IWALNode walNode : walNodes) {
        if (walNode != null) {
          walNode.close();
        }
      }
    }
  }

  @Test
  public void testRegisterWALNode() throws IllegalPathException {
    FirstCreateStrategy roundRobinStrategy = new FirstCreateStrategy();
    IWALNode[] walNodes = new IWALNode[6];
    try {
      for (int i = 0; i < 12; i++) {
        String identifier = String.valueOf(i % 6);
        roundRobinStrategy.registerWALNode(identifier, walDirs[0] + File.separator + identifier);
        IWALNode walNode = roundRobinStrategy.applyForWALNode(identifier);
        if (i < 6) {
          walNodes[i] = walNode;
        } else {
          assertEquals(walNodes[i % 6], walNode);
        }
        walNode.log(i, getInsertRowPlan());
      }

      File walDirFile = new File(walDirs[0]);
      assertTrue(walDirFile.exists());
      File[] nodeDirs = walDirFile.listFiles(File::isDirectory);
      assertNotNull(nodeDirs);
      assertEquals(6, nodeDirs.length);
      for (File nodeDir : nodeDirs) {
        assertTrue(nodeDir.exists());
        assertNotEquals(0, WALFileUtils.listAllWALFiles(nodeDir).length);
      }
    } finally {
      for (IWALNode walNode : walNodes) {
        if (walNode != null) {
          walNode.close();
        }
      }
    }
  }

  private InsertRowPlan getInsertRowPlan() throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    String[] columns = new String[6];
    columns[0] = 1.0 + "";
    columns[1] = 2 + "";
    columns[2] = 10000 + "";
    columns[3] = 100 + "";
    columns[4] = false + "";
    columns[5] = "hh" + 0;

    return new InsertRowPlan(
        new PartialPath("root.test_sg.test_d"),
        time,
        new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
        dataTypes,
        columns);
  }
}
