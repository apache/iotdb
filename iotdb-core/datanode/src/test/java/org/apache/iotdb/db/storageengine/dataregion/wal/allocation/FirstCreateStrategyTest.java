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
package org.apache.iotdb.db.storageengine.dataregion.wal.allocation;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.IWALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FirstCreateStrategyTest {
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private final String[] walDirs =
      new String[] {
        TestConstant.BASE_OUTPUT_PATH.concat("wal_test1"),
        TestConstant.BASE_OUTPUT_PATH.concat("wal_test2"),
        TestConstant.BASE_OUTPUT_PATH.concat("wal_test3")
      };
  private String[] prevWalDirs;

  @Before
  public void setUp() throws Exception {
    prevWalDirs = commonConfig.getWalDirs();
    EnvironmentUtils.envSetUp();
    commonConfig.setWalDirs(walDirs);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    for (String walDir : walDirs) {
      EnvironmentUtils.cleanDir(walDir);
    }
    commonConfig.setWalDirs(prevWalDirs);
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
        walNode.log(i, getInsertRowNode());
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
        roundRobinStrategy.registerWALNode(
            identifier, walDirs[0] + File.separator + identifier, 0, 0L);
        IWALNode walNode = roundRobinStrategy.applyForWALNode(identifier);
        if (i < 6) {
          walNodes[i] = walNode;
        } else {
          assertEquals(walNodes[i % 6], walNode);
        }
        walNode.log(i, getInsertRowNode());
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

  @Test
  public void testReInitializeAfterDiskSpaceCleaned() throws IllegalPathException, IOException {
    // Create unique temporary directory for testing
    Path tempDir = Files.createTempDirectory("iotdb_wal_reinit_test_");

    String[] testWalDirs =
        new String[] {
          tempDir.resolve("wal_reinit_test1").toString(),
          tempDir.resolve("wal_reinit_test2").toString(),
          tempDir.resolve("wal_reinit_test3").toString()
        };

    String[] originalWalDirs = commonConfig.getWalDirs();

    try {
      commonConfig.setWalDirs(testWalDirs);
      // Create strategy with valid directories first
      FirstCreateStrategy strategy = new FirstCreateStrategy();

      // Simulate folderManager becoming null (e.g., due to disk space issues)
      // We'll use reflection to set folderManager to null to test re-initialization
      try {
        java.lang.reflect.Field folderManagerField =
            AbstractNodeAllocationStrategy.class.getDeclaredField("folderManager");
        folderManagerField.setAccessible(true);
        folderManagerField.set(strategy, null);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException("Failed to set folderManager to null for testing", e);
      }

      // Now apply for WAL node, should successfully re-initialize folderManager
      IWALNode walNode = strategy.applyForWALNode("test_reinit_identifier");
      assertNotNull("WAL node should be created after re-initialization", walNode);

      // Verify that re-initialization actually occurred - should return WALNode, not WALFakeNode
      assertTrue(
          "Returned node should be WALNode instance after successful re-initialization",
          walNode instanceof WALNode);

      // Verify that WAL node was created successfully by logging data
      walNode.log(1, getInsertRowNode());

      // Verify that WAL files were created in at least one directory
      boolean walFileCreated = false;
      for (String walDir : testWalDirs) {
        File walDirFile = new File(walDir);
        if (walDirFile.exists()) {
          File[] nodeDirs = walDirFile.listFiles(File::isDirectory);
          if (nodeDirs != null && nodeDirs.length > 0) {
            for (File nodeDir : nodeDirs) {
              if (nodeDir.exists() && WALFileUtils.listAllWALFiles(nodeDir).length > 0) {
                walFileCreated = true;
                break;
              }
            }
          }
        }
        if (walFileCreated) {
          break;
        }
      }
      assertTrue("WAL files should be created after re-initialization", walFileCreated);

      // Clean up
      walNode.close();
    } finally {
      // Clean up the test directories
      for (String walDir : testWalDirs) {
        EnvironmentUtils.cleanDir(walDir);
      }
      // Clean up temp directory
      EnvironmentUtils.cleanDir(tempDir.toString());
      // Restore original WAL directories
      commonConfig.setWalDirs(originalWalDirs);
    }
  }

  private InsertRowNode getInsertRowNode() throws IllegalPathException {
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

    Object[] columns = new Object[6];
    columns[0] = 1.0;
    columns[1] = 2.0F;
    columns[2] = 10000L;
    columns[3] = 100;
    columns[4] = false;
    columns[5] = new Binary("hh" + 0, TSFileConfig.STRING_CHARSET);

    MeasurementSchema[] schemas =
        new MeasurementSchema[] {
          new MeasurementSchema("s1", dataTypes[0]),
          new MeasurementSchema("s2", dataTypes[1]),
          new MeasurementSchema("s3", dataTypes[2]),
          new MeasurementSchema("s4", dataTypes[3]),
          new MeasurementSchema("s5", dataTypes[4]),
          new MeasurementSchema("s6", dataTypes[5]),
        };

    return new InsertRowNode(
        new PlanNodeId("0"),
        new PartialPath("root.test_sg.test_d"),
        false,
        new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
        dataTypes,
        schemas,
        time,
        columns,
        true);
  }
}
