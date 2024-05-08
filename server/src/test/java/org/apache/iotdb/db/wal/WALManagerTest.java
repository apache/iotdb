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
package org.apache.iotdb.db.wal;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.wal.allocation.ElasticStrategy;
import org.apache.iotdb.db.wal.node.WALNode;
import org.apache.iotdb.db.wal.utils.WALFileUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WALManagerTest {
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
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
    prevWalDirs = commonConfig.getWalDirs();
    commonConfig.setWalDirs(walDirs);
    EnvironmentUtils.envSetUp();
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
  public void testDeleteOutdatedWALFiles() throws IllegalPathException {
    WALManager walManager = WALManager.getInstance();
    WALNode[] walNodes = new WALNode[3];
    for (int i = 0; i < 12; i++) {
      WALNode walNode = (WALNode) walManager.applyForWALNode(String.valueOf(i));
      if (i % ElasticStrategy.APPLICATION_NODE_RATIO == 0) {
        walNodes[i / ElasticStrategy.APPLICATION_NODE_RATIO] = walNode;
      } else {
        assertEquals(walNodes[i / ElasticStrategy.APPLICATION_NODE_RATIO], walNode);
      }
      walNode.log(i, getInsertRowNode());
      walNode.rollWALFile();
    }

    for (String walDir : walDirs) {
      File walDirFile = new File(walDir);
      assertTrue(walDirFile.exists());
      File[] nodeDirs = walDirFile.listFiles(File::isDirectory);
      assertNotNull(nodeDirs);
      for (File nodeDir : nodeDirs) {
        assertTrue(nodeDir.exists());
        assertEquals(5, WALFileUtils.listAllWALFiles(nodeDir).length);
      }
    }

    walManager.deleteOutdatedWALFiles();

    for (String walDir : walDirs) {
      File walDirFile = new File(walDir);
      assertTrue(walDirFile.exists());
      File[] nodeDirs = walDirFile.listFiles(File::isDirectory);
      assertNotNull(nodeDirs);
      for (File nodeDir : nodeDirs) {
        assertTrue(nodeDir.exists());
        assertEquals(1, WALFileUtils.listAllWALFiles(nodeDir).length);
      }
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
    columns[0] = 1.0d;
    columns[1] = 2.0f;
    columns[2] = 10000L;
    columns[3] = 100;
    columns[4] = false;
    columns[5] = new Binary("hh" + 0);

    InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath("root.test_sg.test_d"),
            false,
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes,
            time,
            columns,
            false);
    node.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.DOUBLE),
          new MeasurementSchema("s2", TSDataType.FLOAT),
          new MeasurementSchema("s3", TSDataType.INT64),
          new MeasurementSchema("s4", TSDataType.INT32),
          new MeasurementSchema("s5", TSDataType.BOOLEAN),
          new MeasurementSchema("s6", TSDataType.TEXT)
        });
    return node;
  }
}
