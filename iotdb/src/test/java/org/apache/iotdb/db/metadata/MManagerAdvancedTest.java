/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MManagerAdvancedTest {

  private static MManager mmanager = null;

  @Before
  public void setUp() throws Exception {

    mmanager = MManager.getInstance();

    mmanager.setStorageLevelToMTree("root.vehicle.d1");
    mmanager.setStorageLevelToMTree("root.vehicle.d0");

    mmanager.addPathToMTree("root.vehicle.d0.s0", "INT32", "RLE", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d0.s1", "INT64", "RLE", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d0.s2", "FLOAT", "RLE", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d0.s3", "DOUBLE", "RLE", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d0.s4", "BOOLEAN", "PLAIN", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d0.s5", "TEXT", "PLAIN", new String[0]);

    mmanager.addPathToMTree("root.vehicle.d1.s0", "INT32", "RLE", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d1.s1", "INT64", "RLE", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d1.s2", "FLOAT", "RLE", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d1.s3", "DOUBLE", "RLE", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d1.s4", "BOOLEAN", "PLAIN", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d1.s5", "TEXT", "PLAIN", new String[0]);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @org.junit.Test
  public void test() {

    try {
      // test file name
      List<String> fileNames = mmanager.getAllFileNames();
      assertEquals(2, fileNames.size());
      if (fileNames.get(0).equals("root.vehicle.d0")) {
        assertEquals(fileNames.get(1), "root.vehicle.d1");
      } else {
        assertEquals(fileNames.get(1), "root.vehicle.d0");
      }
      // test filename by seriesPath
      assertEquals("root.vehicle.d0", mmanager.getFileNameByPath("root.vehicle.d0.s1"));
      HashMap<String, ArrayList<String>> map = mmanager
          .getAllPathGroupByFileName("root.vehicle.d1.*");
      assertEquals(1, map.keySet().size());
      assertEquals(6, map.get("root.vehicle.d1").size());
      ArrayList<String> paths = mmanager.getPaths("root.vehicle.d0");
      assertEquals(6, paths.size());
      paths = mmanager.getPaths("root.vehicle.d2");
      assertEquals(0, paths.size());
    } catch (PathErrorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCache() throws PathErrorException, IOException, MetadataArgsErrorException {
    mmanager.addPathToMTree("root.vehicle.d2.s0", "DOUBLE", "RLE", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d2.s1", "BOOLEAN", "PLAIN", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d2.s2.g0", "TEXT", "PLAIN", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d2.s3", "TEXT", "PLAIN", new String[0]);

    Assert.assertEquals(TSDataType.INT32,
        mmanager.checkPathStorageLevelAndGetDataType("root.vehicle.d0.s0").getDataType());
    Assert.assertEquals(TSDataType.INT64,
        mmanager.checkPathStorageLevelAndGetDataType("root.vehicle.d0.s1").getDataType());

    Assert.assertEquals(false,
        mmanager.checkPathStorageLevelAndGetDataType("root.vehicle.d0.s100").isSuccessfully());
    Assert.assertEquals(null,
        mmanager.checkPathStorageLevelAndGetDataType("root.vehicle.d0.s100").getDataType());

    MNode node = mmanager.getNodeByDeviceIdFromCache("root.vehicle.d0");
    Assert.assertEquals(TSDataType.INT32, node.getChild("s0").getSchema().dataType);

    try {
      MNode node1 = mmanager.getNodeByDeviceIdFromCache("root.vehicle.d100");
      fail();
    } catch (PathErrorException e) {

    }
  }

  @Test
  public void testGetNextLevelPath()
      throws PathErrorException, IOException, MetadataArgsErrorException {
    mmanager.addPathToMTree("root.vehicle.d2.s0", "DOUBLE", "RLE", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d2.s1", "BOOLEAN", "PLAIN", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d2.s2.g0", "TEXT", "PLAIN", new String[0]);
    mmanager.addPathToMTree("root.vehicle.d2.s3", "TEXT", "PLAIN", new String[0]);

    List<String> paths = mmanager.getLeafNodePathInNextLevel("root.vehicle.d2");
    Assert.assertEquals(3, paths.size());

    paths = mmanager.getLeafNodePathInNextLevel("root.vehicle.d2.s2");
    Assert.assertEquals(1, paths.size());
  }

}
