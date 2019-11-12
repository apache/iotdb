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
package org.apache.iotdb.db.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.storageGroup.StorageGroupException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MManagerImproveTest {
  private static Logger logger = LoggerFactory.getLogger(MManagerImproveTest.class);

  private static final int TIMESERIES_NUM = 1000;
  private static final int DEVICE_NUM = 10;
  private static MManager mManager = null;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    mManager = MManager.getInstance();
    mManager.setStorageGroupToMTree("root.t1.v2");

    for (int j = 0; j < DEVICE_NUM; j++) {
      for (int i = 0; i < TIMESERIES_NUM; i++) {
        String p = "root.t1.v2.d" + j + ".s" + i;
        mManager.addPathToMTree(p, "TEXT", "RLE");
      }
    }

  }

  @After
  public void after() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void checkSetUp() {
    mManager = MManager.getInstance();

    assertTrue(mManager.pathExist("root.t1.v2.d3.s5"));
    assertFalse(mManager.pathExist("root.t1.v2.d9.s" + TIMESERIES_NUM));
    assertFalse(mManager.pathExist("root.t10"));
  }

  @Test
  public void analyseTimeCost() throws PathException, StorageGroupException {
    mManager = MManager.getInstance();

    long startTime, endTime;
    long string_combine, path_exist, list_init, check_filelevel, get_seriestype;
    string_combine = path_exist = list_init = check_filelevel = get_seriestype = 0;

    String deviceId = "root.t1.v2.d3";
    String measurement = "s5";

    startTime = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      String path = deviceId + "." + measurement;
    }
    endTime = System.currentTimeMillis();
    string_combine += endTime - startTime;
    String path = deviceId + "." + measurement;

    startTime = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      assertTrue(mManager.pathExist(path));
    }
    endTime = System.currentTimeMillis();
    path_exist += endTime - startTime;

    startTime = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      List<Path> paths = new ArrayList<>();
      paths.add(new Path(path));
    }
    endTime = System.currentTimeMillis();
    list_init += endTime - startTime;
    List<Path> paths = new ArrayList<>();
    paths.add(new Path(path));

    startTime = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      assertTrue(mManager.checkFileLevel(paths));
    }
    endTime = System.currentTimeMillis();
    check_filelevel += endTime - startTime;

    startTime = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      TSDataType dataType = mManager.getSeriesType(path);
      assertEquals(TSDataType.TEXT, dataType);
    }
    endTime = System.currentTimeMillis();
    get_seriestype += endTime - startTime;

    logger.debug("string combine:\t" + string_combine);
    logger.debug("seriesPath exist:\t" + path_exist);
    logger.debug("list init:\t" + list_init);
    logger.debug("check file level:\t" + check_filelevel);
    logger.debug("get series type:\t" + get_seriestype);
  }

  private void doOriginTest(String deviceId, List<String> measurementList)
      throws PathException, StorageGroupException {
    for (String measurement : measurementList) {
      String path = deviceId + "." + measurement;
      assertTrue(mManager.pathExist(path));
      List<Path> paths = new ArrayList<>();
      paths.add(new Path(path));
      assertTrue(mManager.checkFileLevel(paths));
      TSDataType dataType = mManager.getSeriesType(path);
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  private void doPathLoopOnceTest(String deviceId, List<String> measurementList)
      throws PathException, StorageGroupException {
    for (String measurement : measurementList) {
      String path = deviceId + "." + measurement;
      List<Path> paths = new ArrayList<>();
      paths.add(new Path(path));
      assertTrue(mManager.checkFileLevel(paths));
      TSDataType dataType = mManager.getSeriesTypeWithCheck(path);
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  private void doDealdeviceIdOnceTest(String deviceId, List<String> measurementList)
      throws PathException, StorageGroupException {
    boolean isFileLevelChecked;
    List<Path> tempList = new ArrayList<>();
    tempList.add(new Path(deviceId));
    try {
      isFileLevelChecked = mManager.checkFileLevel(tempList);
    } catch (StorageGroupException e) {
      isFileLevelChecked = false;
    }
    MNode node = mManager.getNodeByPath(deviceId);

    for (String measurement : measurementList) {
      assertTrue(mManager.pathExist(node, measurement));
      List<Path> paths = new ArrayList<>();
      paths.add(new Path(measurement));
      if (!isFileLevelChecked) {
        isFileLevelChecked = mManager.checkFileLevel(node, paths);
      }
      assertTrue(isFileLevelChecked);
      TSDataType dataType = mManager.getSeriesType(node, measurement);
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  private void doRemoveListTest(String deviceId, List<String> measurementList)
      throws PathException, StorageGroupException {
    for (String measurement : measurementList) {
      String path = deviceId + "." + measurement;
      assertTrue(mManager.pathExist(path));
      assertTrue(mManager.checkFileLevel(path));
      TSDataType dataType = mManager.getSeriesType(path);
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  private void doAllImproveTest(String deviceId, List<String> measurementList)
      throws PathException, StorageGroupException {
    boolean isFileLevelChecked;
    try {
      isFileLevelChecked = mManager.checkFileLevel(deviceId);
    } catch (StorageGroupException e) {
      isFileLevelChecked = false;
    }
    MNode node = mManager.getNodeByPathWithCheck(deviceId);

    for (String measurement : measurementList) {
      if (!isFileLevelChecked) {
        isFileLevelChecked = mManager.checkFileLevelWithCheck(node, measurement);
      }
      assertTrue(isFileLevelChecked);
      TSDataType dataType = mManager.getSeriesTypeWithCheck(node, measurement);
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  private void doCacheTest(String deviceId, List<String> measurementList)
      throws CacheException, PathException {
    MNode node = mManager.getNodeByDeviceIdFromCache(deviceId);
    for (String s : measurementList) {
      assertTrue(node.hasChild(s));
      MNode measurementNode = node.getChild(s);
      assertTrue(measurementNode.isLeaf());
      TSDataType dataType = measurementNode.getSchema().getType();
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  @Test
  public void improveTest() throws PathException, StorageGroupException, CacheException {
    mManager = MManager.getInstance();

    long startTime, endTime;
    String[] deviceIdList = new String[DEVICE_NUM];
    for (int i = 0; i < DEVICE_NUM; i++) {
      deviceIdList[i] = "root.t1.v2.d" + i;
    }
    List<String> measurementList = new ArrayList<>();
    for (int i = 0; i < TIMESERIES_NUM; i++) {
      measurementList.add("s" + i);
    }

    startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doOriginTest(deviceId, measurementList);
    }
    endTime = System.currentTimeMillis();
    logger.debug("origin:\t" + (endTime - startTime));

    startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doPathLoopOnceTest(deviceId, measurementList);
    }
    endTime = System.currentTimeMillis();
    logger.debug("seriesPath loop once:\t" + (endTime - startTime));

    startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doDealdeviceIdOnceTest(deviceId, measurementList);
    }
    endTime = System.currentTimeMillis();
    logger.debug("deal deviceId once:\t" + (endTime - startTime));

    startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doRemoveListTest(deviceId, measurementList);
    }
    endTime = System.currentTimeMillis();
    logger.debug("remove list:\t" + (endTime - startTime));

    startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doAllImproveTest(deviceId, measurementList);
    }
    endTime = System.currentTimeMillis();
    logger.debug("improve all:\t" + (endTime - startTime));

    startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doCacheTest(deviceId, measurementList);
    }
    endTime = System.currentTimeMillis();
    logger.debug("add cache:\t" + (endTime - startTime));
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

}
