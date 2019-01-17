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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MManagerImproveTest {

  private static final int TIMESERIES_NUM = 1000;
  private static final int DEVICE_NUM = 10;
  private static MManager mManager = null;

  @Before
  public void setUp() throws Exception {
    mManager = MManager.getInstance();
    mManager.setStorageLevelToMTree("root.t1.v2");

    for (int j = 0; j < DEVICE_NUM; j++) {
      for (int i = 0; i < TIMESERIES_NUM; i++) {
        String p = new StringBuilder().append("root.t1.v2.d").append(j).append(".s").append(i)
            .toString();
        mManager.addPathToMTree(p, "TEXT", "RLE", new String[0]);
      }
    }

    mManager.flushObjectToFile();
  }

  @After
  public void after() throws IOException, FileNodeManagerException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void checkSetUp() {
    mManager = MManager.getInstance();

    assertEquals(true, mManager.pathExist("root.t1.v2.d3.s5"));
    assertEquals(false, mManager.pathExist("root.t1.v2.d9.s" + TIMESERIES_NUM));
    assertEquals(false, mManager.pathExist("root.t10"));
  }

  @Test
  public void analyseTimeCost() throws PathErrorException, ProcessorException {
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
      assertEquals(true, mManager.pathExist(path));
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
      assertEquals(true, mManager.checkFileLevel(paths));
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

    System.out.println("string combine:\t" + string_combine);
    System.out.println("seriesPath exist:\t" + path_exist);
    System.out.println("list init:\t" + list_init);
    System.out.println("check file level:\t" + check_filelevel);
    System.out.println("get series type:\t" + get_seriestype);
  }

  public void doOriginTest(String deviceId, List<String> measurementList)
      throws PathErrorException, ProcessorException {
    for (String measurement : measurementList) {
      String path = deviceId + "." + measurement;
      assertEquals(true, mManager.pathExist(path));
      List<Path> paths = new ArrayList<>();
      paths.add(new Path(path));
      assertEquals(true, mManager.checkFileLevel(paths));
      TSDataType dataType = mManager.getSeriesType(path);
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  public void doPathLoopOnceTest(String deviceId, List<String> measurementList)
      throws PathErrorException, ProcessorException {
    for (String measurement : measurementList) {
      String path = deviceId + "." + measurement;
      List<Path> paths = new ArrayList<>();
      paths.add(new Path(path));
      assertEquals(true, mManager.checkFileLevel(paths));
      TSDataType dataType = mManager.getSeriesTypeWithCheck(path);
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  public void doDealdeviceIdOnceTest(String deviceId, List<String> measurementList)
      throws PathErrorException, ProcessorException {
    boolean isFileLevelChecked;
    List<Path> tempList = new ArrayList<>();
    tempList.add(new Path(deviceId));
    try {
      isFileLevelChecked = mManager.checkFileLevel(tempList);
    } catch (PathErrorException e) {
      isFileLevelChecked = false;
    }
    MNode node = mManager.getNodeByPath(deviceId);

    for (String measurement : measurementList) {
      assertEquals(true, mManager.pathExist(node, measurement));
      List<Path> paths = new ArrayList<>();
      paths.add(new Path(measurement));
      if (!isFileLevelChecked) {
        isFileLevelChecked = mManager.checkFileLevel(node, paths);
      }
      assertEquals(true, isFileLevelChecked);
      TSDataType dataType = mManager.getSeriesType(node, measurement);
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  public void doRemoveListTest(String deviceId, List<String> measurementList)
      throws PathErrorException, ProcessorException {
    for (String measurement : measurementList) {
      String path = deviceId + "." + measurement;
      assertEquals(true, mManager.pathExist(path));
      assertEquals(true, mManager.checkFileLevel(path));
      TSDataType dataType = mManager.getSeriesType(path);
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  public void doAllImproveTest(String deviceId, List<String> measurementList)
      throws PathErrorException, ProcessorException {
    boolean isFileLevelChecked;
    try {
      isFileLevelChecked = mManager.checkFileLevel(deviceId);
    } catch (PathErrorException e) {
      isFileLevelChecked = false;
    }
    MNode node = mManager.getNodeByPathWithCheck(deviceId);

    for (String measurement : measurementList) {
      if (!isFileLevelChecked) {
        isFileLevelChecked = mManager.checkFileLevelWithCheck(node, measurement);
      }
      assertEquals(true, isFileLevelChecked);
      TSDataType dataType = mManager.getSeriesTypeWithCheck(node, measurement);
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  public void doCacheTest(String deviceId, List<String> measurementList)
      throws PathErrorException, ProcessorException {
    MNode node = mManager.getNodeByDeviceIdFromCache(deviceId);
    for (int i = 0; i < measurementList.size(); i++) {
      assertEquals(true, node.hasChild(measurementList.get(i)));
      MNode measurementNode = node.getChild(measurementList.get(i));
      assertEquals(true, measurementNode.isLeaf());
      TSDataType dataType = measurementNode.getSchema().dataType;
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  @Test
  public void improveTest() throws PathErrorException, ProcessorException {
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
    System.out.println("origin:\t" + (endTime - startTime));

    startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doPathLoopOnceTest(deviceId, measurementList);
    }
    endTime = System.currentTimeMillis();
    System.out.println("seriesPath loop once:\t" + (endTime - startTime));

    startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doDealdeviceIdOnceTest(deviceId, measurementList);
    }
    endTime = System.currentTimeMillis();
    System.out.println("deal deviceId once:\t" + (endTime - startTime));

    startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doRemoveListTest(deviceId, measurementList);
    }
    endTime = System.currentTimeMillis();
    System.out.println("remove list:\t" + (endTime - startTime));

    startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doAllImproveTest(deviceId, measurementList);
    }
    endTime = System.currentTimeMillis();
    System.out.println("improve all:\t" + (endTime - startTime));

    startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doCacheTest(deviceId, measurementList);
    }
    endTime = System.currentTimeMillis();
    System.out.println("add cache:\t" + (endTime - startTime));
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

}
