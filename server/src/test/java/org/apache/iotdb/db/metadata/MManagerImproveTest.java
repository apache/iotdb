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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MManagerImproveTest {

  private static Logger logger = LoggerFactory.getLogger(MManagerImproveTest.class);

  private static final int TIMESERIES_NUM = 1000;
  private static final int DEVICE_NUM = 10;
  private static MManager mManager = null;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    mManager = IoTDB.metaManager;
    mManager.setStorageGroup(new PartialPath("root.t1.v2"));

    for (int j = 0; j < DEVICE_NUM; j++) {
      for (int i = 0; i < TIMESERIES_NUM; i++) {
        String p = "root.t1.v2.d" + j + ".s" + i;
        mManager.createTimeseries(
            new PartialPath(p),
            TSDataType.TEXT,
            TSEncoding.PLAIN,
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
    }
  }

  @Test
  public void checkSetUp() throws IllegalPathException {
    mManager = IoTDB.metaManager;

    assertTrue(mManager.isPathExist(new PartialPath("root.t1.v2.d3.s5")));
    assertFalse(mManager.isPathExist(new PartialPath("root.t1.v2.d9.s" + TIMESERIES_NUM)));
    assertFalse(mManager.isPathExist(new PartialPath("root.t10")));
  }

  @Test
  public void analyseTimeCost() throws MetadataException {
    mManager = IoTDB.metaManager;

    long string_combine, path_exist, list_init, check_filelevel, get_seriestype;
    string_combine = path_exist = list_init = check_filelevel = get_seriestype = 0;

    String deviceId = "root.t1.v2.d3";
    String measurement = "s5";
    String path = deviceId + TsFileConstant.PATH_SEPARATOR + measurement;

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      assertTrue(mManager.isPathExist(new PartialPath(path)));
    }
    long endTime = System.currentTimeMillis();
    path_exist += endTime - startTime;

    startTime = System.currentTimeMillis();
    endTime = System.currentTimeMillis();
    list_init += endTime - startTime;

    startTime = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      TSDataType dataType = mManager.getSeriesType(new PartialPath(path));
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
      throws MetadataException {
    for (String measurement : measurementList) {
      String path = deviceId + TsFileConstant.PATH_SEPARATOR + measurement;
      assertTrue(mManager.isPathExist(new PartialPath(path)));
      TSDataType dataType = mManager.getSeriesType(new PartialPath(path));
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  private void doPathLoopOnceTest(String deviceId, List<String> measurementList)
      throws MetadataException {
    for (String measurement : measurementList) {
      String path = deviceId + TsFileConstant.PATH_SEPARATOR + measurement;
      TSDataType dataType = mManager.getSeriesType(new PartialPath(path));
      assertEquals(TSDataType.TEXT, dataType);
    }
  }

  private void doCacheTest(String deviceId, List<String> measurementList) throws MetadataException {
    try {
      IMNode node = mManager.getDeviceNodeWithAutoCreate(new PartialPath(deviceId));
      for (String s : measurementList) {
        assertTrue(node.hasChild(s));
        IMeasurementMNode measurementNode = node.getChild(s).getAsMeasurementMNode();
        TSDataType dataType = measurementNode.getSchema().getType();
        assertEquals(TSDataType.TEXT, dataType);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  @Test
  public void improveTest() throws MetadataException {
    mManager = IoTDB.metaManager;

    String[] deviceIdList = new String[DEVICE_NUM];
    for (int i = 0; i < DEVICE_NUM; i++) {
      deviceIdList[i] = "root.t1.v2.d" + i;
    }
    List<String> measurementList = new ArrayList<>();
    for (int i = 0; i < TIMESERIES_NUM; i++) {
      measurementList.add("s" + i);
    }

    long startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doOriginTest(deviceId, measurementList);
    }
    long endTime = System.currentTimeMillis();
    logger.debug("origin:\t" + (endTime - startTime));

    startTime = System.currentTimeMillis();
    for (String deviceId : deviceIdList) {
      doPathLoopOnceTest(deviceId, measurementList);
    }
    endTime = System.currentTimeMillis();
    logger.debug("seriesPath loop once:\t" + (endTime - startTime));

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
