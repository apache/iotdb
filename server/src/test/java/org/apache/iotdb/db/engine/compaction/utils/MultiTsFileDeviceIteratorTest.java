/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction.utils;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.engine.compaction.execute.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

public class MultiTsFileDeviceIteratorTest extends AbstractCompactionTest {

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(1024);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    for (TsFileResource tsFileResource : seqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
  }

  @Test
  public void getNonAlignedDevicesFromDifferentFilesWithFourLayersInNodeTreeTest()
      throws MetadataException, IOException, WriteProcessException {
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(3);
    registerTimeseriesInMManger(30, 3, false);
    createFiles(3, 10, 3, 100, 0, 0, 50, 50, false, true);
    createFiles(4, 5, 3, 100, 1000, 0, 50, 50, false, true);
    createFiles(2, 15, 3, 100, 1000, 0, 50, 50, false, false);
    createFiles(3, 30, 3, 100, 1000, 0, 50, 50, false, false);

    // sort the deviceId in lexicographical order from small to large
    List<String> deviceIds = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      deviceIds.add("root.testsg.d" + i);
    }
    deviceIds.sort(String::compareTo);

    int deviceNum = 0;
    try (MultiTsFileDeviceIterator multiTsFileDeviceIterator =
        new MultiTsFileDeviceIterator(seqResources, unseqResources)) {
      while (multiTsFileDeviceIterator.hasNextDevice()) {
        Pair<String, Boolean> deviceInfo = multiTsFileDeviceIterator.nextDevice();
        Assert.assertEquals(deviceIds.get(deviceNum), deviceInfo.left);
        Assert.assertFalse(deviceInfo.right);
        deviceNum++;
      }
    }
    Assert.assertEquals(30, deviceNum);
  }

  @Test
  public void getAlignedDevicesFromDifferentFilesWithOneLayerInNodeTreeTest()
      throws MetadataException, IOException, WriteProcessException {
    registerTimeseriesInMManger(30, 3, false);
    createFiles(3, 10, 3, 100, 0, 0, 50, 50, true, true);
    createFiles(4, 5, 3, 100, 1000, 0, 50, 50, true, true);
    createFiles(2, 15, 3, 100, 1000, 0, 50, 50, true, false);
    createFiles(3, 30, 3, 100, 1000, 0, 50, 50, true, false);

    // sort the deviceId in lexicographical order from small to large
    List<String> deviceIds = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      deviceIds.add("root.testsg.d" + (i + TsFileGeneratorUtils.getAlignDeviceOffset()));
    }
    deviceIds.sort(String::compareTo);

    int deviceNum = 0;
    try (MultiTsFileDeviceIterator multiTsFileDeviceIterator =
        new MultiTsFileDeviceIterator(seqResources, unseqResources)) {
      while (multiTsFileDeviceIterator.hasNextDevice()) {
        Pair<String, Boolean> deviceInfo = multiTsFileDeviceIterator.nextDevice();
        Assert.assertEquals(deviceIds.get(deviceNum), deviceInfo.left);
        Assert.assertTrue(deviceInfo.right);
        deviceNum++;
      }
    }
    Assert.assertEquals(30, deviceNum);
  }

  @Test
  public void getNonAlignedDevicesFromDifferentFilesWithFourLayersInNodeTreeTestUsingFileTimeIndex()
      throws MetadataException, IOException, WriteProcessException {
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(3);
    registerTimeseriesInMManger(30, 3, false);
    createFiles(3, 10, 3, 100, 0, 0, 50, 50, false, true);
    createFiles(4, 5, 3, 100, 1000, 0, 50, 50, false, true);
    createFiles(2, 15, 3, 100, 1000, 0, 50, 50, false, false);
    createFiles(3, 30, 3, 100, 1000, 0, 50, 50, false, false);

    // use file time index
    for (TsFileResource resource : seqResources) {
      resource.degradeTimeIndex();
    }
    for (TsFileResource resource : unseqResources) {
      resource.degradeTimeIndex();
    }

    // sort the deviceId in lexicographical order from small to large
    List<String> deviceIds = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      deviceIds.add("root.testsg.d" + i);
    }
    deviceIds.sort(String::compareTo);

    int deviceNum = 0;
    try (MultiTsFileDeviceIterator multiTsFileDeviceIterator =
        new MultiTsFileDeviceIterator(seqResources, unseqResources)) {
      while (multiTsFileDeviceIterator.hasNextDevice()) {
        Pair<String, Boolean> deviceInfo = multiTsFileDeviceIterator.nextDevice();
        Assert.assertEquals(deviceIds.get(deviceNum), deviceInfo.left);
        Assert.assertFalse(deviceInfo.right);
        deviceNum++;
      }
    }
    Assert.assertEquals(30, deviceNum);
  }

  @Test
  public void getAlignedDevicesFromDifferentFilesWithOneLayerInNodeTreeTestUsingFileTimeIndex()
      throws MetadataException, IOException, WriteProcessException {
    registerTimeseriesInMManger(30, 3, false);
    createFiles(3, 10, 3, 100, 0, 0, 50, 50, true, true);
    createFiles(4, 5, 3, 100, 1000, 0, 50, 50, true, true);
    createFiles(2, 15, 3, 100, 1000, 0, 50, 50, true, false);
    createFiles(3, 30, 3, 100, 1000, 0, 50, 50, true, false);

    // use file time index
    for (TsFileResource resource : seqResources) {
      resource.degradeTimeIndex();
    }
    for (TsFileResource resource : unseqResources) {
      resource.degradeTimeIndex();
    }

    // sort the deviceId in lexicographical order from small to large
    List<String> deviceIds = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      deviceIds.add("root.testsg.d" + (i + TsFileGeneratorUtils.getAlignDeviceOffset()));
    }
    deviceIds.sort(String::compareTo);

    int deviceNum = 0;
    try (MultiTsFileDeviceIterator multiTsFileDeviceIterator =
        new MultiTsFileDeviceIterator(seqResources, unseqResources)) {
      while (multiTsFileDeviceIterator.hasNextDevice()) {
        Pair<String, Boolean> deviceInfo = multiTsFileDeviceIterator.nextDevice();
        Assert.assertEquals(deviceIds.get(deviceNum), deviceInfo.left);
        Assert.assertTrue(deviceInfo.right);
        deviceNum++;
      }
    }
    Assert.assertEquals(30, deviceNum);
  }

  /**
   * Create device with nonAligned property. Deleted it and create new device with same deviceID but
   * aligned property. Check whether the deviceID and its property can be obtained correctly.
   */
  @Test
  public void getDeletedDevicesWithSameNameFromDifferentFilesWithFourLayersInNodeTreeTest()
      throws MetadataException, IOException, WriteProcessException {
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(3);
    int oldAlignedDeviceOffset = TsFileGeneratorUtils.alignDeviceOffset;
    TsFileGeneratorUtils.alignDeviceOffset = 0;
    // create nonAligned device
    registerTimeseriesInMManger(30, 5, false);
    createFiles(3, 10, 5, 100, 0, 0, 50, 50, false, true);
    createFiles(4, 5, 5, 100, 1000, 0, 50, 50, false, true);

    // generate mods file, delete devices
    List<String> seriesPaths = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      for (int j = 0; j < 5; j++) {
        seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j);
      }
    }
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    deleteTimeseriesInMManager(seriesPaths);

    // create aligned device with the same deviceID
    createFiles(2, 7, 5, 100, 1600, 1600, 50, 50, true, true);
    createFiles(3, 30, 3, 100, 1000, 0, 50, 50, true, false);

    // sort the deviceId in lexicographical order from small to large
    List<String> deviceIds = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      deviceIds.add("root.testsg.d" + (i + TsFileGeneratorUtils.getAlignDeviceOffset()));
    }
    deviceIds.sort(String::compareTo);

    int deviceNum = 0;
    try (MultiTsFileDeviceIterator multiTsFileDeviceIterator =
        new MultiTsFileDeviceIterator(seqResources, unseqResources)) {
      while (multiTsFileDeviceIterator.hasNextDevice()) {
        Pair<String, Boolean> deviceInfo = multiTsFileDeviceIterator.nextDevice();
        Assert.assertEquals(deviceIds.get(deviceNum), deviceInfo.left);
        Assert.assertTrue(deviceInfo.right);
        deviceNum++;
      }
    }
    Assert.assertEquals(30, deviceNum);
    TsFileGeneratorUtils.alignDeviceOffset = oldAlignedDeviceOffset;
  }

  /**
   * Create device with nonAligned property. Deleted it and create new device with same deviceID but
   * aligned property. Check whether the deviceID and its property can be obtained correctly.
   */
  @Test
  public void getDeletedDevicesWithSameNameFromSeqFilesWithFourLayersInNodeTreeTest()
      throws MetadataException, IOException, WriteProcessException {
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(3);
    int oldAlignedDeviceOffset = TsFileGeneratorUtils.alignDeviceOffset;
    TsFileGeneratorUtils.alignDeviceOffset = 0;
    registerTimeseriesInMManger(30, 5, false);
    createFiles(3, 10, 5, 100, 0, 0, 50, 50, false, true);
    createFiles(4, 30, 5, 100, 1000, 0, 50, 50, false, true);

    // generate mods file, delete d0 ~ d14
    List<String> seriesPaths = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      for (int j = 0; j < 5; j++) {
        seriesPaths.add(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i + PATH_SEPARATOR + "s" + j);
      }
    }
    generateModsFile(seriesPaths, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(seriesPaths, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    deleteTimeseriesInMManager(seriesPaths);

    createFiles(2, 10, 5, 100, 2000, 2000, 50, 50, true, true);

    // sort the deviceId in lexicographical order from small to large
    List<String> deviceIds = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      deviceIds.add("root.testsg.d" + (i + TsFileGeneratorUtils.getAlignDeviceOffset()));
    }
    deviceIds.sort(String::compareTo);

    int deviceNum = 0;
    try (MultiTsFileDeviceIterator multiTsFileDeviceIterator =
        new MultiTsFileDeviceIterator(seqResources)) {
      while (multiTsFileDeviceIterator.hasNextDevice()) {
        Pair<String, Boolean> deviceInfo = multiTsFileDeviceIterator.nextDevice();
        Assert.assertEquals(deviceIds.get(deviceNum), deviceInfo.left);
        if (Integer.parseInt(deviceInfo.left.substring(13)) < 10) {
          Assert.assertTrue(deviceInfo.right);
        } else {
          Assert.assertFalse(deviceInfo.right);
        }
        deviceNum++;
      }
    }
    Assert.assertEquals(30, deviceNum);
    TsFileGeneratorUtils.alignDeviceOffset = oldAlignedDeviceOffset;
  }
}
