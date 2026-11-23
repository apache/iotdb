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

package org.apache.iotdb.db.storageengine.rescon.memory;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.DataNodeMemoryConfig;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleContext;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.TimeIndexLevel;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.db.storageengine.rescon.memory.TsFileResourceManager.REGISTRATION_RAM_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ResourceManagerTest {

  static final String RESOURCE_MANAGER_TEST_SG = "root.resourceManagerTest";
  private int seqFileNum = 10;
  private int measurementNum = 10;
  int deviceNum = 10;
  long ptNum = 100;
  long flushInterval = 20;
  TSEncoding encoding = TSEncoding.PLAIN;

  String[] deviceIds;
  MeasurementSchema[] measurementSchemas;

  List<TsFileResource> seqResources = new ArrayList<>();
  List<TsFileResource> unseqResources = new ArrayList<>();

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final DataNodeMemoryConfig MEMORY_CONFIG =
      IoTDBDescriptor.getInstance().getMemoryConfig();
  private final TsFileResourceManager tsFileResourceManager = TsFileResourceManager.getInstance();
  private long prevTimeIndexMemoryThreshold;
  private TimeIndexLevel timeIndexLevel;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    prevTimeIndexMemoryThreshold =
        MEMORY_CONFIG.getTimeIndexMemoryManager().getTotalMemorySizeInBytes();
    timeIndexLevel = CONFIG.getTimeIndexLevel();
    prepareSeries();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
    CONFIG.setTimeIndexLevel(String.valueOf(timeIndexLevel));
    tsFileResourceManager.setTimeIndexMemoryThreshold(prevTimeIndexMemoryThreshold);
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
    TsFileResourceManager.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
  }

  void prepareSeries() throws MetadataException {
    measurementSchemas = new MeasurementSchema[measurementNum];
    for (int i = 0; i < measurementNum; i++) {
      measurementSchemas[i] =
          new MeasurementSchema(
              "sensor" + i, TSDataType.DOUBLE, encoding, CompressionType.UNCOMPRESSED);
    }
    deviceIds = new String[deviceNum];
    for (int i = 0; i < deviceNum; i++) {
      deviceIds[i] = RESOURCE_MANAGER_TEST_SG + PATH_SEPARATOR + "device" + i;
    }
  }

  private void removeFiles() throws IOException {
    for (TsFileResource tsFileResource : seqResources) {
      if (tsFileResource.getTsFile().exists()) {
        tsFileResource.remove();
      }
    }
    for (TsFileResource tsFileResource : unseqResources) {
      if (tsFileResource.getTsFile().exists()) {
        tsFileResource.remove();
      }
    }
    File[] files = FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".tsfile");
    for (File file : files) {
      file.delete();
    }
    File[] resourceFiles =
        FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".resource");
    for (File resourceFile : resourceFiles) {
      resourceFile.delete();
    }
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
  }

  void prepareFile(TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    for (String deviceId : deviceIds) {
      for (IMeasurementSchema measurementSchema : measurementSchemas) {
        fileWriter.registerTimeseries(new Path(deviceId), measurementSchema);
      }
    }
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(deviceIds[j], i);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementName(),
                  String.valueOf(i + valueOffset)));
        }
        fileWriter.writeRecord(record);
        tsFileResource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(deviceIds[j]), i);
        tsFileResource.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(deviceIds[j]), i);
      }
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flush();
      }
    }
    fileWriter.close();
  }

  @Test
  public void testDegradeMethod() throws IOException, WriteProcessException {
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    tsFileResource.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource, 0, ptNum, 0);
    long previousRamSize = tsFileResource.calculateRamSize();
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
    long reducedMemory = tsFileResource.degradeTimeIndex();
    assertEquals(previousRamSize - tsFileResource.calculateRamSize(), reducedMemory);
    assertEquals(
        TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
  }

  @Test
  public void testDegradeToFileTimeIndex() throws IOException, WriteProcessException {
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    tsFileResource.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource, 0, ptNum, 0);
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
    long curTimeIndexMemoryThreshold = 0;
    tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
    assertEquals(
        TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
  }

  @Test
  public void testNotDegradeToFileTimeIndex() throws IOException, WriteProcessException {
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
    tsFileResource.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource, 0, ptNum, 0);
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
    long previousRamSize = tsFileResource.calculateRamSize();
    long curTimeIndexMemoryThreshold = registeredSize(tsFileResource);
    tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
    assertEquals(0, previousRamSize - tsFileResource.calculateRamSize());
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
  }

  @Test
  public void testTwoResourceToDegrade() throws IOException, WriteProcessException {
    File file1 =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource degradedResource1 = new TsFileResource(file1);
    degradedResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
    degradedResource1.updatePlanIndexes((long) 0);
    prepareFile(degradedResource1, 0, ptNum, 0);
    degradedResource1.degradeTimeIndex();

    TsFileResource tsFileResource1 = new TsFileResource(file1);
    tsFileResource1.setStatusForTest(TsFileResourceStatus.NORMAL);
    tsFileResource1.updatePlanIndexes((long) 0);
    prepareFile(tsFileResource1, 0, ptNum, 0);
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource1.getTimeIndexType()));

    File file2 =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource2 = new TsFileResource(file2);
    tsFileResource2.setStatusForTest(TsFileResourceStatus.NORMAL);
    tsFileResource2.updatePlanIndexes((long) 1);
    prepareFile(tsFileResource2, ptNum, ptNum, 0);
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource2.getTimeIndexType()));

    long curTimeIndexMemoryThreshold =
        registeredSize(degradedResource1) + registeredSize(tsFileResource2);
    tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource1);
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource1.getTimeIndexType()));

    tsFileResourceManager.registerSealedTsFileResource(tsFileResource2);
    assertEquals(
        TimeIndexLevel.FILE_TIME_INDEX, TimeIndexLevel.valueOf(tsFileResource1.getTimeIndexType()));
    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource2.getTimeIndexType()));
  }

  @Test
  public void testMultiDeviceTimeIndexDegrade() throws IOException, WriteProcessException {
    long curTimeIndexMemoryThreshold = 0;
    for (int i = 0; i < 8; i++) {
      File file =
          new File(
              TestConstant.BASE_OUTPUT_PATH.concat(
                  i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      tsFileResource.updatePlanIndexes((long) i);
      assertEquals(
          TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
          TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
      tsFileResource.degradeTimeIndex();
      curTimeIndexMemoryThreshold += registeredSize(tsFileResource);
    }
    for (int i = 8; i < seqFileNum; i++) {
      File file =
          new File(
              TestConstant.BASE_OUTPUT_PATH.concat(
                  i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      tsFileResource.updatePlanIndexes((long) i);
      assertEquals(
          TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
          TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
      curTimeIndexMemoryThreshold += registeredSize(tsFileResource);
    }

    for (int i = 0; i < seqFileNum; i++) {
      File file =
          new File(
              TestConstant.BASE_OUTPUT_PATH.concat(
                  i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setStatusForTest(TsFileResourceStatus.NORMAL);
      tsFileResource.updatePlanIndexes((long) i);
      assertEquals(
          TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
          TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }

    tsFileResourceManager.setTimeIndexMemoryThreshold(curTimeIndexMemoryThreshold);
    seqResources.forEach(tsFileResourceManager::registerSealedTsFileResource);

    assertEquals(10, tsFileResourceManager.getPriorityQueueSize());
    for (int i = 0; i < seqFileNum; i++) {
      // TODO: size of DeviceID may different with string device
      if (i < 8) {
        assertEquals(
            TimeIndexLevel.FILE_TIME_INDEX,
            TimeIndexLevel.valueOf(seqResources.get(i).getTimeIndexType()));
      } else {
        assertEquals(
            TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
            TimeIndexLevel.valueOf(seqResources.get(i).getTimeIndexType()));
      }
    }
  }

  @Test
  public void testAllFileTimeIndexDegrade() throws IOException, WriteProcessException {
    tsFileResourceManager.setTimeIndexMemoryThreshold(0);
    for (int i = 0; i < seqFileNum; i++) {
      File file =
          new File(
              TestConstant.BASE_OUTPUT_PATH.concat(
                  i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      assertEquals(
          TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
          TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
      tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
      assertEquals(
          TimeIndexLevel.FILE_TIME_INDEX,
          TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
    }
  }

  @Test
  public void testForceDegradeTimeIndex() {
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    IDeviceID device1 = IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.d1");
    IDeviceID device2 = IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.d2");
    tsFileResource.updateStartTime(device1, 1);
    tsFileResource.updateStartTime(device1, 2);
    tsFileResource.updateStartTime(device2, 1);
    tsFileResource.updateStartTime(device2, 2);

    assertEquals(
        TimeIndexLevel.ARRAY_DEVICE_TIME_INDEX,
        TimeIndexLevel.valueOf(tsFileResource.getTimeIndexType()));
    tsFileResourceManager.registerSealedTsFileResource(tsFileResource);
    long chargeBeforeDegrade = registeredSize(tsFileResource);
    Assert.assertEquals(chargeBeforeDegrade, tsFileResourceManager.getTotalTimeIndexMemCost());
    TsFileResourceManager.getInstance().forceDegradeTsFileResource(tsFileResource);
    Assert.assertEquals(
        registeredSize(tsFileResource), tsFileResourceManager.getTotalTimeIndexMemCost());
    Assert.assertTrue(chargeBeforeDegrade > tsFileResourceManager.getTotalTimeIndexMemCost());
    Assert.assertTrue(tsFileResource.getTimeIndexType() == ITimeIndex.FILE_TIME_INDEX_TYPE);
    Assert.assertEquals(1, tsFileResourceManager.getDegradedTimeIndexNum());
  }

  @Test
  public void testResidentMetadataAndIndexSizeChanges() throws IOException {
    File file = new File("target/0-1-0-0.tsfile");
    TsFileResource resource = new TsFileResource(file);
    long before = resource.calculateRamSize();
    IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.newDevice");
    resource.updateStartTime(device, 1);
    assertTrue(resource.calculateRamSize() > before);

    resource.close();
    long sealed = resource.calculateRamSize();
    long indexSize = resource.getTimeIndex().calculateRamSize();
    assertEquals(Long.valueOf(indexSize), resource.getDeviceTimeIndexRamSize().get());
    long expectedRelease = indexSize - new FileTimeIndex().calculateRamSize();
    // A freshly sealed resource can also be degraded before any size calculation.
    TsFileResource unmeasured = new TsFileResource(file);
    unmeasured.updateStartTime(device, 1);
    unmeasured.close();
    assertEquals(expectedRelease, unmeasured.degradeTimeIndex());
    assertEquals(expectedRelease, resource.degradeTimeIndex());
    assertEquals(sealed - expectedRelease, resource.calculateRamSize());
    assertEquals(0, resource.degradeTimeIndex());
    Assert.assertFalse(resource.getDeviceTimeIndexRamSize().isPresent());

    long base = resource.calculateRamSize();
    File longerPath = new File("target/longer/directory/0-1-0-0.tsfile");
    resource.setFile(longerPath);
    assertEquals(
        base + MemUtils.getFileRamBytes(longerPath) - MemUtils.getFileRamBytes(file),
        resource.calculateRamSize());
    long beforeMods = resource.calculateRamSize();
    long modSize = resource.getExclusiveModFile().ramBytesUsed();
    assertEquals(beforeMods + modSize, resource.calculateRamSize());
    resource.resetModFile();
    assertEquals(beforeMods, resource.calculateRamSize());
  }

  @Test
  public void testRegisteredBytesFollowMetadataAndIndexChanges() throws Exception {
    tsFileResourceManager.setTimeIndexMemoryThreshold(Long.MAX_VALUE);
    TsFileResource remaining = new TsFileResource(new File("target/0-1-0-0.tsfile"));
    remaining.close();
    tsFileResourceManager.registerSealedTsFileResource(remaining);
    long baseline = tsFileResourceManager.getTotalTimeIndexMemCost();
    TsFileResource resource = new TsFileResource(new File("target/0-2-0-0.tsfile"));
    resource.close();
    resource.setTimeIndex(new ArrayDeviceTimeIndex());
    tsFileResourceManager.registerSealedTsFileResource(resource);
    tsFileResourceManager.registerSealedTsFileResource(resource);
    long originalCharge = registeredSize(resource);
    assertEquals(baseline + originalCharge, tsFileResourceManager.getTotalTimeIndexMemCost());
    resource.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.later"), 7);
    resource.setProgressIndex(new SimpleProgressIndex(1, 1));
    resource.getExclusiveModFile();
    long changedCharge = registeredSize(resource);
    assertTrue(changedCharge > originalCharge);
    assertEquals(baseline + changedCharge, tsFileResourceManager.getTotalTimeIndexMemCost());

    resource.degradeTimeIndex();
    assertEquals(1, tsFileResourceManager.getDegradedTimeIndexNum());
    assertEquals(
        baseline + registeredSize(resource), tsFileResourceManager.getTotalTimeIndexMemCost());
    TsFileResource clone = resource.shallowCloneForNative();
    Assert.assertNull(clone.getMemoryRegistration());
    clone.getCompactionModFile();
    assertEquals(
        baseline + registeredSize(resource), tsFileResourceManager.getTotalTimeIndexMemCost());
    resource.setTimeIndex(new ArrayDeviceTimeIndex());
    assertEquals(0, tsFileResourceManager.getDegradedTimeIndexNum());
    assertEquals(
        baseline + registeredSize(resource), tsFileResourceManager.getTotalTimeIndexMemCost());
    tsFileResourceManager.removeTsFileResource(resource);
    tsFileResourceManager.removeTsFileResource(resource);
    assertEquals(baseline, tsFileResourceManager.getTotalTimeIndexMemCost());
    assertEquals(0, tsFileResourceManager.getDegradedTimeIndexNum());
    tsFileResourceManager.removeTsFileResource(remaining);
    assertEquals(0, tsFileResourceManager.getTotalTimeIndexMemCost());
  }

  @Test
  public void testLoadLastValuesExcludedFromResourceBudget() throws IOException {
    TsFileResource resource = spy(new TsFileResource(new File("target/0-3-0-0.tsfile")));
    resource.setTimeIndex(new ArrayDeviceTimeIndex());
    resource.close();
    long budget = registeredSize(resource);
    tsFileResourceManager.setTimeIndexMemoryThreshold(budget);

    resource.setLastValues(
        Collections.singletonMap(
            IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.d"),
            Collections.singletonList(
                new Pair<>("s1", new TimeValuePair(1, new TsPrimitiveType.TsInt(42))))));
    assertEquals(budget, registeredSize(resource));
    tsFileResourceManager.registerSealedTsFileResource(resource);
    assertEquals(budget, tsFileResourceManager.getTotalTimeIndexMemCost());
    assertEquals(0, tsFileResourceManager.getDegradedTimeIndexNum());

    clearInvocations(resource);
    resource.setLastValues(null);
    verify(resource, never()).calculateMetadataRamSize();
    assertEquals(budget, tsFileResourceManager.getTotalTimeIndexMemCost());
    tsFileResourceManager.removeTsFileResource(resource);
    assertEquals(0, tsFileResourceManager.getTotalTimeIndexMemCost());
  }

  private static long registeredSize(TsFileResource resource) {
    File file = resource.getTsFile();
    return resource.calculateRamSize()
        + REGISTRATION_RAM_BYTES
        + (file.isAbsolute() ? 0 : RamUsageEstimator.sizeOf(file.getAbsolutePath()));
  }

  @Test
  public void testRecoveryUsesFileIdentityAcrossIndexLevelsAndRenames() throws IOException {
    tsFileResourceManager.setTimeIndexMemoryThreshold(Long.MAX_VALUE);
    File file = new File("target/0-11-0-0.tsfile");
    TsFileResource original = new TsFileResource(file);
    original.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.d1"), 7);
    original.close();
    tsFileResourceManager.registerSealedTsFileResource(original);
    TsFileResource recovered = new TsFileResource(file.getAbsoluteFile());
    long charge = tsFileResourceManager.getTotalTimeIndexMemCost();
    tsFileResourceManager.registerSealedTsFileResource(recovered);
    assertEquals(1, tsFileResourceManager.getPriorityQueueSize());
    assertEquals(charge, tsFileResourceManager.getTotalTimeIndexMemCost());

    tsFileResourceManager.forceDegradeTsFileResource(original);
    charge = tsFileResourceManager.getTotalTimeIndexMemCost();
    tsFileResourceManager.registerSealedTsFileResource(recovered);
    assertEquals(charge, tsFileResourceManager.getTotalTimeIndexMemCost());
    File newFile = new File("target/0-12-0-0.tsfile").getAbsoluteFile();
    original.setFile(newFile);
    assertEquals(registeredSize(original), tsFileResourceManager.getTotalTimeIndexMemCost());
    tsFileResourceManager.registerSealedTsFileResource(recovered);
    assertEquals(2, tsFileResourceManager.getPriorityQueueSize());
    // Moving onto an already registered path leaves one charge for the existing owner.
    recovered.setFile(newFile);
    Assert.assertNull(recovered.getMemoryRegistration());
    assertEquals(registeredSize(original), tsFileResourceManager.getTotalTimeIndexMemCost());
    tsFileResourceManager.removeTsFileResource(new TsFileResource(file));
    assertEquals(1, tsFileResourceManager.getPriorityQueueSize());
    tsFileResourceManager.removeTsFileResource(new TsFileResource(newFile));
    assertEquals(0, tsFileResourceManager.getPriorityQueueSize());
    assertEquals(0, tsFileResourceManager.getDegradedTimeIndexNum());
    assertEquals(0, tsFileResourceManager.getTotalTimeIndexMemCost());
    Assert.assertNull(original.getMemoryRegistration());
  }

  @Test
  public void testMetadataRefreshObservesPendingIndexSwap() throws Exception {
    tsFileResourceManager.setTimeIndexMemoryThreshold(Long.MAX_VALUE);
    TsFileResource first = new TsFileResource(new File("target/0-21-0-0.tsfile"));
    TsFileResource second = new TsFileResource(new File("target/0-22-0-0.tsfile"));
    for (int i = 0; i < 16; i++) {
      first.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.d" + i), 1);
    }
    second.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.other"), 2);
    first.close();
    second.close();
    tsFileResourceManager.registerSealedTsFileResource(first);
    tsFileResourceManager.registerSealedTsFileResource(second);
    tsFileResourceManager.setTimeIndexMemoryThreshold(
        tsFileResourceManager.getTotalTimeIndexMemCost());
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<Long> degradation;
      synchronized (tsFileResourceManager) {
        // Hold the manager monitor between the index CAS and its accounting callback.
        degradation = pool.submit(first::degradeTimeIndex);
        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .until(() -> first.getTimeIndex() instanceof FileTimeIndex);
        first.getExclusiveModFile();
        assertTrue(second.getTimeIndex() instanceof ArrayDeviceTimeIndex);
        assertEquals(1, tsFileResourceManager.getDegradedTimeIndexNum());
      }
      assertTrue(degradation.get(10, TimeUnit.SECONDS) > 0);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testSelectionUsesMeasuredSizeOnlyForTheSameIndex() throws IOException {
    tsFileResourceManager.setTimeIndexMemoryThreshold(Long.MAX_VALUE);
    TsFileResource resource = new TsFileResource(new File("target/0-31-0-0.tsfile"));
    ArrayDeviceTimeIndex resident = spy(new ArrayDeviceTimeIndex());
    resource.setTimeIndex(resident);
    resource.close();
    tsFileResourceManager.registerSealedTsFileResource(resource);
    CompactionScheduleContext context = new CompactionScheduleContext();
    ArrayDeviceTimeIndex rebuilt = spy(new ArrayDeviceTimeIndex());
    try {
      assertTrue(resource.getDeviceTimeIndexRamSize().isPresent());
      context.addResourceDeviceTimeIndex(resource, resident);
      context.clearTimePartitionDeviceInfoCache();
      context.addResourceDeviceTimeIndex(resource, resident);
      tsFileResourceManager.forceDegradeTsFileResource(resource);
      context.addResourceDeviceTimeIndex(resource, rebuilt);
      context.addResourceDeviceTimeIndex(resource, rebuilt);
      verify(resident, times(1)).calculateRamSize();
      verify(rebuilt, times(1)).calculateRamSize();
      Assert.assertSame(rebuilt, context.getResourceDeviceInfo(resource));
      tsFileResourceManager.removeTsFileResource(resource);
      context.clearTimePartitionDeviceInfoCache();
      context.addResourceDeviceTimeIndex(resource, resident);
      verify(resident, times(2)).calculateRamSize();
    } finally {
      context.clearTimePartitionDeviceInfoCache();
    }
  }

  @Test
  public void testNullTimeIndex() {
    TsFileResource empty = new TsFileResource();
    Assert.assertNull(empty.replaceDeviceTimeIndexWithFileTimeIndex());
    assertEquals(0, empty.degradeTimeIndex());

    TsFileResource registered = new TsFileResource(new File("target/0-41-0-0.tsfile"));
    registered.setTimeIndex(new FileTimeIndex());
    tsFileResourceManager.registerSealedTsFileResource(registered);
    registered.setTimeIndex(null);
    assertEquals(0, registered.degradeTimeIndex());
    assertEquals(0, tsFileResourceManager.getDegradedTimeIndexNum());
    assertEquals(registeredSize(registered), tsFileResourceManager.getTotalTimeIndexMemCost());
  }
}
