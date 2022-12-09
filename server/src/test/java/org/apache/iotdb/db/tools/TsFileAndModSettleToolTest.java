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
package org.apache.iotdb.db.tools;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class TsFileAndModSettleToolTest {
  private final long newPartitionInterval = 3600_000;
  protected final long maxTimestamp = 50000L; // 100000000L;
  protected final String folder = "target" + File.separator + "settle";
  protected final String STORAGE_GROUP = "root.sg_0";
  protected final String DEVICE1 = STORAGE_GROUP + ".device_1";
  protected final String DEVICE2 = STORAGE_GROUP + ".device_2";
  protected final String SENSOR1 = "sensor_1";
  protected final String SENSOR2 = "sensor_2";
  private final long VALUE_OFFSET = 1;
  private final Planner processor = new Planner();
  private String path = null;
  private IoTDBConfig config;
  private long originPartitionInterval;

  @Before
  public void setUp() {
    config = IoTDBDescriptor.getInstance().getConfig();
    originPartitionInterval = config.getTimePartitionInterval();
    config.setTimePartitionInterval(newPartitionInterval);
    EnvironmentUtils.envSetUp();

    File f = new File(folder);
    if (!f.exists()) {
      boolean success = f.mkdir();
      Assert.assertTrue(success);
    }
    path = folder + File.separator + System.currentTimeMillis() + "-" + 0 + "-0.tsfile";
  }

  @After
  public void tearDown() {
    File[] fileLists = FSFactoryProducer.getFSFactory().listFilesBySuffix(folder, TSFILE_SUFFIX);
    for (File f : fileLists) {
      if (f.exists()) {
        boolean deleteSuccess = f.delete();
        Assert.assertTrue(deleteSuccess);
      }
    }

    File directory = new File(folder);
    FileUtils.deleteDirectory(directory);
    try {
      EnvironmentUtils.cleanEnv();
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      config.setTimePartitionInterval(originPartitionInterval);
    }
  }

  @Test
  public void settleTsFilesAndModsTest() { // offline settleTool test
    try {
      List<TsFileResource> resourcesToBeSettled = createFiles();
      List<TsFileResource> settledResources = new ArrayList<>();
      for (TsFileResource oldResource : resourcesToBeSettled) {
        TsFileAndModSettleTool tsFileAndModSettleTool = TsFileAndModSettleTool.getInstance();
        tsFileAndModSettleTool.settleOneTsFileAndMod(oldResource, settledResources);
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  public List<TsFileResource> createFiles() throws IOException, InterruptedException {
    List<TsFileResource> resourcesToBeSettled = new ArrayList<>();
    HashMap<String, List<String>> deviceSensorsMap = new HashMap<>();
    List<String> sensors = new ArrayList<>();

    // first File
    sensors.add(SENSOR1);
    deviceSensorsMap.put(DEVICE1, sensors);
    String timeseriesPath = STORAGE_GROUP + DEVICE1 + SENSOR1;
    createFile(resourcesToBeSettled, deviceSensorsMap, timeseriesPath);

    // second file
    path = folder + File.separator + System.currentTimeMillis() + "-" + 0 + "-0.tsfile";
    sensors.add(SENSOR2);
    deviceSensorsMap.put(DEVICE1, sensors);
    timeseriesPath = STORAGE_GROUP + DEVICE1 + SENSOR2;
    createFile(resourcesToBeSettled, deviceSensorsMap, timeseriesPath);

    Thread.sleep(100);
    // third file
    path = folder + File.separator + System.currentTimeMillis() + "-" + 0 + "-0.tsfile";
    createOneTsFile(deviceSensorsMap);
    TsFileResource tsFileResource = new TsFileResource(new File(path));
    tsFileResource.serialize();
    tsFileResource.close();
    resourcesToBeSettled.add(tsFileResource);

    return resourcesToBeSettled;
  }

  private void createFile(
      List<TsFileResource> resourcesToBeSettled,
      HashMap<String, List<String>> deviceSensorsMap,
      String timeseriesPath)
      throws IOException {
    createOneTsFile(deviceSensorsMap);
    createlModificationFile(timeseriesPath);
    TsFileResource tsFileResource = new TsFileResource(new File(path));
    tsFileResource.setModFile(
        new ModificationFile(tsFileResource.getTsFilePath() + ModificationFile.FILE_SUFFIX));
    tsFileResource.serialize();
    tsFileResource.close();
    resourcesToBeSettled.add(tsFileResource);
  }

  public void createlModificationFile(String timeseriesPath) {
    String modFilePath = path + ModificationFile.FILE_SUFFIX;
    ModificationFile modificationFile = new ModificationFile(modFilePath);
    List<Modification> mods = new ArrayList<>();
    try {
      PartialPath partialPath = new PartialPath(timeseriesPath);
      mods.add(new Deletion(partialPath, 10000000, 1500, 10000));
      mods.add(new Deletion(partialPath, 10000000, 20000, 30000));
      mods.add(new Deletion(partialPath, 10000000, 45000, 50000));
      for (Modification mod : mods) {
        modificationFile.write(mod);
      }
      modificationFile.close();
    } catch (IllegalPathException | IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  protected void createOneTsFile(HashMap<String, List<String>> deviceSensorsMap) {
    try {
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      TsFileWriter tsFileWriter = new TsFileWriter(f);
      // add measurements into file schema
      try {
        for (Map.Entry<String, List<String>> entry : deviceSensorsMap.entrySet()) {
          String device = entry.getKey();
          for (String sensor : entry.getValue()) {
            tsFileWriter.registerTimeseries(
                new Path(device), new MeasurementSchema(sensor, TSDataType.INT64, TSEncoding.RLE));
          }
        }
      } catch (WriteProcessException e) {
        Assert.fail(e.getMessage());
      }

      for (long timestamp = 0; timestamp < maxTimestamp; timestamp += 1000) {
        for (Map.Entry<String, List<String>> entry : deviceSensorsMap.entrySet()) {
          String device = entry.getKey();
          TSRecord tsRecord = new TSRecord(timestamp, device);
          for (String sensor : entry.getValue()) {
            DataPoint dataPoint = new LongDataPoint(sensor, timestamp + VALUE_OFFSET);
            tsRecord.addTuple(dataPoint);
          }
          tsFileWriter.write(tsRecord);
        }
      }
      tsFileWriter.flushAllChunkGroups();
      tsFileWriter.close();
    } catch (Throwable e) {
      Assert.fail(e.getMessage());
    }
  }
}
