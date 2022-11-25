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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.sys.OperateFilePlan;
import org.apache.iotdb.db.tools.TsFileSplitByPartitionTool;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsFileRewriteToolTest {

  private final long newPartitionInterval = 3600_000;
  protected final long maxTimestamp = 100000000L;
  protected final String folder = "target" + File.separator + "split";
  protected final String STORAGE_GROUP = "root.sg_0";
  protected final String DEVICE1 = STORAGE_GROUP + ".device_1";
  protected final String DEVICE2 = STORAGE_GROUP + ".device_2";
  protected final String SENSOR1 = "sensor_1";
  protected final String SENSOR2 = "sensor_2";
  private final long VALUE_OFFSET = 1;
  private final IPlanExecutor queryExecutor = new PlanExecutor();
  private final Planner processor = new Planner();
  private String path = null;
  private IoTDBConfig config;

  private long originPartitionInterval;

  public TsFileRewriteToolTest() throws QueryProcessException {}

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
    File f = new File(path);
    if (f.exists()) {
      boolean deleteSuccess = f.delete();
      Assert.assertTrue(deleteSuccess);
    }
    config.setTimePartitionInterval(originPartitionInterval);

    File directory = new File(folder);
    try {
      FileUtils.deleteDirectory(directory);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }

    try {
      EnvironmentUtils.cleanEnv();
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void splitOneTsfileWithOneDeviceOneSensorTest() {
    HashMap<String, List<String>> deviceSensorsMap = new HashMap<>();
    List<String> sensors = new ArrayList<>();
    sensors.add(SENSOR1);
    deviceSensorsMap.put(DEVICE1, sensors);
    createOneTsFile(deviceSensorsMap);
    splitFileAndQueryCheck(deviceSensorsMap);
  }

  @Test
  public void splitOneTsfileWithOneDeviceTwoSensorTest() {
    HashMap<String, List<String>> deviceSensorsMap = new HashMap<>();
    List<String> sensors = new ArrayList<>();
    sensors.add(SENSOR1);
    sensors.add(SENSOR2);
    deviceSensorsMap.put(DEVICE1, sensors);
    createOneTsFile(deviceSensorsMap);
    splitFileAndQueryCheck(deviceSensorsMap);
  }

  @Test
  public void splitOneTsfileWithTwoDeviceTwoSensorTest() {
    HashMap<String, List<String>> deviceSensorsMap = new HashMap<>();
    List<String> sensors = new ArrayList<>();
    sensors.add(SENSOR1);
    sensors.add(SENSOR2);
    deviceSensorsMap.put(DEVICE1, sensors);
    deviceSensorsMap.put(DEVICE2, sensors);
    createOneTsFile(deviceSensorsMap);
    splitFileAndQueryCheck(deviceSensorsMap);
  }

  @Ignore
  @Test
  public void loadFileTest() {
    HashMap<String, List<String>> deviceSensorsMap = new HashMap<>();
    List<String> sensors = new ArrayList<>();
    sensors.add(SENSOR1);
    deviceSensorsMap.put(DEVICE1, sensors);
    createOneTsFile(deviceSensorsMap);
    // try load the tsfile
    String sql = String.format("load '%s'", path);
    try {
      queryExecutor.processNonQuery(processor.parseSQLToPhysicalPlan(sql));
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void loadFileWithOnlyOnePageTest() {
    HashMap<String, List<String>> deviceSensorsMap = new HashMap<>();
    List<String> sensors = new ArrayList<>();
    sensors.add(SENSOR1);
    deviceSensorsMap.put(DEVICE1, sensors);
    createOneTsFileWithOnlyOnePage(deviceSensorsMap);
    // try load the tsfile
    try {
      queryExecutor.processNonQuery(
          new OperateFilePlan(new File(path), Operator.OperatorType.LOAD_FILES));
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
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

  private void splitFileAndQueryCheck(HashMap<String, List<String>> deviceSensorsMap) {
    File tsFile = new File(path);
    TsFileResource tsFileResource = new TsFileResource(tsFile);
    List<TsFileResource> splitResource = new ArrayList<>();
    try {
      TsFileSplitByPartitionTool.rewriteTsFile(tsFileResource, splitResource);
    } catch (IOException | WriteProcessException | IllegalPathException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertEquals(maxTimestamp / newPartitionInterval + 1, splitResource.size());

    for (int i = 0; i < splitResource.size(); i++) {
      try {
        queryAndCheckTsFileWithOneDevice(splitResource.get(i).getTsFilePath(), i, deviceSensorsMap);
        long partitionId = splitResource.get(i).getTimePartition();
        Assert.assertEquals(i, partitionId);
      } catch (IOException e) {
        Assert.fail(e.getMessage());
      }
    }
  }

  private void createOneTsFileWithOnlyOnePage(HashMap<String, List<String>> deviceSensorsMap) {
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

      int count = 0;
      for (long timestamp = 1; ; timestamp += newPartitionInterval) {
        if (count == 2) {
          break;
        }
        count++;
        for (Map.Entry<String, List<String>> entry : deviceSensorsMap.entrySet()) {
          String device = entry.getKey();
          TSRecord tsRecord = new TSRecord(timestamp, device);
          for (String sensor : entry.getValue()) {
            DataPoint dataPoint = new LongDataPoint(sensor, timestamp + VALUE_OFFSET);
            tsRecord.addTuple(dataPoint);
          }
          tsFileWriter.write(tsRecord);
        }
        tsFileWriter.flushAllChunkGroups();
      }
      tsFileWriter.close();
    } catch (Throwable e) {
      Assert.fail(e.getMessage());
    }
  }

  protected void createOneTsFile(HashMap<String, List<String>> deviceSensorsMap) {
    try {
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
        // add measurements into file schema
        try {
          for (Map.Entry<String, List<String>> entry : deviceSensorsMap.entrySet()) {
            String device = entry.getKey();
            for (String sensor : entry.getValue()) {
              tsFileWriter.registerTimeseries(
                  new Path(device),
                  new MeasurementSchema(sensor, TSDataType.INT64, TSEncoding.RLE));
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
      }
    } catch (Throwable e) {
      Assert.fail(e.getMessage());
    }
  }

  public void queryAndCheckTsFileWithOneDevice(
      String tsFilePath, int index, HashMap<String, List<String>> deviceSensorsMap)
      throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFilePath);
        TsFileReader readTsFile = new TsFileReader(reader)) {
      ArrayList<Path> paths = new ArrayList<>();

      int totalSensorCount = 0;
      for (Map.Entry<String, List<String>> entry : deviceSensorsMap.entrySet()) {
        String device = entry.getKey();
        for (String sensor : entry.getValue()) {
          totalSensorCount++;
          paths.add(new Path(device, sensor, true));
        }
      }

      QueryExpression queryExpression = QueryExpression.create(paths, null);
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
      long count = 0;
      while (queryDataSet.hasNext()) {
        RowRecord rowRecord = queryDataSet.next();
        Assert.assertEquals(totalSensorCount, rowRecord.getFields().size());
        long timeStamp = rowRecord.getTimestamp();
        Assert.assertEquals(index * newPartitionInterval + count, timeStamp);
        for (int i = 0; i < totalSensorCount; i++) {
          Assert.assertEquals(timeStamp + VALUE_OFFSET, rowRecord.getFields().get(i).getLongV());
        }
        count += 1000;
      }
    }
  }

  @Test
  public void splitOneTsfileWithTwoPagesTest() {
    createOneTsFileWithTwoPages(DEVICE1, SENSOR1);
    splitTwoPagesFileAndQueryCheck(DEVICE1, SENSOR1);
  }

  private void createOneTsFileWithTwoPages(String device, String sensor) {
    TSFileConfig fileConfig = TSFileDescriptor.getInstance().getConfig();
    int originMaxNumberOfPointsInPage = fileConfig.getMaxNumberOfPointsInPage();
    fileConfig.setMaxNumberOfPointsInPage(2);
    try {
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      TsFileWriter tsFileWriter = new TsFileWriter(f);
      // add measurements into file schema
      try {
        tsFileWriter.registerTimeseries(
            new Path(device), new MeasurementSchema(sensor, TSDataType.INT64, TSEncoding.RLE));
      } catch (WriteProcessException e) {
        Assert.fail(e.getMessage());
      }

      long timestamp = 1;
      // First page is crossing time partitions
      // Time stamp (1, 3600001)
      TSRecord tsRecord = new TSRecord(timestamp, device);
      DataPoint dataPoint = new LongDataPoint(sensor, timestamp);
      tsRecord.addTuple(dataPoint);
      tsFileWriter.write(tsRecord);
      timestamp += newPartitionInterval;
      tsRecord = new TSRecord(timestamp, device);
      dataPoint = new LongDataPoint(sensor, timestamp);
      tsRecord.addTuple(dataPoint);
      tsFileWriter.write(tsRecord);
      // Second page is in one time partition
      // Time stamp (3600002, 3600003)
      for (int i = 0; i < 2; i++) {
        timestamp++;
        tsRecord = new TSRecord(timestamp, device);
        dataPoint = new LongDataPoint(sensor, timestamp);
        tsRecord.addTuple(dataPoint);
        tsFileWriter.write(tsRecord);
      }
      tsFileWriter.flushAllChunkGroups();
      tsFileWriter.close();
      fileConfig.setMaxNumberOfPointsInPage(originMaxNumberOfPointsInPage);
    } catch (Throwable e) {
      Assert.fail(e.getMessage());
      fileConfig.setMaxNumberOfPointsInPage(originMaxNumberOfPointsInPage);
    }
  }

  private void splitTwoPagesFileAndQueryCheck(String device, String sensor) {
    File tsFile = new File(path);
    TsFileResource tsFileResource = new TsFileResource(tsFile);
    List<TsFileResource> splitResource = new ArrayList<>();
    try {
      TsFileSplitByPartitionTool.rewriteTsFile(tsFileResource, splitResource);
    } catch (IOException | WriteProcessException | IllegalPathException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertEquals(2, splitResource.size());

    for (int i = 0; i < splitResource.size(); i++) {
      try {
        queryAndCheckTsFile(splitResource.get(i).getTsFilePath(), i, device, sensor);
        long partitionId = splitResource.get(i).getTimePartition();
        Assert.assertEquals(i, partitionId);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void queryAndCheckTsFile(String tsFilePath, int index, String device, String sensor)
      throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFilePath);
        TsFileReader readTsFile = new TsFileReader(reader)) {
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path(device, sensor, true));

      QueryExpression queryExpression = QueryExpression.create(paths, null);
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
      if (index == 0) {
        // First file, contains time stamp 1
        int count = 0;
        while (queryDataSet.hasNext()) {
          count++;
          RowRecord rowRecord = queryDataSet.next();
          long timeStamp = rowRecord.getTimestamp();
          Assert.assertEquals(1, timeStamp);
        }
        Assert.assertEquals(1, count);
      } else {
        // Second file, contains time stamp 3600001, 3600002, 3600003
        int count = 0;
        while (queryDataSet.hasNext()) {
          count++;
          RowRecord rowRecord = queryDataSet.next();
          long timeStamp = rowRecord.getTimestamp();
          Assert.assertEquals(newPartitionInterval + count, timeStamp);
        }
        Assert.assertEquals(3, count);
      }
    }
  }
}
