package org.apache.iotdb.db.tools;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
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

public class TsFileAndModSettleToolTest {
  private final boolean newEnablePartition = true;
  private final long newPartitionInterval = 3600_000;
  protected final long maxTimestamp = 50000L; // 100000000L;
  protected final String folder =
      "C:\\IOTDB\\sourceCode\\choubenson\\iotdb\\data\\data\\sequence\\root.sg_0\\0\\0";
  protected final String STORAGE_GROUP = "root.sg_0";
  protected final String DEVICE1 = STORAGE_GROUP + ".device_1";
  protected final String DEVICE2 = STORAGE_GROUP + ".device_2";
  protected final String SENSOR1 = "sensor_1";
  protected final String SENSOR2 = "sensor_2";
  private final long VALUE_OFFSET = 1;
  private final Planner processor = new Planner();
  private String path = null;
  private IoTDBConfig config;
  private boolean originEnablePartition;
  private long originPartitionInterval;

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();

    config = IoTDBDescriptor.getInstance().getConfig();
    originEnablePartition = config.isEnablePartition();
    originPartitionInterval = config.getPartitionInterval();

    config.setEnablePartition(newEnablePartition);
    config.setPartitionInterval(newPartitionInterval);

    StorageEngine.setEnablePartition(newEnablePartition);
    StorageEngine.setTimePartitionInterval(newPartitionInterval);

    File f = new File(folder);
    if (!f.exists()) {
      boolean success = f.mkdir();
      Assert.assertTrue(success);
    }
    path = folder + File.separator + System.currentTimeMillis() + "-" + 0 + "-0.tsfile";
  }

  @After
  public void tearDown() {
    config.setEnablePartition(originEnablePartition);
    config.setPartitionInterval(originPartitionInterval);

    StorageEngine.setEnablePartition(originEnablePartition);
    StorageEngine.setTimePartitionInterval(originPartitionInterval);

    try {
      EnvironmentUtils.cleanEnv();
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void onlineSettleTest() {
    try {
      List<TsFileResource> oldResources = createFiles();
      for (TsFileResource resource : oldResources) {
        TsFileAndModSettleTool.recoverSettleFileMap.put(resource.getTsFilePath(), new Integer(1));
      }
      // new RegisterManager().register(SettleService.getINSTANCE());
    } catch (IOException | InterruptedException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void settleTsFilesAndModsTest() {
    try {
      List<TsFileResource> resourcesToBeSettled = createFiles();
      for (TsFileResource oldResource : resourcesToBeSettled) {
        try (TsFileAndModSettleTool tsFileAndModSettleTool =
            new TsFileAndModSettleTool(oldResource)) {
          tsFileAndModSettleTool.settleOneTsFileAndMod(oldResource);
        }
      }
    } catch (WriteProcessException | InterruptedException | IllegalPathException | IOException e) {
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
                new Path(device, sensor),
                new MeasurementSchema(sensor, TSDataType.INT64, TSEncoding.RLE));
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
      TsFileWriter tsFileWriter = new TsFileWriter(f);
      // add measurements into file schema
      try {
        for (Map.Entry<String, List<String>> entry : deviceSensorsMap.entrySet()) {
          String device = entry.getKey();
          for (String sensor : entry.getValue()) {
            tsFileWriter.registerTimeseries(
                new Path(device, sensor),
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
      tsFileWriter.close();
    } catch (Throwable e) {
      Assert.fail(e.getMessage());
    }
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
            new Path(device, sensor),
            new MeasurementSchema(sensor, TSDataType.INT64, TSEncoding.RLE));
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
}
