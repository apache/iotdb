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
package org.apache.iotdb.db.engine.alter;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.alter.log.AlteringLogAnalyzer;
import org.apache.iotdb.db.engine.alter.log.AlteringLogger;
import org.apache.iotdb.db.engine.cache.AlteringRecordsCache;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.rescon.TsFileResourceManager;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.MeasurementGroup;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.commons.io.FileUtils;
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
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class RewriteTimeseriesTaskTest {

  String dataRegionId = "0";
  String pathBase =
      "data"
          .concat(File.separator)
          .concat("data")
          .concat(File.separator)
          .concat("sequence")
          .concat(File.separator)
          .concat("root.alttask1")
          .concat(File.separator)
          .concat(dataRegionId)
          .concat(File.separator)
          .concat("0")
          .concat(File.separator);
  String storageGroupName = "root.alttask1";
  String device = "root.alttask1.device_0";
  String alignedDevice = "root.alttask1.device_1";
  String deviceNotRewrite = "root.alttask1.device_3";
  String alignedDeviceNotRewrite = "root.alttask1.device_4";
  String sensorPrefix = "sensor_";
  // the number of rows to include in the tablet
  int rowNum = 1000000;
  // the number of values to include in the tablet
  int sensorNum = 10;

  TSEncoding defaultEncoding = TSEncoding.TS_2DIFF;
  CompressionType defaultCompressionType = CompressionType.GZIP;
  TsFileManager tsFileManager = null;
  int fileCount = 6;

  @Before
  public void setUp() throws Exception {
    try {
      File storageGroupSysDir =
          SystemFileFactory.INSTANCE.getFile(
              FilePathUtils.regularizePath(IoTDBDescriptor.getInstance().getConfig().getSystemDir())
                  + "storage_groups"
                  + File.separator
                  + File.separator
                  + storageGroupName,
              dataRegionId);
      if (!storageGroupSysDir.exists()) {
        storageGroupSysDir.mkdirs();
      }
      File logFile =
          SystemFileFactory.INSTANCE.getFile(
              storageGroupSysDir.getPath(), AlteringLogger.ALTERING_LOG_NAME);
      if (!logFile.exists()) {
        logFile.createNewFile();
      }
      AlteringLogger alteringLogger = new AlteringLogger(logFile);
      PartialPath fullPath = new PartialPath(device, sensorPrefix + "1");
      alteringLogger.addAlterParam(fullPath, TSEncoding.GORILLA, CompressionType.GZIP);
      PartialPath fullPathAlign = new PartialPath(alignedDevice, sensorPrefix + "1");
      alteringLogger.addAlterParam(fullPathAlign, TSEncoding.RLE, CompressionType.GZIP);
      alteringLogger.close();
      AlteringRecordsCache.getInstance()
          .putRecord(
              storageGroupName, fullPath.getFullPath(), TSEncoding.GORILLA, CompressionType.GZIP);
      AlteringRecordsCache.getInstance()
          .putRecord(
              storageGroupName, fullPathAlign.getFullPath(), TSEncoding.RLE, CompressionType.GZIP);
      tsFileManager =
          new TsFileManager(storageGroupName, dataRegionId, storageGroupSysDir.getPath());
      for (int fnum = 1; fnum <= 3; fnum++) {
        File f = FSFactoryProducer.getFSFactory().getFile(pathBase.concat(fnum + "-0-0-0.tsfile"));
        if (f.exists() && !f.delete()) {
          throw new RuntimeException("can not delete " + f.getAbsolutePath());
        }

        long beginTime = fnum * rowNum;
        long beginValue = fnum * 1000000L;
        writeFile(f, beginTime, beginValue, device, alignedDevice);
      }
      int fnum = 4;
      File f = FSFactoryProducer.getFSFactory().getFile(pathBase.concat(fnum + "-0-0-0.tsfile"));
      if (f.exists() && !f.delete()) {
        throw new RuntimeException("can not delete " + f.getAbsolutePath());
      }
      long beginTime = fnum * rowNum;
      long beginValue = fnum * 1000000L;
      writeFile(f, beginTime, beginValue, deviceNotRewrite, alignedDeviceNotRewrite);

      fnum = 5;
      f = FSFactoryProducer.getFSFactory().getFile(pathBase.concat(fnum + "-0-0-0.tsfile"));
      if (f.exists() && !f.delete()) {
        throw new RuntimeException("can not delete " + f.getAbsolutePath());
      }
      beginTime = fnum * rowNum;
      beginValue = fnum * 1000000L;
      writeFile(f, beginTime, beginValue, device, alignedDeviceNotRewrite);

      fnum = 6;
      f = FSFactoryProducer.getFSFactory().getFile(pathBase.concat(fnum + "-0-0-0.tsfile"));
      if (f.exists() && !f.delete()) {
        throw new RuntimeException("can not delete " + f.getAbsolutePath());
      }
      beginTime = fnum * rowNum;
      beginValue = fnum * 1000000L;
      writeFile(f, beginTime, beginValue, deviceNotRewrite, alignedDevice);

    } catch (Exception e) {
      throw new Exception("meet error in TsFileWrite with tablet", e);
    }
  }

  @Test
  public void taskTest() {
    try {
      List<Long> timepartitions = new ArrayList<>(2);
      timepartitions.add(0L);
      File logFile =
          SystemFileFactory.INSTANCE.getFile(
              tsFileManager.getStorageGroupDir(), AlteringLogger.ALTERING_LOG_NAME);
      Assert.assertTrue(logFile.exists());
      AlteringLogAnalyzer analyzer = new AlteringLogAnalyzer(logFile);
      analyzer.analyzer();
      // clear begin
      if (!analyzer.isClearBegin()) {
        AlteringLogger.clearBegin(logFile);
      }
      CompactionTaskManager.getInstance().start();
      RewriteTimeseriesTask task =
          new RewriteTimeseriesTask(
              storageGroupName,
              dataRegionId,
              timepartitions,
              tsFileManager,
              CompactionTaskManager.currentTaskNum,
              tsFileManager.getNextCompactionTaskId(),
              analyzer.isClearBegin(),
              analyzer.getDoneFiles(),
              logFile);
      boolean b = CompactionTaskManager.getInstance().addTaskToWaitingQueue(task);
      Assert.assertTrue(b);
      Future<CompactionTaskSummary> future =
          CompactionTaskManager.getInstance().getCompactionTaskFutureCheckStatusMayBlock(task);
      if (future == null) {
        Assert.assertTrue(task.isTaskFinished());
        Assert.assertTrue(task.isSuccess());
      } else {
        CompactionTaskSummary summary = future.get(600000, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(summary);
        Assert.assertTrue(summary.isFinished());
        Assert.assertTrue(summary.isSuccess());
      }
      for (int fnum = 1; fnum <= fileCount; fnum++) {
        File f = FSFactoryProducer.getFSFactory().getFile(pathBase.concat(fnum + "-0-0-0.tsfile"));
        File falter =
            FSFactoryProducer.getFSFactory().getFile(pathBase.concat(fnum + "-0-0-0.alter"));
        File fold =
            FSFactoryProducer.getFSFactory().getFile(pathBase.concat(fnum + "-0-0-0.alter.old"));
        Assert.assertTrue(f.exists());
        Assert.assertTrue(new File(f.getPath() + ".resource").exists());
        Assert.assertFalse(falter.exists());
        Assert.assertFalse(fold.exists());
        Set<String> devicesCache =
            AlteringRecordsCache.getInstance().getDevicesCache(storageGroupName);
        Map<String, Map<String, Pair<TSEncoding, CompressionType>>> alters = new HashMap<>();
        devicesCache.forEach(
            device -> {
              alters.put(device, AlteringRecordsCache.getInstance().getDeviceRecords(device));
            });
        readCheck(alters, f);
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  private void readCheck(
      Map<String, Map<String, Pair<TSEncoding, CompressionType>>> alters, File targetTsFile) {
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetTsFile.getAbsolutePath(), true)) {
      TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();

      while (deviceIterator.hasNext()) {
        Pair<String, Boolean> deviceInfo = deviceIterator.next();
        String device = deviceInfo.left;
        boolean aligned = deviceInfo.right;
        if (aligned) {
          List<AlignedChunkMetadata> alignedChunkMetadatas = reader.getAlignedChunkMetadata(device);
          for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadatas) {
            List<IChunkMetadata> valueChunkMetadataList =
                alignedChunkMetadata.getValueChunkMetadataList();
            for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
              Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
              ChunkHeader header = chunk.getHeader();
              Map<String, Pair<TSEncoding, CompressionType>> deviceMap = alters.get(device);
              if (deviceMap != null && deviceMap.containsKey(header.getMeasurementID())) {
                Pair<TSEncoding, CompressionType> tsEncodingCompressionTypePair =
                    deviceMap.get(header.getMeasurementID());
                Assert.assertEquals(header.getEncodingType(), tsEncodingCompressionTypePair.left);
                Assert.assertEquals(
                    header.getCompressionType(), tsEncodingCompressionTypePair.right);
              } else {
                Assert.assertEquals(header.getEncodingType(), defaultEncoding);
                Assert.assertEquals(header.getCompressionType(), defaultCompressionType);
              }
            }
          }
        } else {
          Map<String, List<ChunkMetadata>> measurementMap =
              reader.readChunkMetadataInDevice(device);
          for (Map.Entry<String, List<ChunkMetadata>> next : measurementMap.entrySet()) {
            String measurementId = next.getKey();
            List<ChunkMetadata> chunkMetadatas = next.getValue();
            for (ChunkMetadata chunkMetadata : chunkMetadatas) {
              Chunk currentChunk = reader.readMemChunk(chunkMetadata);
              ChunkHeader header = currentChunk.getHeader();
              Map<String, Pair<TSEncoding, CompressionType>> deviceMap = alters.get(device);
              if (deviceMap != null && deviceMap.containsKey(measurementId)) {
                Pair<TSEncoding, CompressionType> tsEncodingCompressionTypePair =
                    deviceMap.get(header.getMeasurementID());
                Assert.assertEquals(header.getEncodingType(), tsEncodingCompressionTypePair.left);
                Assert.assertEquals(
                    header.getCompressionType(), tsEncodingCompressionTypePair.right);
              } else {
                Assert.assertEquals(header.getEncodingType(), defaultEncoding);
                Assert.assertEquals(header.getCompressionType(), defaultCompressionType);
              }
            }
          }
        }
      }
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  private void writeFile(
      File f, long beginTime, long beginValue, String device, String alignedDevice)
      throws IOException, WriteProcessException {
    Schema schema = new Schema();
    List<MeasurementSchema> measurementSchemas = new ArrayList<>();
    // add measurements into file schema (all with INT64 data type)
    for (int i = 0; i < sensorNum; i++) {
      MeasurementSchema measurementSchema =
          new MeasurementSchema(
              sensorPrefix + (i + 1), TSDataType.INT64, defaultEncoding, defaultCompressionType);
      measurementSchemas.add(measurementSchema);
      schema.registerTimeseries(
          new Path(device),
          new MeasurementSchema(
              sensorPrefix + (i + 1), TSDataType.INT64, defaultEncoding, defaultCompressionType));
    }
    // add aligned measurements into file schema
    List<MeasurementSchema> schemas = new ArrayList<>();
    List<MeasurementSchema> alignedMeasurementSchemas = new ArrayList<>();
    for (int i = 0; i < sensorNum; i++) {
      MeasurementSchema schema1 =
          new MeasurementSchema(
              sensorPrefix + (i + 1), TSDataType.INT64, defaultEncoding, defaultCompressionType);
      schemas.add(schema1);
      alignedMeasurementSchemas.add(schema1);
    }
    MeasurementGroup group = new MeasurementGroup(true, schemas);
    schema.registerMeasurementGroup(new Path(alignedDevice), group);

    try (TsFileWriter tsFileWriter = new TsFileWriter(f, schema)) {

      // add measurements into TSFileWriter
      // construct the tablet
      Tablet tablet = new Tablet(device, measurementSchemas);
      long[] timestamps = tablet.timestamps;
      Object[] values = tablet.values;
      long timestamp = beginTime;
      long value = beginValue;
      for (int r = 0; r < rowNum; r++, value++) {
        int row = tablet.rowSize++;
        timestamps[row] = timestamp++;
        for (int i = 0; i < sensorNum; i++) {
          long[] sensor = (long[]) values[i];
          sensor[row] = value;
        }
        // write Tablet to TsFile
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          tsFileWriter.write(tablet);
          tablet.reset();
        }
      }
      // write Tablet to TsFile
      if (tablet.rowSize != 0) {
        tsFileWriter.write(tablet);
        tablet.reset();
      }

      // add aligned measurements into TSFileWriter
      // construct the tablet
      tablet = new Tablet(alignedDevice, alignedMeasurementSchemas);
      timestamps = tablet.timestamps;
      values = tablet.values;
      timestamp = beginTime;
      value = beginValue;
      for (int r = 0; r < rowNum; r++, value++) {
        int row = tablet.rowSize++;
        timestamps[row] = timestamp++;
        for (int i = 0; i < sensorNum; i++) {
          long[] sensor = (long[]) values[i];
          sensor[row] = value;
        }
        // write Tablet to TsFile
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          tsFileWriter.writeAligned(tablet);
          tablet.reset();
        }
      }
      // write Tablet to TsFile
      if (tablet.rowSize != 0) {
        tsFileWriter.writeAligned(tablet);
        tablet.reset();
      }
    }
    Map<String, Integer> deviceToIndex = new HashMap<>();
    deviceToIndex.put(device, 0);
    deviceToIndex.put(alignedDevice, 1);
    long[] startTimes = {beginTime, beginTime};
    long[] endTimes = {beginTime + rowNum - 1, beginTime + rowNum - 1};
    TsFileResource tsFileResource = new TsFileResource(f, deviceToIndex, startTimes, endTimes);
    tsFileResource.serialize();
    tsFileResource.close();
    TsFileResourceManager.getInstance().registerSealedTsFileResource(tsFileResource);
    tsFileManager.keepOrderInsert(tsFileResource, true);
  }

  @After
  public void tearDown() {
    try {
      File storageGroupDirFile =
          SystemFileFactory.INSTANCE.getFile(
              FilePathUtils.regularizePath(IoTDBDescriptor.getInstance().getConfig().getSystemDir())
                  + "storage_groups"
                  + File.separator
                  + File.separator
                  + storageGroupName,
              dataRegionId);
      if (storageGroupDirFile.exists()) {
        FileUtils.forceDeleteOnExit(storageGroupDirFile);
      }
      if (tsFileManager != null) {
        List<TsFileResource> tsFileList = tsFileManager.getTsFileList(true);
        tsFileList.forEach(
            tsFileResource -> {
              try {
                tsFileResource.delete();
              } catch (IOException e) {
                e.printStackTrace();
              }
            });
      } else {
        for (int fnum = 1; fnum <= fileCount; fnum++) {
          File f =
              FSFactoryProducer.getFSFactory().getFile(pathBase.concat(fnum + "-0-0-0.tsfile"));
          if (f.exists()) {
            FileUtils.forceDeleteOnExit(f);
          }
        }
      }
      FileUtils.forceDeleteOnExit(new File(pathBase));
      AlteringRecordsCache.getInstance().clear(storageGroupName);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }
}
