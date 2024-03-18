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
package org.apache.iotdb.db.storageengine.dataregion.compaction;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createChunkWriter;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createCompressionType;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createDataType;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createEncodingType;
import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.writeNonAlignedChunk;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class GenerateFileTest extends AbstractCompactionTest {
  String COMPACTION_TEST_SG = "root.test.g_0";

  // startTime = 1704038400000
  @Test
  public void test() throws IOException, IllegalPathException {
    // IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(512);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(10000);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(5000);
    // TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(3);

    Thread thread0=new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          testFile0();
        } catch (IOException | IllegalPathException e) {
          throw new RuntimeException(e);
        }
      }
    });
    Thread thread1=new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          testFile1();
        } catch (IOException | IllegalPathException e) {
          throw new RuntimeException(e);
        }
      }
    });
    Thread thread2=new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          testFile2();
        } catch (IOException | IllegalPathException e) {
          throw new RuntimeException(e);
        }
      }
    });

    thread0.start();
    thread1.start();
    thread2.start();

    try {
      // 等待第一个子线程完成
      thread0.join();

      // 等待第二个子线程完成
      thread1.join();

      // 等待第三个子线程完成
      thread2.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // 所有子线程完成后，main 函数继续执行
    System.out.println("所有子线程已完成");
  }

  private void testFile0() throws IOException, IllegalPathException {
    // seq file 1
    int deviceNum = 2000;
    int measurementNum = 50;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        if (deviceIndex % 100 == 0) System.out.println("File0 :"+deviceIndex);
        tsFileIOWriter.startChunkGroup(COMPACTION_TEST_SG + PATH_SEPARATOR + "d_" + deviceIndex);

        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<TSEncoding> encodings = createEncodingType(measurementNum);
        List<CompressionType> compressionTypes = createCompressionType(measurementNum);
        List<Integer> measurementIndexes = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          measurementIndexes.add(i);
        }
        List<PartialPath> timeseriesPath =
                createTimeseries(deviceIndex, measurementIndexes, dataTypes, false);

        List<IChunkWriter> iChunkWriters =
                createChunkWriter(timeseriesPath, dataTypes, encodings, compressionTypes, false);

        // write chunk
        for (int i = 0; i < 3; i++) {
          // write page
          List<TimeRange> pages = new ArrayList<>();
          pages.add(new TimeRange(i* 30000L,i* 30000L +4999));
          pages.add(new TimeRange(i* 30000L+5000,i* 30000L +5000+4999));

          for (IChunkWriter iChunkWriter : iChunkWriters) {
            writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
          }
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d_" + deviceIndex, 0L);
        resource.updateEndTime(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d_" + deviceIndex, 69999L);

        // generateModsFile(timeseriesPath, resource, 500, 600);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

  }

  private void testFile1() throws IOException, IllegalPathException {
    // seq file 1
    int deviceNum = 2000;
    int measurementNum = 50;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        if (deviceIndex % 100 == 0) System.out.println("File1 :"+deviceIndex);
        tsFileIOWriter.startChunkGroup(COMPACTION_TEST_SG + PATH_SEPARATOR + "d_" + deviceIndex);

        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<TSEncoding> encodings = createEncodingType(measurementNum);
        List<CompressionType> compressionTypes = createCompressionType(measurementNum);
        List<Integer> measurementIndexes = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          measurementIndexes.add(i);
        }
        List<PartialPath> timeseriesPath =
                createTimeseries(deviceIndex, measurementIndexes, dataTypes, false);

        List<IChunkWriter> iChunkWriters =
                createChunkWriter(timeseriesPath, dataTypes, encodings, compressionTypes, false);

        // write chunk
        for (int i = 0; i < 3; i++) {
          // write page
          List<TimeRange> pages = new ArrayList<>();
          pages.add(new TimeRange(10000+i* 30000L,10000+i* 30000L +4999));
          pages.add(new TimeRange(10000+i* 30000L+5000,10000+i* 30000L +5000+4999));

          for (IChunkWriter iChunkWriter : iChunkWriters) {
            writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
          }
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d_" + deviceIndex, 10000L);
        resource.updateEndTime(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d_" + deviceIndex, 79999L);

        // generateModsFile(timeseriesPath, resource, 500, 600);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

  }

  private void testFile2() throws IOException, IllegalPathException {
    // seq file 1
    int deviceNum = 2000;
    int measurementNum = 50;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileIOWriter tsFileIOWriter = new TsFileIOWriter(resource.getTsFile())) {
      // write the data in device
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        if (deviceIndex % 100 == 0) System.out.println("File2 :"+deviceIndex);
        tsFileIOWriter.startChunkGroup(COMPACTION_TEST_SG + PATH_SEPARATOR + "d_" + deviceIndex);

        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<TSEncoding> encodings = createEncodingType(measurementNum);
        List<CompressionType> compressionTypes = createCompressionType(measurementNum);
        List<Integer> measurementIndexes = new ArrayList<>();
        for (int i = 0; i < measurementNum; i++) {
          measurementIndexes.add(i);
        }
        List<PartialPath> timeseriesPath =
                createTimeseries(deviceIndex, measurementIndexes, dataTypes, false);

        List<IChunkWriter> iChunkWriters =
                createChunkWriter(timeseriesPath, dataTypes, encodings, compressionTypes, false);

        // write chunk
        for (int i = 0; i < 3; i++) {
          // write page
          List<TimeRange> pages = new ArrayList<>();
          pages.add(new TimeRange(20000+i* 30000L,20000+i* 30000L +4999));
          pages.add(new TimeRange(20000+i* 30000L+5000,20000+i* 30000L +5000+4999));

          for (IChunkWriter iChunkWriter : iChunkWriters) {
            writeNonAlignedChunk((ChunkWriterImpl) iChunkWriter, tsFileIOWriter, pages, true);
          }
        }

        tsFileIOWriter.endChunkGroup();
        resource.updateStartTime(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d_" + deviceIndex, 20000L);
        resource.updateEndTime(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d_" + deviceIndex, 89999L);

        // generateModsFile(timeseriesPath, resource, 500, 600);
      }
      tsFileIOWriter.endFile();
    }
    resource.serialize();
    seqResources.add(resource);

  }

  @Test
  public void test1() throws IOException, MetadataException, WriteProcessException {
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(512000000000000L);
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkPointNum(6000);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(2000);
    int deviceNum = 10000;
    int measurementNum = 50;
    TsFileResource resource = createEmptyFileAndResource(true);
    try (TsFileWriter tsFileWriter = new TsFileWriter(resource.getTsFile())) {
      for (int deviceIndex = 0; deviceIndex < deviceNum; deviceIndex++) {
        if (deviceIndex % 100 == 0) System.out.println(deviceIndex);
        List<TSDataType> dataTypes = createDataType(measurementNum);
        List<TSEncoding> encodings = createEncodingType(measurementNum);
        List<CompressionType> compressionTypes = createCompressionType(measurementNum);
        List<MeasurementSchema> measurementSchemas =
            createTimeseriesSchema(measurementNum, dataTypes, encodings, compressionTypes);
        String device = COMPACTION_TEST_SG + "." + "d_" + deviceIndex;
        tsFileWriter.registerTimeseries(new Path(device), measurementSchemas);
        // TsFileGeneratorUtils.writeWithTablet(tsFileWriter,device,measurementSchemas,18000,1704038400000L,0,false);

        Tablet tablet = new Tablet(device, measurementSchemas);
        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;
        tablet.initBitMaps();
        long sensorNum = measurementSchemas.size();
        long startTime = 1704038400000L;
        long value = -1;
        for (long r = 0; r < 18000; r++) {
          int row = tablet.rowSize++;
          value++;
          timestamps[row] = startTime++;
          for (int i = 0; i < sensorNum; i++) {
            switch (measurementSchemas.get(i).getType()) {
              case INT64:
                long[] sensor = (long[]) values[i];
                sensor[row] = value;
                break;
              case INT32:
                ((int[]) values[i])[row] = (int) value;
                break;
              case DOUBLE:
                ((double[]) values[i])[row] = (double) value;
                break;
              case FLOAT:
                ((float[]) values[i])[row] = (float) value;
                break;
              case BOOLEAN:
                ((boolean[]) values[i])[row] = true;
                break;
              case TEXT:
              default:
                ((Binary[]) values[i])[row] =
                    new Binary(String.valueOf(value), TSFileConfig.STRING_CHARSET);
                break;
            }
          }
          // write
          if (tablet.rowSize == tablet.getMaxRowNumber()) {
            tsFileWriter.write(tablet);
            tablet.reset();
          }
        }
        // write
        if (tablet.rowSize != 0) {
          tsFileWriter.write(tablet);
          tablet.reset();
        }
      }
    }
  }

  public List<PartialPath> createTimeseries(
      int deviceIndex,
      List<Integer> meausurementIndex,
      List<TSDataType> dataTypes,
      boolean isAligned)
      throws IllegalPathException {
    List<PartialPath> timeseriesPath = new ArrayList<>();
    for (int i : meausurementIndex) {
      if (!isAligned) {
        timeseriesPath.add(
            new MeasurementPath(
                COMPACTION_TEST_SG
                    + PATH_SEPARATOR
                    + "d_"
                    + deviceIndex
                    + PATH_SEPARATOR
                    + "s_"
                    + i,
                dataTypes.get(i)));
      } else {
        timeseriesPath.add(
            new AlignedPath(
                COMPACTION_TEST_SG + PATH_SEPARATOR + "d_" + deviceIndex,
                Collections.singletonList("s_" + i),
                Collections.singletonList(new MeasurementSchema("s_" + i, dataTypes.get(i)))));
      }
    }
    return timeseriesPath;
  }

  public List<MeasurementSchema> createTimeseriesSchema(
      int measurementNum,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressionTypes) {
    List<MeasurementSchema> timeseriesPath = new ArrayList<>();
    for (int i = 0; i < measurementNum; i++) {
      MeasurementSchema measurementSchema =
          new MeasurementSchema(
              "s_" + i, dataTypes.get(i), encodings.get(i), compressionTypes.get(i));
      timeseriesPath.add(measurementSchema);
    }
    return timeseriesPath;
  }
}
