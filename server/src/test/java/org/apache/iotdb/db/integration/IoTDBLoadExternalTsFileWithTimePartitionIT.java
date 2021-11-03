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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class IoTDBLoadExternalTsFileWithTimePartitionIT {

  private String DOT = ".";
  private String tempDir = "temp";

  String STORAGE_GROUP = "root.ln";
  String[] devices = new String[] {"d1", "d2", "d3"};
  String[] measurements = new String[] {"s1", "s2", "s3"};

  // generate several tsFiles, with timestamp from startTime(inclusive) to endTime(exclusive)
  private long startTime = 0;
  private long endTime = 1000_000;

  private long timePartition = 100; // unit s
  long recordTimeGap = 1000;

  private int originalTsFileNum = 0;

  long[] deviceDataPointNumber = new long[devices.length];

  boolean originalIsEnablePartition;
  long originalPartitionInterval;

  IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Before
  public void setUp() throws Exception {
    originalIsEnablePartition = config.isEnablePartition();
    originalPartitionInterval = config.getPartitionInterval();
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);

    StorageEngine.setEnablePartition(true);
    StorageEngine.setTimePartitionInterval(timePartition);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    StorageEngine.setEnablePartition(originalIsEnablePartition);
    StorageEngine.setTimePartitionInterval(originalPartitionInterval);
    File f = new File(tempDir);
    if (f.exists()) {
      FileUtils.deleteDirectory(f);
    }
  }

  /** get the name of tsfile given counter */
  String getName(int counter) {
    return tempDir + File.separator + System.currentTimeMillis() + "-" + counter + "-0-0.tsfile";
  }

  /** write a record, given timestamp */
  private void writeData(TsFileWriter tsFileWriter, long timestamp)
      throws IOException, WriteProcessException {
    for (String deviceId : devices) {
      TSRecord tsRecord = new TSRecord(timestamp, STORAGE_GROUP + DOT + deviceId);
      for (String measurement : measurements) {
        DataPoint dPoint = new LongDataPoint(measurement, 10000);
        tsRecord.addTuple(dPoint);
      }
      tsFileWriter.write(tsRecord);
    }
  }

  /** register all timeseries in tsfiles */
  private void register(TsFileWriter tsFileWriter) {
    try {
      for (String deviceId : devices) {
        for (String measurement : measurements) {
          tsFileWriter.registerTimeseries(
              new Path(STORAGE_GROUP + DOT + deviceId, measurement),
              new UnaryMeasurementSchema(measurement, TSDataType.INT64, TSEncoding.RLE));
        }
      }
    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
  }

  /** create multiple tsfiles, each correspond to a time partition. */
  private void prepareData() {
    File dir = new File(tempDir);
    if (dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
    dir.mkdir();
    try {
      File f;
      TsFileWriter tsFileWriter = null;
      int counter = 0;
      long recordTimeGap = 1000;
      for (long timestamp = startTime; timestamp < endTime; timestamp += recordTimeGap) {
        if (timestamp % (timePartition * recordTimeGap) == 0) {
          if (tsFileWriter != null) {
            tsFileWriter.flushAllChunkGroups();
            tsFileWriter.close();
            counter++;
          }
          String path = getName(counter);
          f = FSFactoryProducer.getFSFactory().getFile(path);
          tsFileWriter = new TsFileWriter(new TsFileIOWriter(f));
          register(tsFileWriter);
        }
        writeData(tsFileWriter, timestamp);
      }
      tsFileWriter.flushAllChunkGroups();
      tsFileWriter.close();
      originalTsFileNum = counter + 1;
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  @Test
  public void loadTsFileWithTimePartitionTest() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      prepareData();

      statement.execute(String.format("load '%s'", new File(tempDir).getAbsolutePath()));

      String dataDir = config.getDataDirs()[0];
      // sequence/logical_sg/virtual_sg/time_partitions
      File f =
          new File(
              dataDir,
              new PartialPath("sequence") + File.separator + "root.ln" + File.separator + "0");
      Assert.assertEquals(
          (endTime - startTime) / (timePartition), f.list().length * originalTsFileNum);

      int totalPartitionsNum = (int) ((endTime - startTime) / (timePartition) / originalTsFileNum);
      int[] splitTsFilePartitions = new int[totalPartitionsNum];
      for (int i = 0; i < splitTsFilePartitions.length; i++) {
        splitTsFilePartitions[i] = Integer.parseInt(f.list()[i]);
      }
      Arrays.sort(splitTsFilePartitions);

      for (int i = 0; i < (endTime - startTime) / (timePartition) / originalTsFileNum; i++) {
        Assert.assertEquals((i * originalTsFileNum), splitTsFilePartitions[i]);
      }

      // each time partition folder should contain 2 files, the tsfile and the resource file
      for (int i = 0; i < (endTime - startTime) / (timePartition) / originalTsFileNum; i++) {
        Assert.assertEquals(
            2, new File(f.getAbsolutePath(), "" + i * originalTsFileNum).list().length);
      }
    } catch (SQLException | IllegalPathException throwables) {
      throwables.printStackTrace();
      Assert.fail();
    }
  }

  void writeDataWithDifferentDevice(TsFileWriter tsFileWriter, long timestamp, int counter)
      throws IOException, WriteProcessException {
    int mod = (counter % 6);
    if (mod < 3) {
      TSRecord tsRecord = new TSRecord(timestamp, STORAGE_GROUP + DOT + devices[mod]);
      deviceDataPointNumber[mod] += 1;
      for (String measurement : measurements) {
        DataPoint dPoint = new LongDataPoint(measurement, 10000);
        tsRecord.addTuple(dPoint);
      }
      tsFileWriter.write(tsRecord);
    } else {
      for (int i = 2; i <= devices.length; i++) {
        for (int j = 1; j < i; j++) {
          if (i + j == mod) {
            TSRecord tsRecord1 = new TSRecord(timestamp, STORAGE_GROUP + DOT + devices[i - 1]);
            TSRecord tsRecord2 = new TSRecord(timestamp, STORAGE_GROUP + DOT + devices[j - 1]);
            deviceDataPointNumber[i - 1] += 1;
            deviceDataPointNumber[j - 1] += 1;
            for (String measurement : measurements) {
              DataPoint dataPoint1 = new LongDataPoint(measurement, 100);
              DataPoint dataPoint2 = new LongDataPoint(measurement, 10000);
              tsRecord1.addTuple(dataPoint1);
              tsRecord2.addTuple(dataPoint2);
            }
            tsFileWriter.write(tsRecord1);
            tsFileWriter.write(tsRecord2);
            return;
          }
        }
      }
    }
  }

  void prepareDataWithDifferentDevice() {
    startTime = 0;
    endTime = 100_000;
    recordTimeGap = 10;

    long tsfileMaxTime = 1000;
    File dir = new File(tempDir);
    if (dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
    dir.mkdir();
    try {
      File f;
      TsFileWriter tsFileWriter = null;
      int counter = 0;
      for (long timestamp = startTime; timestamp < endTime; timestamp += recordTimeGap) {
        if (timestamp % tsfileMaxTime == 0) {
          if (tsFileWriter != null) {
            tsFileWriter.flushAllChunkGroups();
            tsFileWriter.close();
            counter++;
          }
          String path = getName(counter);
          f = FSFactoryProducer.getFSFactory().getFile(path);
          tsFileWriter = new TsFileWriter(new TsFileIOWriter(f));
          register(tsFileWriter);
        }
        writeDataWithDifferentDevice(tsFileWriter, timestamp, counter);
      }
      tsFileWriter.flushAllChunkGroups();
      tsFileWriter.close();
      originalTsFileNum = counter + 1;
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  @Test
  public void loadTsFileWithDifferentDevice() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      prepareDataWithDifferentDevice();

      statement.execute(String.format("load '%s'", new File(tempDir).getAbsolutePath()));

      String dataDir = config.getDataDirs()[0];
      // sequence/logical_sg/virtual_sg/time_partitions
      File f =
          new File(
              dataDir,
              new PartialPath("sequence") + File.separator + "root.ln" + File.separator + "0");
      Assert.assertEquals((endTime - startTime) / (timePartition), f.list().length);

      int totalPartitionsNum = (int) ((endTime - startTime) / (timePartition));
      int[] splitTsFilePartitions = new int[totalPartitionsNum];
      for (int i = 0; i < splitTsFilePartitions.length; i++) {
        splitTsFilePartitions[i] = Integer.parseInt(f.list()[i]);
      }
      Arrays.sort(splitTsFilePartitions);

      for (int i = 0; i < (endTime - startTime) / (timePartition); i++) {
        Assert.assertEquals(i, splitTsFilePartitions[i]);
      }

      // each time partition folder should contain 2 files, the tsfile and the resource file
      for (int i = 0; i < (endTime - startTime) / (timePartition); i++) {
        Assert.assertEquals(2, new File(f.getAbsolutePath(), "" + i).list().length);
      }

      for (int i = 0; i < devices.length; i++) {
        statement.executeQuery(
            "select count(" + measurements[0] + ") from " + STORAGE_GROUP + DOT + devices[i]);
        ResultSet set = statement.getResultSet();
        set.next();
        long number = set.getLong(1);
        Assert.assertEquals(deviceDataPointNumber[i], number);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
