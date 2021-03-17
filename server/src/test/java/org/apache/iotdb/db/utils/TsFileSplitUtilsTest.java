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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
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
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TsFileSplitUtilsTest {

  private String path = null;

  private IoTDBConfig config;
  private boolean originEnablePartition;
  private long originPartitionInterval;

  private boolean newEnablePartition = true;
  private long newPartitionInterval = 3600_000;

  private long maxTimestamp = 100000000L;

  private final String folder = "target" + File.separator + "split";

  private final String STORAGE_GROUP = "root.sg_0";
  private final String DEVICE1 = STORAGE_GROUP + ".device_1";
  private final String SENSOR1 = "sensor_1";
  private final long VALUE_OFFSET = 1;

  @Before
  public void setUp() {
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
    createOneTsFile();
  }

  @After
  public void tearDown() {
    File f = new File(path);
    if (f.exists()) {
      boolean deleteSuccess = f.delete();
      Assert.assertTrue(deleteSuccess);
    }
    config.setEnablePartition(originEnablePartition);
    config.setPartitionInterval(originPartitionInterval);

    StorageEngine.setEnablePartition(originEnablePartition);
    StorageEngine.setTimePartitionInterval(originPartitionInterval);

    File directory = new File("target" + File.separator + "split");
    try {
      FileUtils.deleteDirectory(directory);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void splitOneTsfileTest() {
    File tsFile = new File(path);
    TsFileResource tsFileResource = new TsFileResource(tsFile);

    List<TsFileResource> splitResource = new ArrayList<>();

    try {
      TsFileSplitUtils.splitOneTsfile(tsFileResource, splitResource);
    } catch (IOException | WriteProcessException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertEquals(maxTimestamp / newPartitionInterval + 1, splitResource.size());

    for (int i = 0; i < splitResource.size(); i++) {
      try {
        queryTsFile(splitResource.get(i).getTsFilePath(), i);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void createOneTsFile() {
    try {
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      TsFileWriter tsFileWriter = new TsFileWriter(f);
      // add measurements into file schema
      try {
        tsFileWriter.registerTimeseries(
            new Path(DEVICE1, SENSOR1),
            new MeasurementSchema(SENSOR1, TSDataType.INT64, TSEncoding.RLE));
      } catch (WriteProcessException e) {
        Assert.fail(e.getMessage());
      }

      for (long timestamp = 0; timestamp < maxTimestamp; timestamp += 1000) {
        // construct TSRecord
        TSRecord tsRecord = new TSRecord(timestamp, DEVICE1);
        DataPoint dataPoint = new LongDataPoint(SENSOR1, timestamp + VALUE_OFFSET);
        tsRecord.addTuple(dataPoint);
        tsFileWriter.write(tsRecord);
      }
      tsFileWriter.flushAllChunkGroups();
      tsFileWriter.close();
    } catch (Throwable e) {
      Assert.fail(e.getMessage());
    }
  }

  private void queryAndCheck(
      ArrayList<Path> paths, ReadOnlyTsFile readTsFile, IExpression statement, int index)
      throws IOException {
    QueryExpression queryExpression = QueryExpression.create(paths, statement);
    QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    long count = 0;
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      Assert.assertEquals(1, rowRecord.getFields().size());
      long timeStamp = rowRecord.getTimestamp();
      Assert.assertEquals(index * newPartitionInterval + count, timeStamp);
      Assert.assertEquals(timeStamp + VALUE_OFFSET, rowRecord.getFields().get(0).getLongV());
      count += 1000;
    }
  }

  public void queryTsFile(String TsFilePath, int index) throws IOException {
    // create reader and get the readTsFile interface
    try (TsFileSequenceReader reader = new TsFileSequenceReader(TsFilePath);
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader)) {
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path(DEVICE1, SENSOR1));
      queryAndCheck(paths, readTsFile, null, index);
    }
  }
}
