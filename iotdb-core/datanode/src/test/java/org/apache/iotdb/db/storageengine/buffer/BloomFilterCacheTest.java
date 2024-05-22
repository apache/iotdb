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
package org.apache.iotdb.db.storageengine.buffer;

import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BloomFilterCacheTest {
  private List<String> pathList;
  private int pathSize = 3;
  private BloomFilterCache bloomFilterCache;

  @Before
  public void setUp() throws Exception {
    // set up and create tsFile
    pathList = new ArrayList<>();
    for (int i = 0; i < pathSize; i++) {
      String path =
          "target"
              .concat(File.separator)
              .concat("data")
              .concat(File.separator)
              .concat("data")
              .concat(File.separator)
              .concat("sequence")
              .concat(File.separator)
              .concat("root.sg" + (i + 1))
              .concat(File.separator)
              .concat(Integer.toString(i))
              .concat(File.separator)
              .concat("0")
              .concat(File.separator)
              .concat("1-0-0-0.tsfile");
      String device = "d" + (i + 1);
      pathList.add(path);
      createTsFile(path, device);
    }
    bloomFilterCache = BloomFilterCache.getInstance();
  }

  @After
  public void tearDown() {
    try {
      // clear opened file streams
      FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
      for (String filePath : pathList) {
        FileUtils.forceDelete(new File(filePath));
      }
      bloomFilterCache.clear();
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testGet() {
    try {
      for (String filePath : pathList) {
        TsFileID tsFileID = new TsFileID(filePath);
        BloomFilter bloomFilter =
            bloomFilterCache.get(
                new BloomFilterCache.BloomFilterCacheKey(
                    filePath,
                    tsFileID.regionId,
                    tsFileID.timePartitionId,
                    tsFileID.fileVersion,
                    tsFileID.compactionVersion));
        TsFileSequenceReader reader = FileReaderManager.getInstance().get(filePath, true);
        BloomFilter bloomFilter1 = reader.readBloomFilter();
        Assert.assertEquals(bloomFilter1, bloomFilter);
        reader.close();
      }
    } catch (IOException e) {
      Assert.fail();
      e.printStackTrace();
    }
  }

  @Test
  public void testRemove() {
    try {
      String path = pathList.get(0);
      TsFileID tsFileID = new TsFileID(path);
      BloomFilterCache.BloomFilterCacheKey key =
          new BloomFilterCache.BloomFilterCacheKey(
              path,
              tsFileID.regionId,
              tsFileID.timePartitionId,
              tsFileID.fileVersion,
              tsFileID.compactionVersion);
      BloomFilter bloomFilter = bloomFilterCache.get(key);
      TsFileSequenceReader reader = FileReaderManager.getInstance().get(path, true);
      BloomFilter bloomFilter1 = reader.readBloomFilter();
      Assert.assertEquals(bloomFilter1, bloomFilter);
      bloomFilterCache.remove(key);
      bloomFilter = bloomFilterCache.getIfPresent(key);
      Assert.assertNull(bloomFilter);
      reader.close();
    } catch (IOException e) {
      Assert.fail();
      e.printStackTrace();
    }
  }

  @Test
  public void testClear() {
    try {
      for (String path : pathList) {
        TsFileID tsFileID = new TsFileID(path);
        BloomFilterCache.BloomFilterCacheKey key =
            new BloomFilterCache.BloomFilterCacheKey(
                path,
                tsFileID.regionId,
                tsFileID.timePartitionId,
                tsFileID.fileVersion,
                tsFileID.compactionVersion);
        BloomFilter bloomFilter = bloomFilterCache.get(key);
        TsFileSequenceReader reader = FileReaderManager.getInstance().get(path, true);
        BloomFilter bloomFilter1 = reader.readBloomFilter();
        Assert.assertEquals(bloomFilter1, bloomFilter);
        reader.close();
      }
      bloomFilterCache.clear();
      for (String path : pathList) {
        TsFileID tsFileID = new TsFileID(path);
        BloomFilterCache.BloomFilterCacheKey key =
            new BloomFilterCache.BloomFilterCacheKey(
                path,
                tsFileID.regionId,
                tsFileID.timePartitionId,
                tsFileID.fileVersion,
                tsFileID.compactionVersion);
        BloomFilter bloomFilter = bloomFilterCache.getIfPresent(key);
        Assert.assertNull(bloomFilter);
      }
    } catch (IOException e) {
      Assert.fail();
      e.printStackTrace();
    }
  }

  /**
   * construct tsFile for test
   *
   * @param path tsFile path
   * @param device device in tsFile
   */
  private void createTsFile(String path, String device) throws Exception {
    try {
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      if (f.exists() && !f.delete()) {
        throw new RuntimeException("can not delete " + f.getAbsolutePath());
      }
      Schema schema = new Schema();
      String sensorPrefix = "sensor_";
      // the number of rows to include in the tablet
      int rowNum = 1000000;
      // the number of values to include in the tablet
      int sensorNum = 10;
      List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
      // add measurements into file metadata (all with INT64 data type)
      for (int i = 0; i < sensorNum; i++) {
        MeasurementSchema measurementSchema =
            new MeasurementSchema(sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF);
        measurementSchemas.add(measurementSchema);
        schema.registerTimeseries(
            new Path(device),
            new MeasurementSchema(sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF));
      }
      // add measurements into TSFileWriter
      try (TsFileWriter tsFileWriter = new TsFileWriter(f, schema)) {
        // construct the tablet
        Tablet tablet = new Tablet(device, measurementSchemas);
        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;
        long timestamp = 1;
        long value = 1000000L;
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
      }
    } catch (Exception e) {
      throw new Exception("meet error in TsFileWrite with tablet", e);
    }
  }
}
