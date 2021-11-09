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
package org.apache.iotdb.db.engine.cache;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BloomFilterCacheTest {
  private List<String> pathList;
  private int pathSize = 3;
  private BloomFilterCache bloomFilterCache;
  private long allocateMemoryForBloomFilterCache;

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
              .concat("0")
              .concat(File.separator)
              .concat("0")
              .concat(File.separator)
              .concat("1-0-0-0.tsfile");
      String device = "d" + (i + 1);
      pathList.add(path);
      createTsFile(path, device);
    }
    // estimate weight and set allocated memory for bloom filter for test
    int estimateWeight = estimateWeight(pathList);
    allocateMemoryForBloomFilterCache =
        IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForBloomFilterCache();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setAllocateMemoryForBloomFilterCache(estimateWeight - 1);
    bloomFilterCache = BloomFilterCache.getInstance();
  }

  @After
  public void tearDown() {
    try {
      for (String filePath : pathList) {
        FileUtils.forceDelete(new File(filePath));
      }
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAllocateMemoryForBloomFilterCache(allocateMemoryForBloomFilterCache);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testGet() {
    try {
      for (String filePath : pathList) {
        BloomFilter bloomFilter =
            bloomFilterCache.get(new BloomFilterCache.BloomFilterCacheKey(filePath));
        Assert.assertNotEquals(bloomFilter, null);
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
      BloomFilterCache.BloomFilterCacheKey key = new BloomFilterCache.BloomFilterCacheKey(path);
      BloomFilter bloomFilter = bloomFilterCache.get(key);
      Assert.assertNotEquals(null, bloomFilter);
      bloomFilterCache.remove(key);
      bloomFilter = bloomFilterCache.getIfPresent(key);
      Assert.assertNull(bloomFilter);
    } catch (IOException e) {
      Assert.fail();
      e.printStackTrace();
    }
  }

  @Test
  public void testClear() {
    try {
      for (String path : pathList) {
        BloomFilterCache.BloomFilterCacheKey key = new BloomFilterCache.BloomFilterCacheKey(path);
        BloomFilter bloomFilter = bloomFilterCache.get(key);
        Assert.assertNotEquals(null, bloomFilter);
      }
      bloomFilterCache.clear();
      for (String path : pathList) {
        BloomFilterCache.BloomFilterCacheKey key = new BloomFilterCache.BloomFilterCacheKey(path);
        BloomFilter bloomFilter = bloomFilterCache.getIfPresent(key);
        Assert.assertNull(bloomFilter);
      }
    } catch (IOException e) {
      Assert.fail();
      e.printStackTrace();
    }
  }

  @Test
  public void testEvict() {
    try {
      bloomFilterCache.clear();
      for (String path : pathList) {
        BloomFilterCache.BloomFilterCacheKey key = new BloomFilterCache.BloomFilterCacheKey(path);
        BloomFilter bloomFilter = bloomFilterCache.get(key);
        Assert.assertNotEquals(null, bloomFilter);
      }
      bloomFilterCache.clearUp();
      BloomFilterCache.BloomFilterCacheKey key =
          new BloomFilterCache.BloomFilterCacheKey(pathList.get(0));
      Assert.assertNull(bloomFilterCache.getIfPresent(key));
      for (int i = 1; i < pathList.size(); i++) {
        key = new BloomFilterCache.BloomFilterCacheKey(pathList.get(i));
        Assert.assertNotEquals(null, bloomFilterCache.getIfPresent(key));
      }
    } catch (IOException e) {
      Assert.fail();
      e.printStackTrace();
    }
  }

  /**
   * estimate weight according to tsFile path
   *
   * @param pathList tsFile path list
   * @return estimate weight
   */
  private int estimateWeight(List<String> pathList) throws IOException {
    int res = 0;
    for (String path : pathList) {
      TsFileSequenceReader reader = FileReaderManager.getInstance().get(path, true);
      BloomFilterCache.BloomFilterCacheKey key = new BloomFilterCache.BloomFilterCacheKey(path);
      Pair<String, long[]> tsFilePrefixPathAndTsFileVersionPair =
          FilePathUtils.getTsFilePrefixPathAndTsFileVersionPair(path);
      String tsFilePrefixPath = tsFilePrefixPathAndTsFileVersionPair.left;
      res +=
          (int)
              (RamUsageEstimator.shallowSizeOf(key)
                  + RamUsageEstimator.sizeOf(tsFilePrefixPath)
                  + RamUsageEstimator.sizeOf(reader.readBloomFilter()));
    }
    return res;
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
      // add measurements into file schema (all with INT64 data type)
      for (int i = 0; i < sensorNum; i++) {
        IMeasurementSchema measurementSchema =
            new UnaryMeasurementSchema(
                sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF);
        measurementSchemas.add(measurementSchema);
        schema.registerTimeseries(
            new Path(device, sensorPrefix + (i + 1)),
            new UnaryMeasurementSchema(
                sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF));
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
