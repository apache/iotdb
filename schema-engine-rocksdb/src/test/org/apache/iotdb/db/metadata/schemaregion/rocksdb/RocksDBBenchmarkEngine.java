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
package org.apache.iotdb.db.metadata.schemaregion.rocksdb;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.MetadataConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaReadWriteHandler.ROCKSDB_PATH;

public class RocksDBBenchmarkEngine {
  private static final Logger logger = LoggerFactory.getLogger(RocksDBBenchmarkEngine.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final int BIN_CAPACITY = 100 * 1000;

  private File logFile;
//  public static List<List<CreateTimeSeriesPlan>> timeSeriesSet = new ArrayList<>();
  public static Set<String> measurementPathSet = new HashSet<>();
  public static Set<String> innerPathSet = new HashSet<>();
//  public static List<SetStorageGroupPlan> storageGroups = new ArrayList<>();

  public RocksDBBenchmarkEngine() {
    String schemaDir = config.getSchemaDir();
    String logFilePath = schemaDir + File.separator + MetadataConstant.METADATA_LOG;
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    System.out.println(logFile.getAbsolutePath());
  }

  public static void main(String[] args) {
    RocksDBBenchmarkEngine engine = new RocksDBBenchmarkEngine();
    engine.startTest();
  }

//  @Test
  public void startTest() {
//    RocksDBTestUtils.printMemInfo("Benchmark rocksdb start");
//    try {
//      prepareBenchmark();
//      RocksDBTestUtils.printBenchmarkBaseline(
//          storageGroups, timeSeriesSet, measurementPathSet, innerPathSet);
//      /** rocksdb benchmark * */
//      RSchemaRegion rocksDBManager = new RSchemaRegion();
//      MRocksDBBenchmark mRocksDBBenchmark = new MRocksDBBenchmark(rocksDBManager);
//      mRocksDBBenchmark.testTimeSeriesCreation(timeSeriesSet);
//      mRocksDBBenchmark.testMeasurementNodeQuery(measurementPathSet);
//      RocksDBTestUtils.printReport(mRocksDBBenchmark.benchmarkResults, "rocksDB");
//      RocksDBTestUtils.printMemInfo("Benchmark finished");
//    } catch (IOException | MetadataException e) {
//      logger.error("Error happened when run benchmark", e);
//    }
  }

  public void prepareBenchmark() throws IOException {
    long time = System.currentTimeMillis();
    if (!logFile.exists()) {
      throw new FileNotFoundException("we need a mlog.bin to init the benchmark test");
    }
    // TODO Add it back after support RocksDB support in v1.0
//    try (MLogReader mLogReader =
//        new MLogReader(config.getSchemaDir(), MetadataConstant.METADATA_LOG)) {
//      parseForTestSet(mLogReader);
//      System.out.println("spend " + (System.currentTimeMillis() - time) + "ms to prepare dataset");
//    } catch (Exception e) {
//      throw new IOException("Failed to parser mlog.bin for err:" + e);
//    }
  }

//  private static void parseForTestSet(MLogReader mLogReader) throws IllegalPathException {
//    List<CreateTimeSeriesPlan> currentList = null;
//    SetStorageGroupPlan setStorageGroupPlan = new SetStorageGroupPlan();
//    setStorageGroupPlan.setPath(new PartialPath("root.iotcloud"));
//    storageGroups.add(setStorageGroupPlan);
//    while (mLogReader.hasNext()) {
//      PhysicalPlan plan = null;
//      try {
//        plan = mLogReader.next();
//        if (plan == null) {
//          continue;
//        }
//        switch (plan.getOperatorType()) {
//          case CREATE_TIMESERIES:
//            CreateTimeSeriesPlan createTimeSeriesPlan = (CreateTimeSeriesPlan) plan;
//            PartialPath path = createTimeSeriesPlan.getPath();
//            if (currentList == null) {
//              currentList = new ArrayList<>(BIN_CAPACITY);
//              timeSeriesSet.add(currentList);
//            }
//            measurementPathSet.add(path.getFullPath());
//
//            innerPathSet.add(path.getDeviceIdString());
//            String[] subNodes = ArrayUtils.subarray(path.getNodes(), 0, path.getNodes().length - 2);
//            innerPathSet.add(String.join(".", subNodes));
//
//            currentList.add(createTimeSeriesPlan);
//            if (currentList.size() >= BIN_CAPACITY) {
//              currentList = null;
//            }
//            break;
//          default:
//            break;
//        }
//      } catch (Exception e) {
//        logger.error(
//            "Can not operate cmd {} for err:", plan == null ? "" : plan.getOperatorType(), e);
//      }
//    }
//  }

  private void resetEnv() throws IOException {
    File rockdDbFile = new File(ROCKSDB_PATH);
    if (rockdDbFile.exists() && rockdDbFile.isDirectory()) {
      FileUtils.deleteDirectory(rockdDbFile);
    }
  }
}
