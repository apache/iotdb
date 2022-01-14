/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class AbstractCompactionTest {
  protected int seqFileNum = 5;
  protected int unseqFileNum = 0;
  protected List<TsFileResource> seqResources = new ArrayList<>();
  protected List<TsFileResource> unseqResources = new ArrayList<>();
  private int chunkGroupSize = 0;
  private int pageSize = 0;
  protected String COMPACTION_TEST_SG = TsFileGeneratorUtils.testStorageGroup;

  private static final long oldTargetChunkSize =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
  private static final int oldChunkGroupSize =
      TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte();
  private static final int oldPagePointSize =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  protected static File SEQ_DIRS =
      new File(
          TestConstant.BASE_OUTPUT_PATH
              + "data"
              + File.separator
              + "sequence"
              + File.separator
              + "root.compactionTest"
              + File.separator
              + "0"
              + File.separator
              + "0");
  protected static File UNSEQ_DIRS =
      new File(
          TestConstant.BASE_OUTPUT_PATH
              + "data"
              + File.separator
              + "unsequence"
              + File.separator
              + "root.compactionTest"
              + File.separator
              + "0"
              + File.separator
              + "0");

  private int fileVersion = 0;

  public void setUp() throws IOException, WriteProcessException, MetadataException {
    if (!SEQ_DIRS.exists()) {
      Assert.assertTrue(SEQ_DIRS.mkdirs());
    }
    if (!UNSEQ_DIRS.exists()) {
      Assert.assertTrue(UNSEQ_DIRS.mkdirs());
    }

    EnvironmentUtils.envSetUp();
    IoTDB.metaManager.init();
  }

  /**
   * @param fileNum the number of file
   * @param deviceNum device number in each file
   * @param measurementNum measurement number in each device of each file
   * @param pointNum data point number of each timeseries in each file
   * @param startTime start time of each timeseries
   * @param startValue start value of each timeseries
   * @param timeInterval time interval of each timeseries between files
   * @param valueInterval value interval of each timeseries between files
   * @param isAlign when it is true, it will create mix tsfile which contains aligned and nonAligned
   *     timeseries
   * @param isSeq
   * @throws IOException
   * @throws WriteProcessException
   * @throws MetadataException
   */
  protected void createFiles(
      int fileNum,
      int deviceNum,
      int measurementNum,
      int pointNum,
      int startTime,
      int startValue,
      int timeInterval,
      int valueInterval,
      boolean isAlign,
      boolean isSeq)
      throws IOException, WriteProcessException, MetadataException {
    for (int i = 0; i < fileNum; i++) {
      String fileName =
          System.currentTimeMillis()
              + FilePathUtils.FILE_NAME_SEPARATOR
              + fileVersion++
              + "-0-0.tsfile";
      String filePath;
      if (isSeq) {
        filePath = SEQ_DIRS.getPath() + File.separator + fileName;
      } else {
        filePath = UNSEQ_DIRS.getPath() + File.separator + fileName;
      }
      File file;
      if (isAlign) {
        file =
            TsFileGeneratorUtils.generateMixTsFile(
                filePath,
                deviceNum,
                measurementNum,
                pointNum,
                startTime + pointNum * i + timeInterval * i,
                startValue + pointNum * i + valueInterval * i,
                chunkGroupSize,
                pageSize);
      } else {
        file =
            TsFileGeneratorUtils.generateNonAlignedTsFile(
                filePath,
                deviceNum,
                measurementNum,
                pointNum,
                startTime + pointNum * i + timeInterval * i,
                startValue + pointNum * i + valueInterval * i,
                chunkGroupSize,
                pageSize);
      }
      addResource(
          file,
          deviceNum,
          startTime + pointNum * i + timeInterval * i,
          startTime + pointNum * i + timeInterval * i + pointNum - 1,
          isAlign,
          isSeq);
    }
  }

  private void addResource(
      File file, int deviceNum, long startTime, long endTime, boolean isAlign, boolean isSeq) {
    TsFileResource resource = new TsFileResource(file);
    int deviceStartindex = 0;
    if (isAlign) {
      for (int i = deviceStartindex; i < deviceStartindex + deviceNum; i++) {
        resource.updateStartTime(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i, startTime);
        resource.updateEndTime(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i, endTime);
      }
      deviceStartindex = TsFileGeneratorUtils.getAlignDeviceOffset();
    }
    for (int i = deviceStartindex; i < deviceStartindex + deviceNum; i++) {
      resource.updateStartTime(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i, startTime);
      resource.updateEndTime(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i, endTime);
    }
    resource.updatePlanIndexes(fileVersion);
    resource.setClosed(true);
    // resource.setTimeIndexType((byte) 0);
    if (isSeq) {
      seqResources.add(resource);
    } else {
      unseqResources.add(resource);
    }
  }

  protected void registerTimeseriesInMManger(int deviceNum, int measurementNum, boolean isAligned)
      throws MetadataException {
    for (int i = 0; i < deviceNum; i++) {
      if (isAligned) {
        List<String> measurements = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();
        List<TSEncoding> encodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        for (int j = 0; j < measurementNum; j++) {
          measurements.add("s" + j);
          dataTypes.add(TSDataType.INT64);
          encodings.add(TSEncoding.PLAIN);
          compressionTypes.add(CompressionType.UNCOMPRESSED);
          IoTDB.metaManager.createTimeseries(
              new PartialPath(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i, "s" + j),
              TSDataType.INT64,
              TSEncoding.PLAIN,
              CompressionType.UNCOMPRESSED,
              Collections.emptyMap());
        }
        IoTDB.metaManager.createAlignedTimeSeries(
            new PartialPath(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + (i + 10000)),
            measurements,
            dataTypes,
            encodings,
            compressionTypes);
      } else {
        for (int j = 0; j < measurementNum; j++) {
          IoTDB.metaManager.createTimeseries(
              new PartialPath(COMPACTION_TEST_SG + PATH_SEPARATOR + "d" + i, "s" + j),
              TSDataType.INT64,
              TSEncoding.PLAIN,
              CompressionType.UNCOMPRESSED,
              Collections.emptyMap());
        }
      }
    }
  }

  public void tearDown() throws IOException, StorageEngineException {
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanEnv();
    if (SEQ_DIRS.exists()) {
      FileUtils.deleteDirectory(SEQ_DIRS);
    }
    if (UNSEQ_DIRS.exists()) {
      FileUtils.deleteDirectory(UNSEQ_DIRS);
    }
    IoTDBDescriptor.getInstance().getConfig().setTargetChunkSize(oldTargetChunkSize);
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(oldChunkGroupSize);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(oldPagePointSize);
  }

  private void removeFiles() throws IOException {
    for (TsFileResource tsFileResource : seqResources) {
      if (tsFileResource.getTsFile().exists()) {
        tsFileResource.remove();
      }
    }
    for (TsFileResource tsFileResource : unseqResources) {
      if (tsFileResource.getTsFile().exists()) {
        tsFileResource.remove();
      }
    }
    File[] files = FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".tsfile");
    for (File file : files) {
      file.delete();
    }
    File[] resourceFiles =
        FSFactoryProducer.getFSFactory().listFilesBySuffix("target", ".resource");
    for (File resourceFile : resourceFiles) {
      resourceFile.delete();
    }
  }

  public void setChunkGroupSize(int chunkGroupSize) {
    this.chunkGroupSize = chunkGroupSize;
  }

  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }
}
