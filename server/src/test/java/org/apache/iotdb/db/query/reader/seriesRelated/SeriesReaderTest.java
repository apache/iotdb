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

package org.apache.iotdb.db.query.reader.seriesRelated;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.cache.TsFileMetaDataCache;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.*;

public class SeriesReaderTest {

  private static final String SERIES_READER_TEST_SG = "root.seriesReaderTest";
  private TSEncoding encoding = TSEncoding.PLAIN;
  private int seqFileNum = 5;
  private int unseqFileNum = 5;
  private int measurementNum = 10;
  private int deviceNum = 10;
  private long ptNum = 100;
  private long flushInterval = 20;
  private String[] deviceIds;
  private MeasurementSchema[] measurementSchemas;

  private List<TsFileResource> seqResources = new ArrayList<>();
  private List<TsFileResource> unseqResources = new ArrayList<>();


  @Before
  public void setUp() throws MetadataException, PathException, IOException, WriteProcessException {
    MManager.getInstance().init();
    prepareSeries();
    prepareFiles(seqFileNum, unseqFileNum);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
    TsFileMetaDataCache.getInstance().clear();
    DeviceMetaDataCache.getInstance().clear();
    MManager.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
    MergeManager.getINSTANCE().stop();
  }

  @Test
  public void batchTest() {
    try {
      SeriesReader seriesReader = new SeriesReader(new Path(SERIES_READER_TEST_SG + PATH_SEPARATOR + "device0", "sensor0"),
              TSDataType.INT32, new QueryContext(), seqResources, unseqResources, null, null);
      IBatchReader batchReader = new SeriesRawDataBatchReader(seriesReader);
      int count = 0;
      while (batchReader.hasNextBatch()) {
        BatchData batchData = batchReader.nextBatch();
        assertEquals(TSDataType.INT32, batchData.getDataType());
        assertEquals(20, batchData.length());
        for (int i = 0; i < batchData.length(); i++) {
          long expectedTime = i + 20 * count;
          assertEquals(expectedTime, batchData.currentTime());
          if (expectedTime < 200) {
            assertEquals(20000+expectedTime, batchData.getInt());
          }
          else if (expectedTime < 260 || (expectedTime >= 300 && expectedTime < 380) || expectedTime >= 400) {
            assertEquals(10000+expectedTime, batchData.getInt());
          }
          else {
            assertEquals(expectedTime, batchData.getInt());
          }
          batchData.next();
        }
        count++;
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void pointTest() {
    try {
      SeriesReader seriesReader = new SeriesReader(new Path(SERIES_READER_TEST_SG + PATH_SEPARATOR + "device0", "sensor0"),
              TSDataType.INT32, new QueryContext(), seqResources, unseqResources, null, null);
      IPointReader pointReader = new SeriesRawDataPointReader(seriesReader);
      long expectedTime = 0;
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = pointReader.nextTimeValuePair();
        assertEquals(expectedTime, timeValuePair.getTimestamp());
        int value = timeValuePair.getValue().getInt();
        if (expectedTime < 200) {
          assertEquals(20000+expectedTime, value);
        }
        else if (expectedTime < 260 || (expectedTime >= 300 && expectedTime < 380) || expectedTime >= 400) {
          assertEquals(10000+expectedTime, value);
        }
        else {
          assertEquals(expectedTime, value);
        }
        expectedTime++;

      }
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }

  }


  private void prepareSeries() throws MetadataException, PathException {
    measurementSchemas = new MeasurementSchema[measurementNum];
    for (int i = 0; i < measurementNum; i++) {
      measurementSchemas[i] = new MeasurementSchema("sensor" + i, TSDataType.INT32,
              encoding, CompressionType.UNCOMPRESSED);
    }
    deviceIds = new String[deviceNum];
    for (int i = 0; i < deviceNum; i++) {
      deviceIds[i] = SERIES_READER_TEST_SG + PATH_SEPARATOR + "device" + i;
    }
    MManager.getInstance().setStorageGroupToMTree(SERIES_READER_TEST_SG);
    for (String device : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        MManager.getInstance().addPathToMTree(
                device + PATH_SEPARATOR + measurementSchema.getMeasurementId(), measurementSchema
                        .getType(), measurementSchema.getEncodingType(), measurementSchema.getCompressor(),
                Collections.emptyMap());
      }
    }
  }

  private void removeFiles() throws IOException {
    for (TsFileResource tsFileResource : seqResources) {
      tsFileResource.remove();
    }
    for (TsFileResource tsFileResource : unseqResources) {
      tsFileResource.remove();
    }

    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    FileReaderManager.getInstance().stop();
  }

  private void prepareFiles(int seqFileNum, int unseqFileNum) throws IOException, WriteProcessException {
    for (int i = 0; i < seqFileNum; i++) {
      File file = new File(TestConstant.BASE_OUTPUT_PATH.concat(
              i + "seq" + IoTDBConstant.TSFILE_NAME_SEPARATOR + i + IoTDBConstant.TSFILE_NAME_SEPARATOR
                      + i + IoTDBConstant.TSFILE_NAME_SEPARATOR + 0
                      + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.setHistoricalVersions(Collections.singleton((long) i));
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }
    for (int i = 0; i < unseqFileNum; i++) {
      File file = new File(TestConstant.BASE_OUTPUT_PATH.concat(
              i + "unseq" + IoTDBConstant.TSFILE_NAME_SEPARATOR
                      + i + IoTDBConstant.TSFILE_NAME_SEPARATOR
                      + i + IoTDBConstant.TSFILE_NAME_SEPARATOR + 0
                      + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.setHistoricalVersions(Collections.singleton((long) (i + seqFileNum)));
      unseqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum * (i + 1) / unseqFileNum, 10000);
    }

    File file = new File(TestConstant.BASE_OUTPUT_PATH
            .concat(unseqFileNum + "unseq" + IoTDBConstant.TSFILE_NAME_SEPARATOR + unseqFileNum
                    + IoTDBConstant.TSFILE_NAME_SEPARATOR + unseqFileNum
                    + IoTDBConstant.TSFILE_NAME_SEPARATOR + 0 + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setClosed(true);
    tsFileResource.setHistoricalVersions(Collections.singleton((long) (seqFileNum + unseqFileNum)));
    unseqResources.add(tsFileResource);
    prepareFile(tsFileResource, 0, ptNum * 2, 20000);
  }

  private void prepareFile(TsFileResource tsFileResource, long timeOffset, long ptNum,
                   long valueOffset)
          throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getFile());
    for (MeasurementSchema measurementSchema : measurementSchemas) {
      fileWriter.addMeasurement(measurementSchema);
    }
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {
      for (int j = 0; j < deviceNum; j++) {
        TSRecord record = new TSRecord(i, deviceIds[j]);
        for (int k = 0; k < measurementNum; k++) {
          record.addTuple(DataPoint.getDataPoint(measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementId(), String.valueOf(i + valueOffset)));
        }
        fileWriter.write(record);
        tsFileResource.updateStartTime(deviceIds[j], i);
        tsFileResource.updateEndTime(deviceIds[j], i);
      }
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushForTest(tsFileResource.getHistoricalVersions().iterator().next());
      }
    }
    fileWriter.close();
  }
}
