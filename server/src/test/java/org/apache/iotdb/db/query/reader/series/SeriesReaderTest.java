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

package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SeriesReaderTest {

  private static final String SERIES_READER_TEST_SG = "root.seriesReaderTest";
  private List<String> deviceIds = new ArrayList<>();
  private List<UnaryMeasurementSchema> measurementSchemas = new ArrayList<>();

  private List<TsFileResource> seqResources = new ArrayList<>();
  private List<TsFileResource> unseqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    SeriesReaderTestUtil.setUp(measurementSchemas, deviceIds, seqResources, unseqResources);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unseqResources);
  }

  @Test
  public void batchTest() {
    try {
      Set<String> allSensors = new HashSet<>();
      allSensors.add("sensor0");
      SeriesReader seriesReader =
          new SeriesReader(
              new PartialPath(SERIES_READER_TEST_SG + ".device0.sensor0"),
              allSensors,
              TSDataType.INT32,
              EnvironmentUtils.TEST_QUERY_CONTEXT,
              seqResources,
              unseqResources,
              null,
              null,
              true);
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
            assertEquals(20000 + expectedTime, batchData.getInt());
          } else if (expectedTime < 260
              || (expectedTime >= 300 && expectedTime < 380)
              || expectedTime >= 400) {
            assertEquals(10000 + expectedTime, batchData.getInt());
          } else {
            assertEquals(expectedTime, batchData.getInt());
          }
          batchData.next();
        }
        count++;
      }
    } catch (IOException | IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void pointTest() {
    try {
      Set<String> allSensors = new HashSet<>();
      allSensors.add("sensor0");
      SeriesReader seriesReader =
          new SeriesReader(
              new PartialPath(SERIES_READER_TEST_SG + ".device0.sensor0"),
              allSensors,
              TSDataType.INT32,
              EnvironmentUtils.TEST_QUERY_CONTEXT,
              seqResources,
              unseqResources,
              null,
              null,
              true);
      IPointReader pointReader = new SeriesRawDataPointReader(seriesReader);
      long expectedTime = 0;
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = pointReader.nextTimeValuePair();
        assertEquals(expectedTime, timeValuePair.getTimestamp());
        int value = timeValuePair.getValue().getInt();
        if (expectedTime < 200) {
          assertEquals(20000 + expectedTime, value);
        } else if (expectedTime < 260
            || (expectedTime >= 300 && expectedTime < 380)
            || expectedTime >= 400) {
          assertEquals(10000 + expectedTime, value);
        } else {
          assertEquals(expectedTime, value);
        }
        expectedTime++;
      }
    } catch (IOException | IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void descOrderTest() {
    try {
      Set<String> allSensors = new HashSet<>();
      allSensors.add("sensor0");
      SeriesReader seriesReader =
          new SeriesReader(
              new PartialPath(SERIES_READER_TEST_SG + ".device0.sensor0"),
              allSensors,
              TSDataType.INT32,
              EnvironmentUtils.TEST_QUERY_CONTEXT,
              seqResources,
              unseqResources,
              null,
              null,
              false);
      IPointReader pointReader = new SeriesRawDataPointReader(seriesReader);
      long expectedTime = 499;
      while (pointReader.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = pointReader.nextTimeValuePair();
        assertEquals(expectedTime, timeValuePair.getTimestamp());
        int value = timeValuePair.getValue().getInt();
        if (expectedTime < 200) {
          assertEquals(20000 + expectedTime, value);
        } else if (expectedTime < 260
            || (expectedTime >= 300 && expectedTime < 380)
            || expectedTime >= 400) {
          assertEquals(10000 + expectedTime, value);
        } else {
          assertEquals(expectedTime, value);
        }
        expectedTime--;
      }
    } catch (IOException | IllegalPathException e) {
      e.printStackTrace();
      fail();
    }
  }
}
