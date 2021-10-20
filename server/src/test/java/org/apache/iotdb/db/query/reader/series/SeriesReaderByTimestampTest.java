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

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SeriesReaderByTimestampTest {

  private static final String SERIES_READER_TEST_SG = "root.seriesReaderTest";
  private List<String> deviceIds = new ArrayList<>();
  private List<UnaryMeasurementSchema> measurementSchemas = new ArrayList<>();

  private List<TsFileResource> seqResources = new ArrayList<>();
  private List<TsFileResource> unseqResources = new ArrayList<>();

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    EnvironmentUtils.envSetUp();
    SeriesReaderTestUtil.setUp(measurementSchemas, deviceIds, seqResources, unseqResources);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
    SeriesReaderTestUtil.tearDown(seqResources, unseqResources);
  }

  @Test
  public void test() throws IOException, IllegalPathException {
    QueryDataSource dataSource = new QueryDataSource(seqResources, unseqResources);

    Set<String> allSensors = new HashSet<>();
    allSensors.add("sensor0");

    SeriesReaderByTimestamp seriesReader =
        new SeriesReaderByTimestamp(
            new PartialPath(SERIES_READER_TEST_SG + ".device0.sensor0"),
            allSensors,
            TSDataType.INT32,
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            dataSource,
            null,
            true);

    long timestamps[] = new long[500];
    for (int i = 0; i < 500; i++) {
      timestamps[i] = i;
    }
    Object[] values = seriesReader.getValuesInTimestamps(timestamps, timestamps.length);
    for (int time = 0; time < 500; time++) {
      if (time < 200) {
        Assert.assertEquals(time + 20000, values[time]);
      } else if (time < 260 || (time >= 300 && time < 380) || (time >= 400)) {
        Assert.assertEquals(time + 10000, values[time]);
      } else {
        Assert.assertEquals(time, values[time]);
      }
    }
  }
}
