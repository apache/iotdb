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
package org.apache.iotdb.cluster.query.reader.mult;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class MultSeriesRawDataPointReaderTest {

  private MultSeriesRawDataPointReader reader;

  @Before
  public void setUp() throws IllegalPathException, IOException {
    BatchData batchData = TestUtils.genBatchData(TSDataType.DOUBLE, 0, 100);
    Map<String, IPointReader> pointReaderMap = Maps.newHashMap();
    SeriesRawDataPointReader seriesRawDataBatchReader =
        Mockito.mock(SeriesRawDataPointReader.class);
    Mockito.when(seriesRawDataBatchReader.hasNextTimeValuePair()).thenReturn(true);
    TimeValuePair timeValuePair =
        new TimeValuePair(batchData.currentTime(), batchData.currentTsPrimitiveType());
    Mockito.when(seriesRawDataBatchReader.nextTimeValuePair()).thenReturn(timeValuePair);
    pointReaderMap.put("root.a.b", seriesRawDataBatchReader);
    pointReaderMap.put("root.a.c", seriesRawDataBatchReader);
    reader = new MultSeriesRawDataPointReader(pointReaderMap);
  }

  @Test
  public void testMultSeriesReader() throws IOException, StorageEngineException {
    boolean hasNext = this.reader.hasNextTimeValuePair("root.a.b");
    assertEquals(true, hasNext);
    TimeValuePair timeValuePair = this.reader.nextTimeValuePair("root.a.b");
    assertEquals(0, timeValuePair.getTimestamp());
    assertEquals(0 * 1.0, timeValuePair.getValue().getDouble(), 0.0001);
  }
}
