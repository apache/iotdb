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

package org.apache.iotdb.db.storageengine.dataregion.utils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TableDeviceLastValueCollectorTest {

  @Test
  public void testIgnoreBlobChunkMetadataLastValue() {
    final TableDeviceLastValueCollector collector =
        TableDeviceLastValueCollector.create(Long.MAX_VALUE);
    final IDeviceID deviceID = Mockito.mock(IDeviceID.class);
    final IChunkMetadata chunkMetadata = Mockito.mock(IChunkMetadata.class);
    final Statistics<? extends Serializable> statistics = Mockito.mock(Statistics.class);

    Mockito.when(deviceID.ramBytesUsed()).thenReturn(0L);
    Mockito.when(chunkMetadata.getDataType()).thenReturn(TSDataType.BLOB);
    Mockito.when(chunkMetadata.getMeasurementUid()).thenReturn("s1");
    Mockito.when(chunkMetadata.getEndTime()).thenReturn(1L);
    Mockito.when(chunkMetadata.getStatistics()).thenReturn(statistics);
    Mockito.when(statistics.getLastValue())
        .thenThrow(new AssertionError("BLOB last value should not be read"));

    collector.update(deviceID, chunkMetadata);

    final Map<IDeviceID, List<Pair<String, TimeValuePair>>> lastValues =
        collector.toTsFileResourceLastValues();
    Assert.assertNotNull(lastValues);
    Assert.assertTrue(lastValues.containsKey(deviceID));
    Assert.assertEquals(Collections.emptyList(), lastValues.get(deviceID));
  }

  @Test
  public void testIgnoreBlobTimeseriesMetadataLastValue() {
    final TableDeviceLastValueCollector collector =
        TableDeviceLastValueCollector.create(Long.MAX_VALUE);
    final IDeviceID deviceID = Mockito.mock(IDeviceID.class);
    final TimeseriesMetadata timeseriesMetadata = Mockito.mock(TimeseriesMetadata.class);
    final Statistics<? extends Serializable> statistics = Mockito.mock(Statistics.class);

    Mockito.when(deviceID.ramBytesUsed()).thenReturn(0L);
    Mockito.when(timeseriesMetadata.getTsDataType()).thenReturn(TSDataType.BLOB);
    Mockito.when(timeseriesMetadata.getMeasurementId()).thenReturn("s1");
    Mockito.when(timeseriesMetadata.getStatistics()).thenReturn(statistics);
    Mockito.when(statistics.getEndTime()).thenReturn(1L);
    Mockito.when(statistics.getLastValue())
        .thenThrow(new AssertionError("BLOB last value should not be read"));

    collector.update(deviceID, Collections.singletonList(timeseriesMetadata));

    final Map<IDeviceID, List<Pair<String, TimeValuePair>>> lastValues =
        collector.toTsFileResourceLastValues();
    Assert.assertNotNull(lastValues);
    Assert.assertTrue(lastValues.containsKey(deviceID));
    Assert.assertEquals(Collections.emptyList(), lastValues.get(deviceID));
  }
}
