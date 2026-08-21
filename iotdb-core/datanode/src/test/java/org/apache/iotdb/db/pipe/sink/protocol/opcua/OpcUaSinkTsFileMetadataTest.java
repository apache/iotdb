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

package org.apache.iotdb.db.pipe.sink.protocol.opcua;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OpcUaSinkTsFileMetadataTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testReadLastValuesFromTreeTsFile() throws Exception {
    final File tsFile = new File(temporaryFolder.getRoot(), "tree.tsfile");
    final String device = "root.sg.d1";
    final List<MeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.INT64),
            new MeasurementSchema("blob", TSDataType.BLOB));
    final Tablet tablet = new Tablet(device, schemas, 3);
    for (int i = 0; i < 3; ++i) {
      tablet.addTimestamp(i, i + 1L);
    }
    tablet.addValue("s1", 0, 10L);
    tablet.addValue("s1", 1, 20L);
    tablet.addValue("s1", 2, null);
    tablet.addValue("blob", 0, new Binary("old", TSFileConfig.STRING_CHARSET));
    tablet.addValue("blob", 1, null);
    tablet.addValue("blob", 2, new Binary("last", TSFileConfig.STRING_CHARSET));
    tablet.rowSize = 3;

    try (final TsFileWriter writer = new TsFileWriter(tsFile)) {
      writer.registerTimeseries(new Path(device), schemas);
      writer.write(tablet);
    }

    final Map<IDeviceID, List<Pair<MeasurementSchema, TimeValuePair>>> deviceLastValues =
        OpcUaSink.readLastValues(tsFile);
    Assert.assertEquals(1, deviceLastValues.size());
    final Map<String, TimeValuePair> lastValues =
        toMeasurementMap(deviceLastValues.values().iterator().next());

    Assert.assertFalse(lastValues.containsKey(TsFileConstant.TIME_COLUMN_ID));
    assertLongLastValue(lastValues.get("s1"), 2L, 20L);
    assertBinaryLastValue(lastValues.get("blob"), 3L, "last");
  }

  private static Map<String, TimeValuePair> toMeasurementMap(
      final List<Pair<MeasurementSchema, TimeValuePair>> lastValues) {
    final Map<String, TimeValuePair> result = new LinkedHashMap<>();
    lastValues.forEach(
        lastValue -> result.put(lastValue.getLeft().getMeasurementId(), lastValue.getRight()));
    return result;
  }

  private static void assertLongLastValue(
      final TimeValuePair lastValue, final long timestamp, final long value) {
    Assert.assertNotNull(lastValue);
    Assert.assertEquals(timestamp, lastValue.getTimestamp());
    Assert.assertEquals(value, lastValue.getValue().getLong());
  }

  private static void assertBinaryLastValue(
      final TimeValuePair lastValue, final long timestamp, final String value) {
    Assert.assertNotNull(lastValue);
    Assert.assertEquals(timestamp, lastValue.getTimestamp());
    Assert.assertEquals(value, lastValue.getValue().getBinary().toString());
  }
}
