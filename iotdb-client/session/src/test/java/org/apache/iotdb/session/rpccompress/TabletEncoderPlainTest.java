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

package org.apache.iotdb.session.rpccompress;

import org.apache.iotdb.session.util.SessionUtils;

import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.PlainEncoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * The bulk PLAIN path of {@link TabletEncoder} must write exactly the bytes the generic {@link
 * PlainEncoder} writes value by value, including the "repeat the last non-null value" rule.
 */
public class TabletEncoderPlainTest {

  private static final TSDataType[] TYPES = {
    TSDataType.INT64, TSDataType.TIMESTAMP, TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.BOOLEAN
  };

  @Test
  public void plainBulkPathMatchesPlainEncoder() {
    Random random = new Random(42);
    for (int iteration = 0; iteration < 100; iteration++) {
      Tablet tablet = randomTablet(random, iteration);
      TabletEncoder encoder =
          new TabletEncoder(
              CompressionType.UNCOMPRESSED,
              Collections.nCopies(TYPES.length + 1, TSEncoding.PLAIN));

      PublicBAOS expectedTime = new PublicBAOS();
      Encoder timeEncoder = new PlainEncoder(TSDataType.INT64, 0);
      for (int row = 0; row < tablet.getRowSize(); row++) {
        timeEncoder.encode(tablet.getTimestamp(row), expectedTime);
      }
      Assert.assertArrayEquals(
          "time column, iteration " + iteration,
          Arrays.copyOf(expectedTime.getBuf(), expectedTime.size()),
          bytes(encoder.encodeTime(tablet)));

      PublicBAOS expectedValues = new PublicBAOS();
      for (int column = 0; column < TYPES.length; column++) {
        SessionUtils.encodeValue(
            TYPES[column], tablet, column, new PlainEncoder(TYPES[column], 0), expectedValues);
      }
      byte[] actualValues = bytes(encoder.encodeValues(tablet));
      // encodeValues appends the bit maps after the value columns
      Assert.assertArrayEquals(
          "value columns, iteration " + iteration,
          Arrays.copyOf(expectedValues.getBuf(), expectedValues.size()),
          Arrays.copyOf(actualValues, expectedValues.size()));
    }
  }

  private static Tablet randomTablet(Random random, int iteration) {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    for (int column = 0; column < TYPES.length; column++) {
      schemas.add(new MeasurementSchema("s" + column, TYPES[column]));
    }
    int rows = 1 + random.nextInt(300);
    Tablet tablet = new Tablet("root.sg.d1", schemas, rows);
    for (int row = 0; row < rows; row++) {
      tablet.addTimestamp(row, random.nextLong());
      for (int column = 0; column < TYPES.length; column++) {
        if (random.nextInt(5) == 0) {
          continue; // leave a null
        }
        switch (TYPES[column]) {
          case INT64:
          case TIMESTAMP:
            tablet.addValue(row, column, random.nextLong());
            break;
          case DOUBLE:
            tablet.addValue(row, column, iteration % 7 == 0 ? Double.NaN : random.nextGaussian());
            break;
          case FLOAT:
            tablet.addValue(row, column, random.nextFloat() - 0.5f);
            break;
          case BOOLEAN:
            tablet.addValue(row, column, random.nextBoolean());
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }
    return tablet;
  }

  private static byte[] bytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }
}
