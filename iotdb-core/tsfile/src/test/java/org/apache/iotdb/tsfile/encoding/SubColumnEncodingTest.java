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

package org.apache.iotdb.tsfile.encoding;

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class SubColumnEncodingTest {

  @Test
  public void testSubColumnIntRoundTrip() throws Exception {
    int[] values = {
      21, 22, 23, 23, 24, 25, 100, 101, 102, -7, -7, 0, Integer.MIN_VALUE, Integer.MAX_VALUE
    };
    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(TSEncoding.SUBCOLUMN).getEncoder(TSDataType.INT32);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int value : values) {
      encoder.encode(value, out);
    }
    encoder.flush(out);

    Decoder decoder = Decoder.getDecoderByType(TSEncoding.SUBCOLUMN, TSDataType.INT32);
    ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
    for (int value : values) {
      Assert.assertTrue(decoder.hasNext(buffer));
      Assert.assertEquals(value, decoder.readInt(buffer));
    }
    Assert.assertFalse(decoder.hasNext(buffer));
  }

  @Test
  public void testSubColumnLongRoundTrip() throws Exception {
    long[] values = {1L, 2L, -3L, Long.MIN_VALUE, Long.MAX_VALUE};
    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(TSEncoding.SUBCOLUMN).getEncoder(TSDataType.INT64);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (long value : values) {
      encoder.encode(value, out);
    }
    encoder.flush(out);

    Decoder decoder = Decoder.getDecoderByType(TSEncoding.SUBCOLUMN, TSDataType.INT64);
    ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
    for (long value : values) {
      Assert.assertTrue(decoder.hasNext(buffer));
      Assert.assertEquals(value, decoder.readLong(buffer));
    }
    Assert.assertFalse(decoder.hasNext(buffer));
  }

  @Test
  public void testSubColumnFloatRoundTrip() throws Exception {
    float[] values = {1.23f, -4.5f, 0.001f, 100.125f, 100.125f, -0.75f};
    Encoder encoder =
        TSEncodingBuilder.getEncodingBuilder(TSEncoding.SUBCOLUMN).getEncoder(TSDataType.FLOAT);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (float value : values) {
      encoder.encode(value, out);
    }
    encoder.flush(out);

    Decoder decoder = Decoder.getDecoderByType(TSEncoding.SUBCOLUMN, TSDataType.FLOAT);
    ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
    for (float value : values) {
      Assert.assertTrue(decoder.hasNext(buffer));
      Assert.assertEquals(value, decoder.readFloat(buffer), 0.000001f);
    }
    Assert.assertFalse(decoder.hasNext(buffer));
  }

  @Test
  public void testSqlEncodingNameMapsToSubColumn() {
    Assert.assertEquals(TSEncoding.SUBCOLUMN, TSEncoding.valueOf("SubColumn".toUpperCase()));
  }
}
