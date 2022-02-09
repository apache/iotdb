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
package org.apache.iotdb.tsfile.write.writer;

import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.encoding.decoder.PlainDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.PlainEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PageWriterTest {

  @Test
  public void testWriteInt() {
    PageWriter writer = new PageWriter();
    writer.setTimeEncoder(new PlainEncoder(TSDataType.INT64, 0));
    writer.setValueEncoder(new PlainEncoder(TSDataType.INT32, 0));
    writer.initStatistics(TSDataType.INT32);
    int value = 1;
    int timeCount = 0;
    try {
      writer.write(timeCount++, value);
      assertEquals(9, writer.estimateMaxMemSize());
      ByteBuffer buffer1 = writer.getUncompressedBytes();
      ByteBuffer buffer = ByteBuffer.wrap(buffer1.array());
      writer.reset(new UnaryMeasurementSchema("s0", TSDataType.INT32, TSEncoding.RLE));
      assertEquals(0, writer.estimateMaxMemSize());
      int timeSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      byte[] timeBytes = new byte[timeSize];
      buffer.get(timeBytes);
      ByteBuffer buffer2 = ByteBuffer.wrap(timeBytes);
      PlainDecoder decoder = new PlainDecoder();
      for (int i = 0; i < timeCount; i++) {
        assertEquals(i, decoder.readLong(buffer2));
      }
      decoder.reset();
      assertEquals(value, decoder.readInt(buffer));
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testWriteLong() {
    PageWriter writer = new PageWriter();
    writer.setTimeEncoder(new PlainEncoder(TSDataType.INT64, 0));
    writer.setValueEncoder(new PlainEncoder(TSDataType.INT64, 0));
    writer.initStatistics(TSDataType.INT64);
    long value = 123142120391L;
    int timeCount = 0;
    try {
      writer.write(timeCount++, value);
      assertEquals(16, writer.estimateMaxMemSize());
      ByteBuffer buffer1 = writer.getUncompressedBytes();
      ByteBuffer buffer = ByteBuffer.wrap(buffer1.array());
      writer.reset(new UnaryMeasurementSchema("s0", TSDataType.INT64, TSEncoding.RLE));
      assertEquals(0, writer.estimateMaxMemSize());
      int timeSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      byte[] timeBytes = new byte[timeSize];
      buffer.get(timeBytes);
      ByteBuffer buffer2 = ByteBuffer.wrap(timeBytes);
      PlainDecoder decoder = new PlainDecoder();
      for (int i = 0; i < timeCount; i++) {
        assertEquals(i, decoder.readLong(buffer2));
      }
      assertEquals(value, decoder.readLong(buffer));

    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testWriteFloat() {
    PageWriter writer = new PageWriter();
    writer.setTimeEncoder(new PlainEncoder(TSDataType.INT64, 0));
    writer.setValueEncoder(new PlainEncoder(TSDataType.FLOAT, 0));
    writer.initStatistics(TSDataType.FLOAT);
    float value = 2.2f;
    int timeCount = 0;
    try {
      writer.write(timeCount++, value);
      assertEquals(12, writer.estimateMaxMemSize());
      ByteBuffer buffer1 = writer.getUncompressedBytes();
      ByteBuffer buffer = ByteBuffer.wrap(buffer1.array());
      writer.reset(new UnaryMeasurementSchema("s0", TSDataType.INT64, TSEncoding.RLE));
      assertEquals(0, writer.estimateMaxMemSize());
      int timeSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      byte[] timeBytes = new byte[timeSize];
      buffer.get(timeBytes);
      ByteBuffer buffer2 = ByteBuffer.wrap(timeBytes);
      PlainDecoder decoder = new PlainDecoder();
      for (int i = 0; i < timeCount; i++) {
        assertEquals(i, decoder.readLong(buffer2));
      }
      assertEquals(value, decoder.readFloat(buffer), TestConstant.float_min_delta);

    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testWriteBoolean() {
    PageWriter writer = new PageWriter();
    writer.setTimeEncoder(new PlainEncoder(TSDataType.INT64, 0));
    writer.setValueEncoder(new PlainEncoder(TSDataType.BOOLEAN, 0));
    writer.initStatistics(TSDataType.BOOLEAN);
    boolean value = false;
    int timeCount = 0;
    try {
      writer.write(timeCount++, value);
      assertEquals(9, writer.estimateMaxMemSize());
      ByteBuffer buffer1 = writer.getUncompressedBytes();
      ByteBuffer buffer = ByteBuffer.wrap(buffer1.array());
      writer.reset(new UnaryMeasurementSchema("s0", TSDataType.INT64, TSEncoding.RLE));
      assertEquals(0, writer.estimateMaxMemSize());
      int timeSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      byte[] timeBytes = new byte[timeSize];
      buffer.get(timeBytes);
      ByteBuffer buffer2 = ByteBuffer.wrap(timeBytes);
      PlainDecoder decoder = new PlainDecoder();
      for (int i = 0; i < timeCount; i++) {
        assertEquals(i, decoder.readLong(buffer2));
      }
      assertEquals(value, decoder.readBoolean(buffer));
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testWriteBinary() {
    PageWriter writer = new PageWriter();
    writer.setTimeEncoder(new PlainEncoder(TSDataType.INT64, 0));
    writer.setValueEncoder(new PlainEncoder(TSDataType.TEXT, 0));
    writer.initStatistics(TSDataType.TEXT);
    String value = "I have a dream";
    int timeCount = 0;
    try {
      writer.write(timeCount++, new Binary(value));
      assertEquals(23, writer.estimateMaxMemSize());
      ByteBuffer buffer1 = writer.getUncompressedBytes();
      ByteBuffer buffer = ByteBuffer.wrap(buffer1.array());
      writer.reset(new UnaryMeasurementSchema("s0", TSDataType.INT64, TSEncoding.RLE));
      assertEquals(0, writer.estimateMaxMemSize());
      int timeSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      byte[] timeBytes = new byte[timeSize];
      buffer.get(timeBytes);
      ByteBuffer buffer2 = ByteBuffer.wrap(timeBytes);
      PlainDecoder decoder = new PlainDecoder();
      for (int i = 0; i < timeCount; i++) {
        assertEquals(i, decoder.readLong(buffer2));
      }
      assertEquals(value, decoder.readBinary(buffer).getStringValue());

    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testWriteDouble() {
    PageWriter writer = new PageWriter();
    writer.setTimeEncoder(new PlainEncoder(TSDataType.INT64, 0));
    writer.setValueEncoder(new PlainEncoder(TSDataType.DOUBLE, 0));
    writer.initStatistics(TSDataType.DOUBLE);
    double value = 1d;
    int timeCount = 0;
    try {
      writer.write(timeCount++, value);
      assertEquals(16, writer.estimateMaxMemSize());
      ByteBuffer buffer1 = writer.getUncompressedBytes();
      ByteBuffer buffer = ByteBuffer.wrap(buffer1.array());
      writer.reset(new UnaryMeasurementSchema("s0", TSDataType.INT64, TSEncoding.RLE));
      assertEquals(0, writer.estimateMaxMemSize());
      int timeSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      byte[] timeBytes = new byte[timeSize];
      buffer.get(timeBytes);
      ByteBuffer buffer2 = ByteBuffer.wrap(timeBytes);
      PlainDecoder decoder = new PlainDecoder();
      for (int i = 0; i < timeCount; i++) {
        assertEquals(i, decoder.readLong(buffer2));
      }
      assertEquals(value, decoder.readDouble(buffer), 0);

    } catch (IOException e) {
      fail();
    }
  }
}
