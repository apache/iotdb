/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.write.series;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.constant.TimeseriesTestConstant;
import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.encoding.decoder.PlainDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.PlainEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.write.page.PageWriter;
import org.junit.Assert;
import org.junit.Test;

public class PageWriterTest {

  @Test
  public void testNoFreq() {
    PageWriter writer = new PageWriter();
    writer.setTimeEncoder(new PlainEncoder(EndianType.LITTLE_ENDIAN, TSDataType.INT64, 0));
    writer.setValueEncoder(new PlainEncoder(EndianType.LITTLE_ENDIAN, TSDataType.INT64, 0));
    short s1 = 12;
    boolean b1 = false;
    int i1 = 1;
    long l1 = 123142120391L;
    float f1 = 2.2f;
    double d1 = 1294283.4323d;
    String str1 = "I have a dream";
    int timeCount = 0;
    try {
      writer.write(timeCount++, s1);
      writer.write(timeCount++, b1);
      writer.write(timeCount++, i1);
      writer.write(timeCount++, l1);
      writer.write(timeCount++, f1);
      writer.write(timeCount++, d1);
      writer.write(timeCount++, new Binary(str1));
      assertEquals(101, writer.estimateMaxMemSize());
      ByteBuffer buffer1 = writer.getUncompressedBytes();
      ByteBuffer buffer = ByteBuffer.wrap(buffer1.array());
      writer.reset();
      assertEquals(0, writer.estimateMaxMemSize());
      int timeSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      byte[] timeBytes = new byte[timeSize];
      buffer.get(timeBytes);
      ByteBuffer buffer2 = ByteBuffer.wrap(timeBytes);
      PlainDecoder decoder = new PlainDecoder(EndianType.LITTLE_ENDIAN);
      for (int i = 0; i < timeCount; i++) {
        assertEquals(i, decoder.readLong(buffer2));
      }
      assertEquals(s1, decoder.readShort(buffer));
      assertEquals(b1, decoder.readBoolean(buffer));
      assertEquals(i1, decoder.readInt(buffer));
      assertEquals(l1, decoder.readLong(buffer));
      Assert.assertEquals(f1, decoder.readFloat(buffer), TimeseriesTestConstant.float_min_delta);
      assertEquals(d1, decoder.readDouble(buffer), TimeseriesTestConstant.double_min_delta);
      assertEquals(str1, decoder.readBinary(buffer).getStringValue());

    } catch (IOException e) {
      fail();
    }
  }
}
