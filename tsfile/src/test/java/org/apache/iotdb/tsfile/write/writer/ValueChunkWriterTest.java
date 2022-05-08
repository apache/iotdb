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

import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.PlainEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.chunk.ValueChunkWriter;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ValueChunkWriterTest {

  @Test
  public void testWrite1() {
    Encoder valueEncoder = new PlainEncoder(TSDataType.FLOAT, 0);
    ValueChunkWriter chunkWriter =
        new ValueChunkWriter(
            "s1", CompressionType.UNCOMPRESSED, TSDataType.FLOAT, TSEncoding.PLAIN, valueEncoder);
    for (int time = 1; time <= 20; time++) {
      chunkWriter.write(time, (float) time, time % 4 == 0);
    }
    chunkWriter.sealCurrentPage();
    // page without statistics size: 69
    assertEquals(69L, chunkWriter.getCurrentChunkSize());
  }

  @Test
  public void testWrite2() {
    Encoder valueEncoder = new PlainEncoder(TSDataType.FLOAT, 0);
    ValueChunkWriter chunkWriter =
        new ValueChunkWriter(
            "s1", CompressionType.UNCOMPRESSED, TSDataType.FLOAT, TSEncoding.PLAIN, valueEncoder);
    for (int time = 1; time <= 20; time++) {
      chunkWriter.write(time, (float) time, time % 4 == 0);
    }
    chunkWriter.sealCurrentPage();
    for (int time = 20; time <= 40; time++) {
      chunkWriter.write(time, (float) time, time % 4 == 0);
    }
    chunkWriter.sealCurrentPage();
    // two pages with statistics size: (69 + 41) * 2
    assertEquals(220L, chunkWriter.getCurrentChunkSize());
  }
}
