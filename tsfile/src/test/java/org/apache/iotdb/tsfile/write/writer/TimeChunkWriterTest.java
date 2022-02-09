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
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.chunk.TimeChunkWriter;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class TimeChunkWriterTest {

  @Test
  public void testWrite1() {
    Encoder timeEncoder = new PlainEncoder(TSDataType.INT64, 0);
    TimeChunkWriter chunkWriter =
        new TimeChunkWriter("c1", CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, timeEncoder);
    for (long time = 1; time <= 10; time++) {
      chunkWriter.write(time);
    }
    assertFalse(chunkWriter.checkPageSizeAndMayOpenANewPage());
    chunkWriter.sealCurrentPage();
    // page without statistics size: 82 + chunk header size: 8
    assertEquals(90L, chunkWriter.getCurrentChunkSize());

    try {
      TestTsFileOutput testTsFileOutput = new TestTsFileOutput();
      TsFileIOWriter writer = new TsFileIOWriter(testTsFileOutput, true);
      chunkWriter.writeAllPagesOfChunkToTsFile(writer);
      PublicBAOS publicBAOS = testTsFileOutput.publicBAOS;
      ByteBuffer buffer = ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
      assertEquals(
          (byte) (0x80 | MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER), ReadWriteIOUtils.readByte(buffer));
      assertEquals("c1", ReadWriteIOUtils.readVarIntString(buffer));
      assertEquals(82, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      assertEquals(TSDataType.VECTOR.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(CompressionType.UNCOMPRESSED.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(TSEncoding.PLAIN.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(82, buffer.remaining());
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testWrite2() {
    Encoder timeEncoder = new PlainEncoder(TSDataType.INT64, 0);
    TimeChunkWriter chunkWriter =
        new TimeChunkWriter("c1", CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, timeEncoder);
    for (long time = 1; time <= 10; time++) {
      chunkWriter.write(time);
    }
    chunkWriter.sealCurrentPage();
    for (long time = 11; time <= 20; time++) {
      chunkWriter.write(time);
    }
    chunkWriter.sealCurrentPage();
    assertEquals(2, chunkWriter.getNumOfPages());
    // two pages with statistics size: (82 + 17) * 2 + chunk header size: 9
    assertEquals(207L, chunkWriter.getCurrentChunkSize());

    try {
      TestTsFileOutput testTsFileOutput = new TestTsFileOutput();
      TsFileIOWriter writer = new TsFileIOWriter(testTsFileOutput, true);
      chunkWriter.writeAllPagesOfChunkToTsFile(writer);
      PublicBAOS publicBAOS = testTsFileOutput.publicBAOS;
      ByteBuffer buffer = ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
      assertEquals((byte) (0x80 | MetaMarker.CHUNK_HEADER), ReadWriteIOUtils.readByte(buffer));
      assertEquals("c1", ReadWriteIOUtils.readVarIntString(buffer));
      assertEquals(198, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      assertEquals(TSDataType.VECTOR.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(CompressionType.UNCOMPRESSED.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(TSEncoding.PLAIN.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(198, buffer.remaining());
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }
}
