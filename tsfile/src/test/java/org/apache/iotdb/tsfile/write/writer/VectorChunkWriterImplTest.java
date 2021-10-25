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

import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.chunk.VectorChunkWriterImpl;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class VectorChunkWriterImplTest {

  @Test
  public void testWrite1() {
    VectorMeasurementSchemaStub measurementSchema = new VectorMeasurementSchemaStub();
    VectorChunkWriterImpl chunkWriter = new VectorChunkWriterImpl(measurementSchema);

    for (int time = 1; time <= 20; time++) {
      chunkWriter.write(time, (float) time, false);
      chunkWriter.write(time, time, false);
      chunkWriter.write(time, (double) time, false);
      chunkWriter.write(time);
    }

    chunkWriter.sealCurrentPage();
    // time chunk: 17 + 4 + 160;
    // value chunk 1: 19 + 2 + 4 + 3 + 80;
    // value chunk 2: 19 + 2 + 4 + 3 + 20;
    // value chunk 3: 20 + 4 + 7 + 20 * 8;
    assertEquals(528, chunkWriter.getSerializedChunkSize());

    try {
      TestTsFileOutput testTsFileOutput = new TestTsFileOutput();
      TsFileIOWriter writer = new TsFileIOWriter(testTsFileOutput, true);
      chunkWriter.writeToFileWriter(writer);
      PublicBAOS publicBAOS = testTsFileOutput.publicBAOS;
      ByteBuffer buffer = ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
      // time chunk
      assertEquals(
          (byte) (0x80 | MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER), ReadWriteIOUtils.readByte(buffer));
      assertEquals("vectorName", ReadWriteIOUtils.readVarIntString(buffer));
      assertEquals(164, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      assertEquals(TSDataType.VECTOR.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(CompressionType.UNCOMPRESSED.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(TSEncoding.PLAIN.serialize(), ReadWriteIOUtils.readByte(buffer));
      buffer.position(buffer.position() + 164);

      // value chunk 1
      assertEquals(0x40 | MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER, ReadWriteIOUtils.readByte(buffer));
      assertEquals("vectorName.s1", ReadWriteIOUtils.readVarIntString(buffer));
      assertEquals(89, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      assertEquals(TSDataType.FLOAT.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(CompressionType.UNCOMPRESSED.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(TSEncoding.PLAIN.serialize(), ReadWriteIOUtils.readByte(buffer));
      buffer.position(buffer.position() + 89);

      // value chunk 2
      assertEquals(0x40 | MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER, ReadWriteIOUtils.readByte(buffer));
      assertEquals("vectorName.s2", ReadWriteIOUtils.readVarIntString(buffer));
      assertEquals(29, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      assertEquals(TSDataType.INT32.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(CompressionType.UNCOMPRESSED.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(TSEncoding.PLAIN.serialize(), ReadWriteIOUtils.readByte(buffer));
      buffer.position(buffer.position() + 29);

      // value chunk 2
      assertEquals(0x40 | MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER, ReadWriteIOUtils.readByte(buffer));
      assertEquals("vectorName.s3", ReadWriteIOUtils.readVarIntString(buffer));
      assertEquals(171, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      assertEquals(TSDataType.DOUBLE.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(CompressionType.UNCOMPRESSED.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(TSEncoding.PLAIN.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(171, buffer.remaining());
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testWrite2() {
    VectorMeasurementSchemaStub measurementSchema = new VectorMeasurementSchemaStub();
    VectorChunkWriterImpl chunkWriter = new VectorChunkWriterImpl(measurementSchema);

    for (int time = 1; time <= 20; time++) {
      chunkWriter.write(time, (float) time, false);
      chunkWriter.write(time, time, false);
      chunkWriter.write(time, (double) time, false);
      chunkWriter.write(time);
    }
    chunkWriter.sealCurrentPage();
    for (int time = 21; time <= 40; time++) {
      chunkWriter.write(time, (float) time, false);
      chunkWriter.write(time, time, false);
      chunkWriter.write(time, (double) time, false);
      chunkWriter.write(time);
    }
    chunkWriter.sealCurrentPage();

    // time chunk: 17 + (4 + 17 + 160) * 2
    // value chunk 1: 20 + (2 + 41 + 4 + 3 + 80) * 2
    // value chunk 2: 20 + (2 + 41 + 4 + 3 + 20) * 2
    // value chunk 3: 20 + (4 + 57 + 4 + 3 + 160) * 2
    assertEquals(1295, chunkWriter.getSerializedChunkSize());

    try {
      TestTsFileOutput testTsFileOutput = new TestTsFileOutput();
      TsFileIOWriter writer = new TsFileIOWriter(testTsFileOutput, true);
      chunkWriter.writeToFileWriter(writer);
      PublicBAOS publicBAOS = testTsFileOutput.publicBAOS;
      ByteBuffer buffer = ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
      // time chunk
      assertEquals((byte) (0x80 | MetaMarker.CHUNK_HEADER), ReadWriteIOUtils.readByte(buffer));
      assertEquals("vectorName", ReadWriteIOUtils.readVarIntString(buffer));
      assertEquals(362, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      assertEquals(TSDataType.VECTOR.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(CompressionType.UNCOMPRESSED.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(TSEncoding.PLAIN.serialize(), ReadWriteIOUtils.readByte(buffer));
      buffer.position(buffer.position() + 362);

      // value chunk 1
      assertEquals(0x40 | MetaMarker.CHUNK_HEADER, ReadWriteIOUtils.readByte(buffer));
      assertEquals("vectorName.s1", ReadWriteIOUtils.readVarIntString(buffer));
      assertEquals(260, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      assertEquals(TSDataType.FLOAT.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(CompressionType.UNCOMPRESSED.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(TSEncoding.PLAIN.serialize(), ReadWriteIOUtils.readByte(buffer));
      buffer.position(buffer.position() + 260);

      // value chunk 2
      assertEquals(0x40 | MetaMarker.CHUNK_HEADER, ReadWriteIOUtils.readByte(buffer));
      assertEquals("vectorName.s2", ReadWriteIOUtils.readVarIntString(buffer));
      assertEquals(140, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      assertEquals(TSDataType.INT32.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(CompressionType.UNCOMPRESSED.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(TSEncoding.PLAIN.serialize(), ReadWriteIOUtils.readByte(buffer));
      buffer.position(buffer.position() + 140);

      // value chunk 2
      assertEquals(0x40 | MetaMarker.CHUNK_HEADER, ReadWriteIOUtils.readByte(buffer));
      assertEquals("vectorName.s3", ReadWriteIOUtils.readVarIntString(buffer));
      assertEquals(456, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      assertEquals(TSDataType.DOUBLE.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(CompressionType.UNCOMPRESSED.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(TSEncoding.PLAIN.serialize(), ReadWriteIOUtils.readByte(buffer));
      assertEquals(456, buffer.remaining());

    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }
}
