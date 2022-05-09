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

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.PlainDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.PlainEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.page.TimePageWriter;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TimePageWriterTest {

  @Test
  public void testWrite() {
    Encoder timeEncoder = new PlainEncoder(TSDataType.INT64, 0);
    ICompressor compressor = ICompressor.getCompressor(CompressionType.UNCOMPRESSED);
    TimePageWriter pageWriter = new TimePageWriter(timeEncoder, compressor);
    try {
      pageWriter.write(1L);
      assertEquals(8, pageWriter.estimateMaxMemSize());
      ByteBuffer buffer1 = pageWriter.getUncompressedBytes();
      ByteBuffer buffer = ByteBuffer.wrap(buffer1.array());
      pageWriter.reset();
      assertEquals(0, pageWriter.estimateMaxMemSize());
      byte[] timeBytes = new byte[8];
      buffer.get(timeBytes);
      ByteBuffer buffer2 = ByteBuffer.wrap(timeBytes);
      PlainDecoder decoder = new PlainDecoder();
      assertEquals(1L, decoder.readLong(buffer2));
      decoder.reset();
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testWritePageHeaderAndDataIntoBuffWithoutCompress1() {
    Encoder timeEncoder = new PlainEncoder(TSDataType.INT64, 0);
    ICompressor compressor = ICompressor.getCompressor(CompressionType.UNCOMPRESSED);
    TimePageWriter pageWriter = new TimePageWriter(timeEncoder, compressor);
    PublicBAOS publicBAOS = new PublicBAOS();
    try {
      pageWriter.write(1L);
      pageWriter.write(2L);
      pageWriter.write(3L);
      // without page statistics
      assertEquals(2, pageWriter.writePageHeaderAndDataIntoBuff(publicBAOS, true));
      // total size
      assertEquals(26, publicBAOS.size());
      TimeStatistics statistics = pageWriter.getStatistics();
      assertEquals(1L, statistics.getStartTime());
      assertEquals(3L, statistics.getEndTime());
      assertEquals(3, statistics.getCount());
      ByteBuffer buffer = ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
      // uncompressedSize
      assertEquals(24, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      // compressedSize
      assertEquals(24, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      assertEquals(1L, ReadWriteIOUtils.readLong(buffer));
      assertEquals(2L, ReadWriteIOUtils.readLong(buffer));
      assertEquals(3L, ReadWriteIOUtils.readLong(buffer));
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testWritePageHeaderAndDataIntoBuffWithoutCompress2() {
    Encoder timeEncoder = new PlainEncoder(TSDataType.INT64, 0);
    ICompressor compressor = ICompressor.getCompressor(CompressionType.UNCOMPRESSED);
    TimePageWriter pageWriter = new TimePageWriter(timeEncoder, compressor);
    PublicBAOS publicBAOS = new PublicBAOS();
    try {
      pageWriter.write(1L);
      pageWriter.write(2L);
      pageWriter.write(3L);
      // with page statistics
      assertEquals(0, pageWriter.writePageHeaderAndDataIntoBuff(publicBAOS, false));
      // total size
      assertEquals(43, publicBAOS.size());
      TimeStatistics statistics = pageWriter.getStatistics();
      assertEquals(1L, statistics.getStartTime());
      assertEquals(3L, statistics.getEndTime());
      assertEquals(3, statistics.getCount());
      ByteBuffer buffer = ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
      // uncompressedSize
      assertEquals(24, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      // compressedSize
      assertEquals(24, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      TimeStatistics testStatistics =
          (TimeStatistics) TimeStatistics.deserialize(buffer, TSDataType.VECTOR);
      assertEquals(1L, testStatistics.getStartTime());
      assertEquals(3L, testStatistics.getEndTime());
      assertEquals(3, testStatistics.getCount());
      assertEquals(1L, ReadWriteIOUtils.readLong(buffer));
      assertEquals(2L, ReadWriteIOUtils.readLong(buffer));
      assertEquals(3L, ReadWriteIOUtils.readLong(buffer));
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testWritePageHeaderAndDataIntoBuffWithSnappy() {
    Encoder timeEncoder = new PlainEncoder(TSDataType.INT64, 0);
    ICompressor compressor = ICompressor.getCompressor(CompressionType.SNAPPY);
    TimePageWriter pageWriter = new TimePageWriter(timeEncoder, compressor);
    PublicBAOS publicBAOS = new PublicBAOS();
    try {
      pageWriter.write(1L);
      pageWriter.write(2L);
      pageWriter.write(3L);
      // without page statistics
      assertEquals(2, pageWriter.writePageHeaderAndDataIntoBuff(publicBAOS, true));

      // total size
      assertEquals(22, publicBAOS.size());
      TimeStatistics statistics = pageWriter.getStatistics();
      assertEquals(1L, statistics.getStartTime());
      assertEquals(3L, statistics.getEndTime());
      assertEquals(3, statistics.getCount());
      ByteBuffer compressedBuffer = ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
      // uncompressedSize
      assertEquals(24, ReadWriteForEncodingUtils.readUnsignedVarInt(compressedBuffer));
      // compressedSize
      assertEquals(20, ReadWriteForEncodingUtils.readUnsignedVarInt(compressedBuffer));
      byte[] compress = new byte[20];
      compressedBuffer.get(compress);
      byte[] uncompress = new byte[24];
      IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.SNAPPY);
      unCompressor.uncompress(compress, 0, 20, uncompress, 0);
      ByteBuffer uncompressedBuffer = ByteBuffer.wrap(uncompress);
      assertEquals(1L, ReadWriteIOUtils.readLong(uncompressedBuffer));
      assertEquals(2L, ReadWriteIOUtils.readLong(uncompressedBuffer));
      assertEquals(3L, ReadWriteIOUtils.readLong(uncompressedBuffer));
    } catch (IOException e) {
      fail();
    }
  }
}
