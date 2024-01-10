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
import org.apache.iotdb.tsfile.file.metadata.statistics.FloatStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.page.ValuePageWriter;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ValuePageWriterTest {

  @Test
  public void testWrite1() {
    Encoder valueEncoder = new PlainEncoder(TSDataType.FLOAT, 0);
    ICompressor compressor = ICompressor.getCompressor(CompressionType.UNCOMPRESSED);
    ValuePageWriter pageWriter = new ValuePageWriter(valueEncoder, compressor, TSDataType.FLOAT);
    try {
      pageWriter.write(1L, 1.0f, false);
      assertEquals(9, pageWriter.estimateMaxMemSize());
      ByteBuffer buffer1 = pageWriter.getUncompressedBytes();
      ByteBuffer buffer = ByteBuffer.wrap(buffer1.array());
      pageWriter.reset(TSDataType.FLOAT);
      assertEquals(5, pageWriter.estimateMaxMemSize());
      assertEquals(1, ReadWriteIOUtils.readInt(buffer));
      assertEquals(((byte) (1 << 7)), ReadWriteIOUtils.readByte(buffer));
      PlainDecoder decoder = new PlainDecoder();
      assertEquals(1.0f, ReadWriteIOUtils.readFloat(buffer), 0.000001f);
      assertEquals(0, buffer.remaining());
      decoder.reset();
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testWrite2() {
    Encoder valueEncoder = new PlainEncoder(TSDataType.FLOAT, 0);
    ICompressor compressor = ICompressor.getCompressor(CompressionType.UNCOMPRESSED);
    ValuePageWriter pageWriter = new ValuePageWriter(valueEncoder, compressor, TSDataType.FLOAT);
    try {
      for (int time = 1; time <= 16; time++) {
        pageWriter.write(time, (float) time, time % 4 == 0);
      }
      assertEquals(55, pageWriter.estimateMaxMemSize());
      ByteBuffer buffer1 = pageWriter.getUncompressedBytes();
      ByteBuffer buffer = ByteBuffer.wrap(buffer1.array());
      pageWriter.reset(TSDataType.FLOAT);
      assertEquals(5, pageWriter.estimateMaxMemSize());
      assertEquals(16, ReadWriteIOUtils.readInt(buffer));
      assertEquals(((byte) (0xEE)), ReadWriteIOUtils.readByte(buffer));
      assertEquals(((byte) (0xEE)), ReadWriteIOUtils.readByte(buffer));
      PlainDecoder decoder = new PlainDecoder();
      for (int value = 1; value <= 16; value++) {
        if (value % 4 != 0) {
          assertEquals((float) value, ReadWriteIOUtils.readFloat(buffer), 0.000001f);
        }
      }
      assertEquals(0, buffer.remaining());
      decoder.reset();
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testWrite3() {
    Encoder valueEncoder = new PlainEncoder(TSDataType.FLOAT, 0);
    ICompressor compressor = ICompressor.getCompressor(CompressionType.UNCOMPRESSED);
    ValuePageWriter pageWriter = new ValuePageWriter(valueEncoder, compressor, TSDataType.FLOAT);
    try {
      for (int time = 1; time <= 20; time++) {
        pageWriter.write(time, (float) time, time % 4 == 0);
      }
      assertEquals(67, pageWriter.estimateMaxMemSize());
      ByteBuffer buffer1 = pageWriter.getUncompressedBytes();
      ByteBuffer buffer = ByteBuffer.wrap(buffer1.array());
      pageWriter.reset(TSDataType.FLOAT);
      assertEquals(5, pageWriter.estimateMaxMemSize());
      assertEquals(20, ReadWriteIOUtils.readInt(buffer));
      assertEquals(((byte) (0xEE)), ReadWriteIOUtils.readByte(buffer));
      assertEquals(((byte) (0xEE)), ReadWriteIOUtils.readByte(buffer));
      assertEquals(((byte) (0xE0)), ReadWriteIOUtils.readByte(buffer));
      PlainDecoder decoder = new PlainDecoder();
      for (int value = 1; value <= 20; value++) {
        if (value % 4 != 0) {
          assertEquals((float) value, ReadWriteIOUtils.readFloat(buffer), 0.000001f);
        }
      }
      assertEquals(0, buffer.remaining());
      decoder.reset();
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testWritePageHeaderAndDataIntoBuffWithoutCompress1() {
    Encoder valueEncoder = new PlainEncoder(TSDataType.FLOAT, 0);
    ICompressor compressor = ICompressor.getCompressor(CompressionType.UNCOMPRESSED);
    ValuePageWriter pageWriter = new ValuePageWriter(valueEncoder, compressor, TSDataType.FLOAT);
    PublicBAOS publicBAOS = new PublicBAOS();
    try {
      for (int time = 1; time <= 20; time++) {
        pageWriter.write(time, (float) time, time % 4 == 0);
      }
      // without page statistics
      assertEquals(2, pageWriter.writePageHeaderAndDataIntoBuff(publicBAOS, true));
      // total size
      assertEquals(69, publicBAOS.size());
      Statistics<Float> statistics = (Statistics<Float>) pageWriter.getStatistics();
      assertEquals(1L, statistics.getStartTime());
      assertEquals(19L, statistics.getEndTime());
      assertEquals(15, statistics.getCount());
      assertEquals(1.0f, statistics.getFirstValue(), 0.000001f);
      assertEquals(19.0f, statistics.getLastValue(), 0.000001f);
      assertEquals(1.0f, statistics.getMinValue(), 0.000001f);
      assertEquals(19.0f, statistics.getMaxValue(), 0.000001f);
      assertEquals(150.0f, (float) statistics.getSumDoubleValue(), 0.000001f);

      ByteBuffer buffer = ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());

      // uncompressedSize
      assertEquals(67, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      // compressedSize
      assertEquals(67, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));

      // bitmap
      assertEquals(20, ReadWriteIOUtils.readInt(buffer));
      assertEquals(((byte) (0xEE)), ReadWriteIOUtils.readByte(buffer));
      assertEquals(((byte) (0xEE)), ReadWriteIOUtils.readByte(buffer));
      assertEquals(((byte) (0xE0)), ReadWriteIOUtils.readByte(buffer));

      for (int value = 1; value <= 20; value++) {
        if (value % 4 != 0) {
          assertEquals((float) value, ReadWriteIOUtils.readFloat(buffer), 0.000001f);
        }
      }
      assertEquals(0, buffer.remaining());
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testWritePageHeaderAndDataIntoBuffWithoutCompress2() {
    Encoder valueEncoder = new PlainEncoder(TSDataType.FLOAT, 0);
    ICompressor compressor = ICompressor.getCompressor(CompressionType.UNCOMPRESSED);
    ValuePageWriter pageWriter = new ValuePageWriter(valueEncoder, compressor, TSDataType.FLOAT);
    PublicBAOS publicBAOS = new PublicBAOS();
    try {
      for (int time = 1; time <= 20; time++) {
        pageWriter.write(time, (float) time, time % 4 == 0);
      }
      // without page statistics
      assertEquals(0, pageWriter.writePageHeaderAndDataIntoBuff(publicBAOS, false));
      // total size
      assertEquals(110, publicBAOS.size());
      Statistics<Float> statistics = (Statistics<Float>) pageWriter.getStatistics();
      assertEquals(1L, statistics.getStartTime());
      assertEquals(19L, statistics.getEndTime());
      assertEquals(15, statistics.getCount());
      assertEquals(1.0f, statistics.getFirstValue(), 0.000001f);
      assertEquals(19.0f, statistics.getLastValue(), 0.000001f);
      assertEquals(1.0f, statistics.getMinValue(), 0.000001f);
      assertEquals(19.0f, statistics.getMaxValue(), 0.000001f);
      assertEquals(150.0f, (float) statistics.getSumDoubleValue(), 0.000001f);

      ByteBuffer buffer = ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
      // uncompressedSize
      assertEquals(67, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      // compressedSize
      assertEquals(67, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));

      // Statistics
      FloatStatistics testStatistics =
          (FloatStatistics) FloatStatistics.deserialize(buffer, TSDataType.FLOAT);
      assertEquals(1L, testStatistics.getStartTime());
      assertEquals(19L, testStatistics.getEndTime());
      assertEquals(15, testStatistics.getCount());
      assertEquals(1.0f, testStatistics.getFirstValue(), 0.000001f);
      assertEquals(19.0f, testStatistics.getLastValue(), 0.000001f);
      assertEquals(1.0f, testStatistics.getMinValue(), 0.000001f);
      assertEquals(19.0f, testStatistics.getMaxValue(), 0.000001f);
      assertEquals(150.0f, (float) testStatistics.getSumDoubleValue(), 0.000001f);

      // bitmap
      assertEquals(20, ReadWriteIOUtils.readInt(buffer));
      assertEquals(((byte) (0xEE)), ReadWriteIOUtils.readByte(buffer));
      assertEquals(((byte) (0xEE)), ReadWriteIOUtils.readByte(buffer));
      assertEquals(((byte) (0xE0)), ReadWriteIOUtils.readByte(buffer));

      for (int value = 1; value <= 20; value++) {
        if (value % 4 != 0) {
          assertEquals((float) value, ReadWriteIOUtils.readFloat(buffer), 0.000001f);
        }
      }
      assertEquals(0, buffer.remaining());
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testWritePageHeaderAndDataIntoBuffWithSnappy() {
    Encoder valueEncoder = new PlainEncoder(TSDataType.FLOAT, 0);
    ICompressor compressor = ICompressor.getCompressor(CompressionType.SNAPPY);
    ValuePageWriter pageWriter = new ValuePageWriter(valueEncoder, compressor, TSDataType.FLOAT);
    PublicBAOS publicBAOS = new PublicBAOS();
    try {
      for (int time = 1; time <= 20; time++) {
        pageWriter.write(time, (float) time, time % 4 == 0);
      }
      // without page statistics
      assertEquals(2, pageWriter.writePageHeaderAndDataIntoBuff(publicBAOS, true));
      // total size
      assertEquals(72, publicBAOS.size());
      Statistics<Float> statistics = (Statistics<Float>) pageWriter.getStatistics();
      assertEquals(1L, statistics.getStartTime());
      assertEquals(19L, statistics.getEndTime());
      assertEquals(15, statistics.getCount());
      assertEquals(1.0f, statistics.getFirstValue(), 0.000001f);
      assertEquals(19.0f, statistics.getLastValue(), 0.000001f);
      assertEquals(1.0f, statistics.getMinValue(), 0.000001f);
      assertEquals(19.0f, statistics.getMaxValue(), 0.000001f);
      assertEquals(150.0f, (float) statistics.getSumDoubleValue(), 0.000001f);

      ByteBuffer buffer = ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());

      // uncompressedSize
      assertEquals(67, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
      // compressedSize
      assertEquals(70, ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));

      byte[] compress = new byte[70];
      buffer.get(compress);
      byte[] uncompress = new byte[67];
      IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.SNAPPY);
      unCompressor.uncompress(compress, 0, 70, uncompress, 0);
      ByteBuffer uncompressedBuffer = ByteBuffer.wrap(uncompress);

      // bitmap
      assertEquals(20, ReadWriteIOUtils.readInt(uncompressedBuffer));
      assertEquals(((byte) (0xEE)), ReadWriteIOUtils.readByte(uncompressedBuffer));
      assertEquals(((byte) (0xEE)), ReadWriteIOUtils.readByte(uncompressedBuffer));
      assertEquals(((byte) (0xE0)), ReadWriteIOUtils.readByte(uncompressedBuffer));

      for (int value = 1; value <= 20; value++) {
        if (value % 4 != 0) {
          assertEquals((float) value, ReadWriteIOUtils.readFloat(uncompressedBuffer), 0.000001f);
        }
      }
      assertEquals(0, uncompressedBuffer.remaining());
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }
}
