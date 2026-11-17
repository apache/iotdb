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

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

/**
 * {@link TabletEncoder} must compress data that does not shrink, whatever {@link
 * ICompressor#getMaxBytesForCompression} estimates for it, and keep writing the format the DataNode
 * reads: the compressed bytes followed by the uncompressed length as a big-endian int.
 */
public class TabletEncoderCompressionTest {

  private static final List<TSEncoding> ALL_PLAIN =
      Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN);

  @Test
  public void gzipCompressesNoisyDoublesEncodedWithGorilla() throws IOException {
    int rows = 10;
    Tablet tablet = tablet(TSDataType.DOUBLE, rows);
    Random random = new Random(1);
    double measurement = 20.0;
    double[] expected = new double[rows];
    for (int row = 0; row < rows; row++) {
      // a slowly drifting, noisy measurement
      measurement += (random.nextDouble() - 0.5) * 0.1;
      expected[row] = measurement + random.nextGaussian() * 0.01;
      tablet.addTimestamp(row, row);
      tablet.addValue(row, 0, expected[row]);
    }
    List<TSEncoding> encodings = Arrays.asList(TSEncoding.TS_2DIFF, TSEncoding.GORILLA);

    ByteBuffer uncompressed =
        roundTrip(CompressionType.GZIP, encodings, encoder -> encoder.encodeValues(tablet));

    Decoder decoder = Decoder.getDecoderByType(TSEncoding.GORILLA, TSDataType.DOUBLE);
    for (int row = 0; row < rows; row++) {
      Assert.assertEquals(expected[row], decoder.readDouble(uncompressed), 0);
    }
  }

  @Test
  public void gzipCompressesRandomLongsEncodedWithPlain() throws IOException {
    int rows = 1000;
    Tablet tablet = randomLongTablet(rows, new Random(2));

    ByteBuffer uncompressed =
        roundTrip(CompressionType.GZIP, ALL_PLAIN, encoder -> encoder.encodeValues(tablet));

    long[] expected = (long[]) tablet.getValues()[0];
    Decoder decoder = Decoder.getDecoderByType(TSEncoding.PLAIN, TSDataType.INT64);
    for (int row = 0; row < rows; row++) {
      Assert.assertEquals(expected[row], decoder.readLong(uncompressed));
    }
  }

  @Test
  public void gzipCompressesRandomTimestampsEncodedWithPlain() throws IOException {
    int rows = 1000;
    Tablet tablet = randomLongTablet(rows, new Random(3));

    ByteBuffer uncompressed =
        roundTrip(CompressionType.GZIP, ALL_PLAIN, encoder -> encoder.encodeTime(tablet));

    Decoder decoder = Decoder.getDecoderByType(TSEncoding.PLAIN, TSDataType.INT64);
    for (int row = 0; row < rows; row++) {
      Assert.assertEquals(tablet.getTimestamp(row), decoder.readLong(uncompressed));
    }
  }

  /**
   * Walks across the sizes where the GZIP output first outgrows the estimate. Output that is at
   * most 4 bytes over it still fits an array with room for the length, but not together with the
   * length, so this covers both ways of running out of room.
   */
  @Test
  public void gzipCompressesIncompressibleDataOfEverySmallSize() throws IOException {
    Random random = new Random(4);
    for (int rows = 1; rows <= 64; rows++) {
      Tablet tablet = randomLongTablet(rows, random);
      roundTrip(CompressionType.GZIP, ALL_PLAIN, encoder -> encoder.encodeValues(tablet));
    }
  }

  /** LZMA2 only estimates 100 extra bytes, while its overhead grows with the input. */
  @Test
  public void lzma2CompressesLargeIncompressibleData() throws IOException {
    Tablet tablet = randomLongTablet(150_000, new Random(5));
    roundTrip(CompressionType.LZMA2, ALL_PLAIN, encoder -> encoder.encodeValues(tablet));
  }

  @Test
  public void everyCodecRoundTripsIncompressibleAndCompressibleData() throws IOException {
    for (CompressionType type :
        Arrays.asList(
            CompressionType.SNAPPY,
            CompressionType.LZ4,
            CompressionType.GZIP,
            CompressionType.ZSTD,
            CompressionType.LZMA2)) {
      Tablet incompressible = randomLongTablet(1000, new Random(6));
      roundTrip(type, ALL_PLAIN, encoder -> encoder.encodeValues(incompressible));
      roundTrip(type, ALL_PLAIN, encoder -> encoder.encodeTime(incompressible));

      Tablet compressible = repeatedLongTablet(1000);
      roundTrip(type, ALL_PLAIN, encoder -> encoder.encodeValues(compressible));
      roundTrip(type, ALL_PLAIN, encoder -> encoder.encodeTime(compressible));
    }
  }

  /** Pins the format on data for which the estimate has always been large enough. */
  @Test
  public void compressedBytesStayTheSameAsWithEstimateSizedBuffer() throws IOException {
    for (CompressionType type :
        Arrays.asList(CompressionType.SNAPPY, CompressionType.LZ4, CompressionType.ZSTD)) {
      assertSameAsEstimateSizedCompression(type, randomLongTablet(1000, new Random(7)));
      assertSameAsEstimateSizedCompression(type, repeatedLongTablet(1000));
    }
    assertSameAsEstimateSizedCompression(CompressionType.GZIP, repeatedLongTablet(1000));
    assertSameAsEstimateSizedCompression(CompressionType.LZMA2, repeatedLongTablet(1000));
  }

  private static void assertSameAsEstimateSizedCompression(CompressionType type, Tablet tablet)
      throws IOException {
    byte[] raw =
        sentBytes(new TabletEncoder(CompressionType.UNCOMPRESSED, ALL_PLAIN).encodeValues(tablet));
    ICompressor compressor = ICompressor.getCompressor(type);
    byte[] compressed = new byte[compressor.getMaxBytesForCompression(raw.length) + 4];
    int compressedLength = compressor.compress(raw, 0, raw.length, compressed);
    ByteBuffer.wrap(compressed, compressedLength, 4).putInt(raw.length);

    Assert.assertArrayEquals(
        type.toString(),
        Arrays.copyOf(compressed, compressedLength + 4),
        sentBytes(new TabletEncoder(type, ALL_PLAIN).encodeValues(tablet)));
  }

  /**
   * Encodes a column with the given codec, decompresses it the way {@code TabletDecoder} does on
   * the DataNode, and checks that this gives back the uncompressed encoding of the column.
   *
   * @return the decompressed column
   */
  private static ByteBuffer roundTrip(
      CompressionType type, List<TSEncoding> encodings, Function<TabletEncoder, ByteBuffer> column)
      throws IOException {
    byte[] raw =
        sentBytes(column.apply(new TabletEncoder(CompressionType.UNCOMPRESSED, encodings)));
    ByteBuffer received =
        ByteBuffer.wrap(sentBytes(column.apply(new TabletEncoder(type, encodings))));

    int uncompressedLength = received.getInt(received.limit() - 4);
    byte[] uncompressed = new byte[uncompressedLength];
    IUnCompressor.getUnCompressor(type)
        .uncompress(
            received.array(),
            received.arrayOffset() + received.position(),
            received.remaining() - 4,
            uncompressed,
            0);

    Assert.assertArrayEquals(type.toString(), raw, uncompressed);
    return ByteBuffer.wrap(uncompressed);
  }

  /** Thrift sends what is between the position and the limit of the buffer. */
  private static byte[] sentBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  private static Tablet randomLongTablet(int rows, Random random) {
    Tablet tablet = tablet(TSDataType.INT64, rows);
    for (int row = 0; row < rows; row++) {
      tablet.addTimestamp(row, random.nextLong());
      tablet.addValue(row, 0, random.nextLong());
    }
    return tablet;
  }

  private static Tablet repeatedLongTablet(int rows) {
    Tablet tablet = tablet(TSDataType.INT64, rows);
    for (int row = 0; row < rows; row++) {
      tablet.addTimestamp(row, 1_700_000_000_000L);
      tablet.addValue(row, 0, 42L);
    }
    return tablet;
  }

  private static Tablet tablet(TSDataType type, int rows) {
    List<IMeasurementSchema> schemas = Collections.singletonList(new MeasurementSchema("s0", type));
    return new Tablet("root.sg.d1", schemas, rows);
  }
}
