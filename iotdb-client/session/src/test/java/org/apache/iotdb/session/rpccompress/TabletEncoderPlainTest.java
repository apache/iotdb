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

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.PlainEncoder;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * The PLAIN path of {@link TabletEncoder} must write exactly the bytes of the reference layout: the
 * generic {@link PlainEncoder} for the fixed-width types (including the "repeat the last non-null
 * value" rule and the canonical NaN), fixed-length ints for INT32 and DATE, length-prefixed bytes
 * for the binary types, followed by the bit maps. Every codec must decompress to these bytes.
 */
public class TabletEncoderPlainTest {

  private static final TSDataType[] TYPES = {
    TSDataType.INT64,
    TSDataType.TIMESTAMP,
    TSDataType.DOUBLE,
    TSDataType.FLOAT,
    TSDataType.BOOLEAN,
    TSDataType.INT32,
    TSDataType.DATE,
    TSDataType.TEXT,
    TSDataType.STRING,
    TSDataType.BLOB
  };

  // NaNs whose bits differ from Double.NaN / Float.NaN; PlainEncoder writes the canonical bits
  private static final double NON_CANONICAL_DOUBLE_NAN =
      Double.longBitsToDouble(0x7ff8000000000001L);
  private static final float NON_CANONICAL_FLOAT_NAN = Float.intBitsToFloat(0x7fc00001);

  @Test
  public void plainPathMatchesTheReferenceForEveryCodec() throws IOException {
    for (CompressionType compressionType : CompressionType.values()) {
      Random random = new Random(42);
      for (int iteration = 0; iteration < 100; iteration++) {
        // TabletEncoder sizes GZIP output too small for noisy data (a separate, existing issue),
        // so GZIP gets data that compresses well
        boolean compressible = compressionType == CompressionType.GZIP;
        Tablet tablet = randomTablet(random, iteration, compressible);
        TabletEncoder encoder =
            new TabletEncoder(
                compressionType, Collections.nCopies(TYPES.length + 1, TSEncoding.PLAIN));
        String context = compressionType + ", iteration " + iteration;

        Assert.assertArrayEquals(
            "time column, " + context,
            expectedTime(tablet),
            uncompress(compressionType, encoder.encodeTime(tablet)));
        Assert.assertArrayEquals(
            "value columns and bit maps, " + context,
            expectedValues(tablet),
            uncompress(compressionType, encoder.encodeValues(tablet)));
      }
    }
  }

  /**
   * A table-model tablet built with the raw constructor may have no value array for a TAG column
   * that is entirely null. Encoding must not read it, whatever the encodings.
   */
  @Test
  public void allNullBinaryColumnWithoutValueArray() {
    int rows = 10;
    List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("tag", TSDataType.STRING),
            new MeasurementSchema("value", TSDataType.INT64));
    List<ColumnCategory> categories = Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);
    long[] timestamps = new long[rows];
    long[] longs = new long[rows];
    for (int row = 0; row < rows; row++) {
      timestamps[row] = row;
      longs[row] = row * 10L;
    }
    BitMap allNull = new BitMap(rows);
    allNull.markAll();

    Tablet withoutArray =
        new Tablet(
            "table1",
            schemas,
            categories,
            timestamps,
            new Object[] {null, longs},
            new BitMap[] {allNull, null},
            rows);
    Tablet withArray =
        new Tablet(
            "table1",
            schemas,
            categories,
            timestamps,
            new Object[] {new Binary[rows], longs},
            new BitMap[] {allNull, null},
            rows);

    for (TSEncoding valueEncoding : new TSEncoding[] {TSEncoding.PLAIN, TSEncoding.TS_2DIFF}) {
      TabletEncoder encoder =
          new TabletEncoder(
              CompressionType.UNCOMPRESSED,
              Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN, valueEncoding));
      Assert.assertEquals(
          "values, " + valueEncoding,
          encoder.encodeValues(withArray),
          encoder.encodeValues(withoutArray));
    }
  }

  private static byte[] expectedTime(Tablet tablet) {
    PublicBAOS expected = new PublicBAOS();
    Encoder timeEncoder = new PlainEncoder(TSDataType.INT64, 0);
    for (int row = 0; row < tablet.getRowSize(); row++) {
      timeEncoder.encode(tablet.getTimestamp(row), expected);
    }
    return Arrays.copyOf(expected.getBuf(), expected.size());
  }

  private static byte[] expectedValues(Tablet tablet) throws IOException {
    PublicBAOS expected = new PublicBAOS();
    for (int column = 0; column < TYPES.length; column++) {
      TSDataType type = TYPES[column];
      Object values = tablet.getValues()[column];
      for (int row = 0; row < tablet.getRowSize(); row++) {
        boolean isNull = tablet.isNull(row, column);
        switch (type) {
          case INT32:
            ReadWriteIOUtils.write(((int[]) values)[row], expected);
            break;
          case DATE:
            ReadWriteIOUtils.write(
                isNull ? 0 : DateUtils.parseDateExpressionToInt(((LocalDate[]) values)[row]),
                expected);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            ReadWriteIOUtils.write(
                isNull ? Binary.EMPTY_VALUE : ((Binary[]) values)[row], expected);
            break;
          default:
            // the fixed-width types are checked column-wise against PlainEncoder below
            break;
        }
      }
      if (!type.isBinary() && type != TSDataType.INT32 && type != TSDataType.DATE) {
        SessionUtils.encodeValue(type, tablet, column, new PlainEncoder(type, 0), expected);
      }
    }
    for (BitMap bitMap : tablet.getBitMaps()) {
      boolean columnHasNull = bitMap != null && !bitMap.isAllUnmarked(tablet.getRowSize());
      expected.write(BytesUtils.boolToByte(columnHasNull));
      if (columnHasNull) {
        expected.write(bitMap.getTruncatedByteArray(tablet.getRowSize()));
      }
    }
    return Arrays.copyOf(expected.getBuf(), expected.size());
  }

  private static Tablet randomTablet(Random random, int iteration, boolean compressible) {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    for (int column = 0; column < TYPES.length; column++) {
      schemas.add(new MeasurementSchema("s" + column, TYPES[column]));
    }
    int rows = 1 + random.nextInt(300);
    Tablet tablet = new Tablet("root.sg.d1", schemas, rows);
    for (int row = 0; row < rows; row++) {
      tablet.addTimestamp(row, compressible ? row : random.nextLong());
      for (int column = 0; column < TYPES.length; column++) {
        if (random.nextInt(5) == 0) {
          continue; // leave a null
        }
        int small = random.nextInt(4);
        switch (TYPES[column]) {
          case INT64:
          case TIMESTAMP:
            tablet.addValue(row, column, compressible ? (long) small : random.nextLong());
            break;
          case DOUBLE:
            tablet.addValue(row, column, randomDouble(random, iteration, compressible, small));
            break;
          case FLOAT:
            tablet.addValue(row, column, randomFloat(random, iteration, compressible, small));
            break;
          case BOOLEAN:
            tablet.addValue(row, column, random.nextBoolean());
            break;
          case INT32:
            tablet.addValue(row, column, compressible ? small : random.nextInt());
            break;
          case DATE:
            tablet.addValue(row, column, LocalDate.of(2000 + small, 1 + random.nextInt(12), 1));
            break;
          case TEXT:
          case STRING:
          case BLOB:
            tablet.addValue(
                row,
                column,
                ("v" + (compressible ? small : random.nextInt())).getBytes(StandardCharsets.UTF_8));
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }
    return tablet;
  }

  private static double randomDouble(
      Random random, int iteration, boolean compressible, int small) {
    if (iteration % 7 == 0 && small == 0) {
      return random.nextBoolean() ? Double.NaN : NON_CANONICAL_DOUBLE_NAN;
    }
    return compressible ? small : random.nextGaussian();
  }

  private static float randomFloat(Random random, int iteration, boolean compressible, int small) {
    if (iteration % 7 == 0 && small == 0) {
      return random.nextBoolean() ? Float.NaN : NON_CANONICAL_FLOAT_NAN;
    }
    return compressible ? small : random.nextFloat() - 0.5f;
  }

  /** Undoes TabletEncoder's compression: the compressed bytes, then the uncompressed size. */
  private static byte[] uncompress(CompressionType compressionType, ByteBuffer buffer)
      throws IOException {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    if (compressionType == CompressionType.UNCOMPRESSED) {
      return bytes;
    }
    int uncompressedSize =
        ByteBuffer.wrap(bytes, bytes.length - Integer.BYTES, Integer.BYTES).getInt();
    byte[] uncompressed = new byte[uncompressedSize];
    int length =
        IUnCompressor.getUnCompressor(compressionType)
            .uncompress(bytes, 0, bytes.length - Integer.BYTES, uncompressed, 0);
    Assert.assertEquals(compressionType + " uncompressed size", uncompressedSize, length);
    return uncompressed;
  }
}
