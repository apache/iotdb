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
package org.apache.iotdb.tsfile.read.reader;

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.DoublePrecisionDecoderV1;
import org.apache.iotdb.tsfile.encoding.decoder.IntRleDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.LongRleDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.PlainDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.SinglePrecisionDecoderV1;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.DoublePrecisionEncoderV1;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.IntRleEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.LongRleEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.PlainEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.SinglePrecisionEncoderV1;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.page.PageWriter;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PageReaderTest {

  private static final int POINTS_COUNT_IN_ONE_PAGE = 1000000;

  @Test
  public void testLong() {

    LoopWriteReadTest test =
        new LoopWriteReadTest(
            "Test INT64",
            new LongRleEncoder(),
            new LongRleDecoder(),
            TSDataType.INT64,
            POINTS_COUNT_IN_ONE_PAGE) {
          @Override
          public Object generateValueByIndex(int i) {
            return Long.MAX_VALUE - i;
          }
        };
    test.test(TSDataType.INT64);
  }

  @Test
  public void testBoolean() {
    LoopWriteReadTest test =
        new LoopWriteReadTest(
            "Test Boolean",
            new IntRleEncoder(),
            new IntRleDecoder(),
            TSDataType.BOOLEAN,
            POINTS_COUNT_IN_ONE_PAGE) {
          @Override
          public Object generateValueByIndex(int i) {
            return i % 3 == 0;
          }
        };
    test.test(TSDataType.BOOLEAN);
  }

  @Test
  public void testInt() {
    LoopWriteReadTest test =
        new LoopWriteReadTest(
            "Test INT32",
            new IntRleEncoder(),
            new IntRleDecoder(),
            TSDataType.INT32,
            POINTS_COUNT_IN_ONE_PAGE) {
          @Override
          public Object generateValueByIndex(int i) {
            return i;
          }
        };
    test.test(TSDataType.INT32);
  }

  @Test
  public void testFloat() {
    LoopWriteReadTest test =
        new LoopWriteReadTest(
            "Test FLOAT",
            new SinglePrecisionEncoderV1(),
            new SinglePrecisionDecoderV1(),
            TSDataType.FLOAT,
            POINTS_COUNT_IN_ONE_PAGE) {
          @Override
          public Object generateValueByIndex(int i) {
            return (float) i / 10 - (float) i / 100;
          }
        };
    test.test(TSDataType.FLOAT);

    LoopWriteReadTest test2 =
        new LoopWriteReadTest(
            "Test FLOAT",
            new SinglePrecisionEncoderV1(),
            new SinglePrecisionDecoderV1(),
            TSDataType.FLOAT,
            POINTS_COUNT_IN_ONE_PAGE) {
          @Override
          public Object generateValueByIndex(int i) {
            return (float) i / 100 - (float) i / 10;
          }
        };
    test2.test(TSDataType.FLOAT);
  }

  @Test
  public void testDouble() {
    LoopWriteReadTest test =
        new LoopWriteReadTest(
            "Test Double",
            new DoublePrecisionEncoderV1(),
            new DoublePrecisionDecoderV1(),
            TSDataType.DOUBLE,
            POINTS_COUNT_IN_ONE_PAGE) {
          @Override
          public Object generateValueByIndex(int i) {
            return (double) i / 10 - (double) i / 100;
          }
        };
    test.test(TSDataType.DOUBLE);

    LoopWriteReadTest test2 =
        new LoopWriteReadTest(
            "Test Double",
            new DoublePrecisionEncoderV1(),
            new DoublePrecisionDecoderV1(),
            TSDataType.DOUBLE,
            POINTS_COUNT_IN_ONE_PAGE) {
          @Override
          public Object generateValueByIndex(int i) {
            return (double) i / 1000 - (double) i / 100;
          }
        };
    test2.test(TSDataType.DOUBLE);
  }

  @Test
  public void testBinary() {
    LoopWriteReadTest test =
        new LoopWriteReadTest(
            "Test Double",
            new PlainEncoder(TSDataType.TEXT, 1000),
            new PlainDecoder(),
            TSDataType.TEXT,
            POINTS_COUNT_IN_ONE_PAGE) {
          @Override
          public Object generateValueByIndex(int i) {
            return new Binary("TEST TEXT" + i);
          }
        };
    test.test(TSDataType.TEXT);
  }

  private abstract static class LoopWriteReadTest {

    private Encoder encoder;
    private Decoder decoder;
    private TSDataType dataType;
    private PageWriter pageWriter;
    private String name;
    private int count;

    public LoopWriteReadTest(
        String name, Encoder encoder, Decoder decoder, TSDataType dataType, int count) {
      this.name = name;
      this.encoder = encoder;
      this.decoder = decoder;
      this.dataType = dataType;
      this.count = count;
    }

    public void test(TSDataType dataType) {
      try {
        pageWriter = new PageWriter();
        pageWriter.setTimeEncoder(new DeltaBinaryEncoder.LongDeltaEncoder());
        pageWriter.setValueEncoder(this.encoder);
        pageWriter.initStatistics(dataType);
        writeData();

        ByteBuffer page = ByteBuffer.wrap(pageWriter.getUncompressedBytes().array());

        PageReader pageReader =
            new PageReader(
                page, dataType, decoder, new DeltaBinaryDecoder.LongDeltaDecoder(), null);

        int index = 0;
        BatchData data = pageReader.getAllSatisfiedPageData();
        Assert.assertNotNull(data);

        while (data.hasCurrent()) {
          Assert.assertEquals(Long.valueOf(index), (Long) data.currentTime());
          Assert.assertEquals(generateValueByIndex(index), data.currentValue());
          data.next();
          index++;
        }
        Assert.assertEquals(count, index);
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail("Fail when executing test: [" + name + "]");
      }
    }

    public void testDelete(TSDataType dataType) {
      try {
        pageWriter = new PageWriter();
        pageWriter.setTimeEncoder(new DeltaBinaryEncoder.LongDeltaEncoder());
        pageWriter.setValueEncoder(this.encoder);
        pageWriter.initStatistics(dataType);
        writeData();

        ByteBuffer page = ByteBuffer.wrap(pageWriter.getUncompressedBytes().array());

        PageReader pageReader =
            new PageReader(
                page, dataType, decoder, new DeltaBinaryDecoder.LongDeltaDecoder(), null);

        int index = 0;
        List<TimeRange> deleteIntervals = new ArrayList<>();
        deleteIntervals.add(new TimeRange(5, 10));
        deleteIntervals.add(new TimeRange(20, 30));
        deleteIntervals.add(new TimeRange(50, 70));
        pageReader.setDeleteIntervalList(deleteIntervals);
        BatchData data = pageReader.getAllSatisfiedPageData();
        Assert.assertNotNull(data);

        for (TimeRange range : pageReader.getDeleteIntervalList()) {
          while (data.hasCurrent()) {
            Assert.assertEquals(Long.valueOf(index), (Long) data.currentTime());
            Assert.assertEquals(generateValueByIndex(index), data.currentValue());
            data.next();
            index++;
            if (index == range.getMin()) {
              index = (int) (range.getMax() + 1);
              break;
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail("Fail when executing test: [" + name + "]");
      }
    }

    private void writeData() {
      for (int i = 0; i < count; i++) {
        switch (dataType) {
          case BOOLEAN:
            pageWriter.write(i, (Boolean) generateValueByIndex(i));
            break;
          case INT32:
            pageWriter.write(i, (Integer) generateValueByIndex(i));
            break;
          case INT64:
            pageWriter.write(i, (Long) generateValueByIndex(i));
            break;
          case FLOAT:
            pageWriter.write(i, (Float) generateValueByIndex(i));
            break;
          case DOUBLE:
            pageWriter.write(i, (Double) generateValueByIndex(i));
            break;
          case TEXT:
            pageWriter.write(i, (Binary) generateValueByIndex(i));
            break;
        }
      }
    }

    public abstract Object generateValueByIndex(int i);
  }

  @Test
  public void testPageDelete() {
    LoopWriteReadTest test =
        new LoopWriteReadTest(
            "Test INT64", new LongRleEncoder(), new LongRleDecoder(), TSDataType.INT64, 100) {
          @Override
          public Object generateValueByIndex(int i) {
            return Long.MAX_VALUE - i;
          }
        };
    test.testDelete(TSDataType.INT64);
  }
}
