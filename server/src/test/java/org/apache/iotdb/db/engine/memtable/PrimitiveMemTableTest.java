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
package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.utils.MathUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

public class PrimitiveMemTableTest {

  double delta;

  @Before
  public void setUp() {
    delta = Math.pow(0.1, TSFileDescriptor.getInstance().getConfig().getFloatPrecision());
  }

  @Test
  public void memSeriesSortIteratorTest() throws IOException {
    TSDataType dataType = TSDataType.INT32;
    WritableMemChunk series =
        new WritableMemChunk(
            new MeasurementSchema("s1", dataType, TSEncoding.PLAIN), TVList.newList(dataType));
    int count = 1000;
    for (int i = 0; i < count; i++) {
      series.write(i, i);
    }
    IPointReader it = series.getSortedTVListForQuery().getIterator();
    int i = 0;
    while (it.hasNextTimeValuePair()) {
      Assert.assertEquals(i, it.nextTimeValuePair().getTimestamp());
      i++;
    }
    Assert.assertEquals(count, i);
  }

  @Test
  public void simpleTest() throws IOException, QueryProcessException, MetadataException {
    IMemTable memTable = new PrimitiveMemTable();
    int count = 10;
    String deviceId = "d1";
    String[] measurementId = new String[count];
    for (int i = 0; i < measurementId.length; i++) {
      measurementId[i] = "s" + i;
    }

    int dataSize = 10000;
    for (int i = 0; i < dataSize; i++) {
      memTable.write(
          deviceId,
          measurementId[0],
          new MeasurementSchema(measurementId[0], TSDataType.INT32, TSEncoding.PLAIN),
          dataSize - i - 1,
          i + 10);
    }
    for (int i = 0; i < dataSize; i++) {
      memTable.write(
          deviceId,
          measurementId[0],
          new MeasurementSchema(measurementId[0], TSDataType.INT32, TSEncoding.PLAIN),
          i,
          i);
    }
    ReadOnlyMemChunk memChunk =
        memTable.query(
            deviceId,
            measurementId[0],
            TSDataType.INT32,
            TSEncoding.RLE,
            Collections.emptyMap(),
            Long.MIN_VALUE,
            null);
    IPointReader iterator = memChunk.getPointReader();
    for (int i = 0; i < dataSize; i++) {
      iterator.hasNextTimeValuePair();
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      Assert.assertEquals(i, timeValuePair.getTimestamp());
      Assert.assertEquals(i, timeValuePair.getValue().getValue());
    }
  }

  private void write(
      IMemTable memTable,
      String deviceId,
      String sensorId,
      TSDataType dataType,
      TSEncoding encoding,
      int size)
      throws IOException, QueryProcessException, MetadataException {
    TimeValuePair[] ret = genTimeValuePair(size, dataType);

    for (TimeValuePair aRet : ret) {
      memTable.write(
          deviceId,
          sensorId,
          new MeasurementSchema(sensorId, dataType, encoding),
          aRet.getTimestamp(),
          aRet.getValue().getValue());
    }
    IPointReader tvPair =
        memTable
            .query(
                deviceId,
                sensorId,
                dataType,
                encoding,
                Collections.emptyMap(),
                Long.MIN_VALUE,
                null)
            .getPointReader();
    Arrays.sort(ret);
    TimeValuePair last = null;
    for (int i = 0; i < ret.length; i++) {
      while (last != null && (i < ret.length && last.getTimestamp() == ret[i].getTimestamp())) {
        i++;
      }
      if (i >= ret.length) {
        break;
      }
      TimeValuePair pair = ret[i];
      last = pair;
      tvPair.hasNextTimeValuePair();
      TimeValuePair next = tvPair.nextTimeValuePair();
      Assert.assertEquals(pair.getTimestamp(), next.getTimestamp());
      if (dataType == TSDataType.DOUBLE) {
        Assert.assertEquals(
            pair.getValue().getDouble(),
            MathUtils.roundWithGivenPrecision(next.getValue().getDouble()),
            delta);
      } else if (dataType == TSDataType.FLOAT) {
        float expected = pair.getValue().getFloat();
        float actual = MathUtils.roundWithGivenPrecision(next.getValue().getFloat());
        Assert.assertEquals(expected, actual, delta + Float.MIN_NORMAL);
      } else {
        Assert.assertEquals(pair.getValue(), next.getValue());
      }
    }
  }

  @Test
  public void testFloatType() throws IOException, QueryProcessException, MetadataException {
    IMemTable memTable = new PrimitiveMemTable();
    String deviceId = "d1";
    int size = 100;
    write(memTable, deviceId, "s1", TSDataType.FLOAT, TSEncoding.RLE, size);
  }

  @Test
  public void testAllType() throws IOException, QueryProcessException, MetadataException {
    IMemTable memTable = new PrimitiveMemTable();
    int count = 10;
    String deviceId = "d1";
    String[] measurementId = new String[count];
    for (int i = 0; i < measurementId.length; i++) {
      measurementId[i] = "s" + i;
    }
    int index = 0;

    int size = 10000;
    write(memTable, deviceId, measurementId[index++], TSDataType.BOOLEAN, TSEncoding.RLE, size);
    write(memTable, deviceId, measurementId[index++], TSDataType.INT32, TSEncoding.RLE, size);
    write(memTable, deviceId, measurementId[index++], TSDataType.INT64, TSEncoding.RLE, size);
    write(memTable, deviceId, measurementId[index++], TSDataType.FLOAT, TSEncoding.RLE, size);
    write(memTable, deviceId, measurementId[index++], TSDataType.DOUBLE, TSEncoding.RLE, size);
    write(memTable, deviceId, measurementId[index++], TSDataType.TEXT, TSEncoding.PLAIN, size);
  }

  private TimeValuePair[] genTimeValuePair(int size, TSDataType dataType) {
    TimeValuePair[] ret = new TimeValuePair[size];
    Random rand = new Random();
    for (int i = 0; i < size; i++) {
      switch (dataType) {
        case BOOLEAN:
          ret[i] = new TimeValuePair(rand.nextLong(), TsPrimitiveType.getByType(dataType, true));
          break;
        case INT32:
          ret[i] =
              new TimeValuePair(
                  rand.nextLong(), TsPrimitiveType.getByType(dataType, rand.nextInt()));
          break;
        case INT64:
          ret[i] =
              new TimeValuePair(
                  rand.nextLong(), TsPrimitiveType.getByType(dataType, rand.nextLong()));
          break;
        case FLOAT:
          ret[i] =
              new TimeValuePair(
                  rand.nextLong(), TsPrimitiveType.getByType(dataType, rand.nextFloat()));
          break;
        case DOUBLE:
          ret[i] =
              new TimeValuePair(
                  rand.nextLong(), TsPrimitiveType.getByType(dataType, rand.nextDouble()));
          break;
        case TEXT:
          ret[i] =
              new TimeValuePair(
                  rand.nextLong(),
                  TsPrimitiveType.getByType(dataType, new Binary("a" + rand.nextDouble())));
          break;
        default:
          throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
      }
    }
    return ret;
  }
}
