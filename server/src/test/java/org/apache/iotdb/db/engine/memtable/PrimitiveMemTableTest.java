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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import org.apache.iotdb.db.utils.MathUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PrimitiveMemTableTest {

  double delta;

  @Before
  public void setUp() {
    delta = Math.pow(0.1, TSFileDescriptor.getInstance().getConfig().getFloatPrecision());
  }

  @Test
  public void memSeriesCloneTest() {
    TSDataType dataType = TSDataType.INT32;
    WritableMemChunk series = new WritableMemChunk(dataType, TVList.newList(dataType));
    int count = 1000;
    for (int i = 0; i < count; i++) {
      series.write(i, i);
    }
    Iterator<TimeValuePair> it = series.getSortedTimeValuePairList().iterator();
    int i = 0;
    while (it.hasNext()) {
      Assert.assertEquals(i, it.next().getTimestamp());
      i++;
    }
    Assert.assertEquals(count, i);
  }

  @Test
  public void simpleTest() {
    IMemTable memTable = new PrimitiveMemTable("sg");
    int count = 10;
    String deviceId = "d1";
    String measurementId[] = new String[count];
    for (int i = 0; i < measurementId.length; i++) {
      measurementId[i] = "s" + i;
    }

    int dataSize = 10000;
    for (int i = 0; i < dataSize; i++) {
      memTable.write(deviceId, measurementId[0], TSDataType.INT32, dataSize - i - 1,
          i + 10);
    }
    for (int i = 0; i < dataSize; i++) {
      memTable.write(deviceId, measurementId[0], TSDataType.INT32, i, i);
    }
    Iterator<TimeValuePair> tvPair = memTable
        .query(deviceId, measurementId[0], TSDataType.INT32, Collections.emptyMap(), Long.MIN_VALUE)
        .getSortedTimeValuePairList().iterator();
    for (int i = 0; i < dataSize; i++) {
      TimeValuePair timeValuePair = tvPair.next();
      Assert.assertEquals(i, timeValuePair.getTimestamp());
      Assert.assertEquals(i, timeValuePair.getValue().getValue());
    }
  }

  private void write(IMemTable memTable, String deviceId, String sensorId, TSDataType dataType,
      int size) {
    TimeValuePair[] ret = genTimeValuePair(size, dataType);

    for (int i = 0; i < ret.length; i++) {
      memTable.write(deviceId, sensorId, dataType, ret[i].getTimestamp(),
          ret[i].getValue().getValue());
    }
    Iterator<TimeValuePair> tvPair = memTable
        .query(deviceId, sensorId, dataType, Collections.emptyMap(), Long.MIN_VALUE)
        .getSortedTimeValuePairList()
        .iterator();
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
      TimeValuePair next = tvPair.next();
      Assert.assertEquals(pair.getTimestamp(), next.getTimestamp());
      if (dataType == TSDataType.DOUBLE) {
        Assert.assertEquals(pair.getValue().getDouble(),
            MathUtils.roundWithGivenPrecision(next.getValue().getDouble()), delta);
      } else if (dataType == TSDataType.FLOAT) {
        float expected = pair.getValue().getFloat();
        float actual = MathUtils.roundWithGivenPrecision(next.getValue().getFloat());
        Assert.assertEquals(expected, actual, delta+ Float.MIN_NORMAL);
      } else {
        Assert.assertEquals(pair.getValue(), next.getValue());
      }
    }
  }

  @Test
  public void testFloatType() {
    IMemTable memTable = new PrimitiveMemTable("sg");
    String deviceId = "d1";
    int size = 100;
    write(memTable, deviceId, "s1", TSDataType.FLOAT, size);
  }

  @Test
  public void testAllType() {
    IMemTable memTable = new PrimitiveMemTable("sg");
    int count = 10;
    String deviceId = "d1";
    String measurementId[] = new String[count];
    for (int i = 0; i < measurementId.length; i++) {
      measurementId[i] = "s" + i;
    }
    int index = 0;

    int size = 10000;
    write(memTable, deviceId, measurementId[index++], TSDataType.BOOLEAN, size);
    write(memTable, deviceId, measurementId[index++], TSDataType.INT32, size);
    write(memTable, deviceId, measurementId[index++], TSDataType.INT64, size);
    write(memTable, deviceId, measurementId[index++], TSDataType.FLOAT, size);
    write(memTable, deviceId, measurementId[index++], TSDataType.DOUBLE, size);
    write(memTable, deviceId, measurementId[index++], TSDataType.TEXT, size);
  }

  private TimeValuePair[] genTimeValuePair(int size, TSDataType dataType) {
    TimeValuePair[] ret = new TimeValuePair[size];
    Random rand = new Random();
    for (int i = 0; i < size; i++) {
      switch (dataType) {
        case BOOLEAN:
          ret[i] = new TimeValuePairInMemTable(rand.nextLong(),
              TsPrimitiveType.getByType(dataType, true));
          break;
        case INT32:
          ret[i] = new TimeValuePairInMemTable(rand.nextLong(),
              TsPrimitiveType.getByType(dataType, rand.nextInt()));
          break;
        case INT64:
          ret[i] = new TimeValuePairInMemTable(rand.nextLong(),
              TsPrimitiveType.getByType(dataType, rand.nextLong()));
          break;
        case FLOAT:
          ret[i] = new TimeValuePairInMemTable(rand.nextLong(),
              TsPrimitiveType.getByType(dataType, rand.nextFloat()));
          break;
        case DOUBLE:
          ret[i] = new TimeValuePairInMemTable(rand.nextLong(),
              TsPrimitiveType.getByType(dataType, rand.nextDouble()));
          break;
        case TEXT:
          ret[i] = new TimeValuePairInMemTable(rand.nextLong(),
              TsPrimitiveType.getByType(dataType, new Binary("a" + rand.nextDouble())));
          break;
        default:
          throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
      }
    }
    return ret;
  }
}
