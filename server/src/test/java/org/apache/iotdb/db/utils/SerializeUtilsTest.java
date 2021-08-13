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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class SerializeUtilsTest {
  @Test
  public void serdesStringTest() {
    String str = "abcd%+/123\n\t";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serialize(str, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    Assert.assertEquals(str, SerializeUtils.deserializeString(buffer));
  }

  @Test
  public void serdesStringListTest() {
    List<String> slist = Arrays.asList("abc", "123", "y87@", "9+&d\n");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeStringList(slist, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    Assert.assertEquals(slist, SerializeUtils.deserializeStringList(buffer));
  }

  @Test
  public void serdesIntListTest() {
    List<Integer> intlist = Arrays.asList(12, 34, 567, 8910);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeIntList(intlist, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    List<Integer> anotherIntlist = new ArrayList<>();
    SerializeUtils.deserializeIntList(anotherIntlist, buffer);
    Assert.assertEquals(intlist, anotherIntlist);
  }

  @Test
  public void serdesIntSetTest() {
    List<Integer> intlist = Arrays.asList(12, 34, 567, 8910);
    Set<Integer> intSet = new TreeSet<>(intlist);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeIntSet(intSet, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    Set<Integer> anotherIntlist = new TreeSet<>();
    SerializeUtils.deserializeIntSet(anotherIntlist, buffer);
    Assert.assertEquals(intSet, anotherIntlist);
  }

  @Test
  public void serdesINT32BatchDataTest() {
    BatchData batchData = new BatchData(TSDataType.INT32);
    int ivalue = 0;
    for (long time = 0; time < 10; time++) {
      batchData.putAnObject(time, ivalue);
      ivalue++;
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(batchData, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    BatchData anotherBatch = SerializeUtils.deserializeBatchData(buffer);
    while (batchData.hasCurrent()) {
      Assert.assertEquals(batchData.currentValue(), anotherBatch.currentValue());
      batchData.next();
      anotherBatch.next();
    }
  }

  @Test
  public void serdesINT64BatchDataTest() {
    BatchData batchData = new BatchData(TSDataType.INT64);
    long lvalue = 0;
    for (long time = 0; time < 10; time++) {
      batchData.putAnObject(time, lvalue);
      lvalue++;
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(batchData, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    BatchData anotherBatch = SerializeUtils.deserializeBatchData(buffer);
    while (batchData.hasCurrent()) {
      Assert.assertEquals(batchData.currentValue(), anotherBatch.currentValue());
      batchData.next();
      anotherBatch.next();
    }
  }

  @Test
  public void serdesFLOATBatchDataTest() {
    BatchData batchData = new BatchData(TSDataType.FLOAT);
    float fvalue = 0f;
    for (long time = 0; time < 10; time++) {
      batchData.putAnObject(time, fvalue);
      fvalue++;
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(batchData, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    BatchData anotherBatch = SerializeUtils.deserializeBatchData(buffer);
    while (batchData.hasCurrent()) {
      Assert.assertEquals(batchData.currentValue(), anotherBatch.currentValue());
      batchData.next();
      anotherBatch.next();
    }
  }

  @Test
  public void serdesDOUBLEBatchDataTest() {
    BatchData batchData = new BatchData(TSDataType.DOUBLE);
    double dvalue = 0d;
    for (long time = 0; time < 10; time++) {
      batchData.putAnObject(time, dvalue);
      dvalue++;
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(batchData, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    BatchData anotherBatch = SerializeUtils.deserializeBatchData(buffer);
    while (batchData.hasCurrent()) {
      Assert.assertEquals(batchData.currentValue(), anotherBatch.currentValue());
      batchData.next();
      anotherBatch.next();
    }
  }

  @Test
  public void serdesBOOLEANBatchDataTest() {
    BatchData batchData = new BatchData(TSDataType.BOOLEAN);
    batchData.putAnObject(1, true);
    batchData.putAnObject(2, false);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(batchData, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    BatchData anotherBatch = SerializeUtils.deserializeBatchData(buffer);
    while (batchData.hasCurrent()) {
      Assert.assertEquals(batchData.currentValue(), anotherBatch.currentValue());
      batchData.next();
      anotherBatch.next();
    }
  }

  @Test
  public void serdesTEXTBatchDataTest() {
    BatchData batchData = new BatchData(TSDataType.TEXT);
    String svalue = "";
    for (long time = 0; time < 10; time++) {
      batchData.putAnObject(time, Binary.valueOf(svalue));
      svalue += String.valueOf(time);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(batchData, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    BatchData anotherBatch = SerializeUtils.deserializeBatchData(buffer);
    while (batchData.hasCurrent()) {
      Assert.assertEquals(batchData.currentValue(), anotherBatch.currentValue());
      batchData.next();
      anotherBatch.next();
    }
  }

  /** This method tests SerializeUtils.serializeTVPair() and SerializeUtils.deserializeTVPair() */
  @Test
  public void serdesTVPairTest() {
    List<TimeValuePair> TVPairs = new ArrayList<>();
    TimeValuePair p1 = new TimeValuePair(0, TsPrimitiveType.getByType(TSDataType.BOOLEAN, true));
    TVPairs.add(p1);
    TimeValuePair p2 = new TimeValuePair(0, TsPrimitiveType.getByType(TSDataType.INT32, 1));
    TVPairs.add(p2);
    TimeValuePair p3 = new TimeValuePair(0, TsPrimitiveType.getByType(TSDataType.INT64, 1L));
    TVPairs.add(p3);
    TimeValuePair p4 = new TimeValuePair(0, TsPrimitiveType.getByType(TSDataType.FLOAT, 1.0f));
    TVPairs.add(p4);
    TimeValuePair p5 = new TimeValuePair(0, TsPrimitiveType.getByType(TSDataType.DOUBLE, 1.0d));
    TVPairs.add(p5);
    TimeValuePair p6 =
        new TimeValuePair(0, TsPrimitiveType.getByType(TSDataType.TEXT, Binary.valueOf("a")));
    TVPairs.add(p6);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    for (TimeValuePair tv : TVPairs) {
      SerializeUtils.serializeTVPair(tv, outputStream);
      ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
      Assert.assertEquals(tv, SerializeUtils.deserializeTVPair(buffer));
      baos.reset();
    }
  }

  /** This method tests SerializeUtils.serializeTVPairs() and SerializeUtils.deserializeTVPairs() */
  @Test
  public void serdesTVPairsTest() {
    List<List<TimeValuePair>> TVPairs = new ArrayList<>();
    TimeValuePair p1 = new TimeValuePair(0, TsPrimitiveType.getByType(TSDataType.BOOLEAN, true));
    TVPairs.add(Collections.singletonList(p1));
    TimeValuePair p2 = new TimeValuePair(0, TsPrimitiveType.getByType(TSDataType.INT32, 1));
    TVPairs.add(Collections.singletonList(p2));
    TimeValuePair p3 = new TimeValuePair(0, TsPrimitiveType.getByType(TSDataType.INT64, 1L));
    TVPairs.add(Collections.singletonList(p3));
    TimeValuePair p4 = new TimeValuePair(0, TsPrimitiveType.getByType(TSDataType.FLOAT, 1.0f));
    TVPairs.add(Collections.singletonList(p4));
    TimeValuePair p5 = new TimeValuePair(0, TsPrimitiveType.getByType(TSDataType.DOUBLE, 1.0d));
    TVPairs.add(Collections.singletonList(p5));
    TimeValuePair p6 =
        new TimeValuePair(0, TsPrimitiveType.getByType(TSDataType.TEXT, Binary.valueOf("a")));
    TVPairs.add(Collections.singletonList(p6));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    for (List<TimeValuePair> tv : TVPairs) {
      SerializeUtils.serializeTVPairs(tv, outputStream);
      ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
      Assert.assertEquals(tv, SerializeUtils.deserializeTVPairs(buffer));
      baos.reset();
    }
  }

  /** This method tests SerializeUtils.serializeObject() and SerializeUtils.deserializeObject() */
  @Test
  public void serdesObjectTest() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeObject(1, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    Assert.assertEquals(1, SerializeUtils.deserializeObject(buffer));
  }

  /** This method tests SerializeUtils.serializeObjects() and SerializeUtils.deserializeObjects() */
  @Test
  public void serdesObjectsTest() {
    Object[] objects = {1, "2", 3d};
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeObjects(objects, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    Assert.assertArrayEquals(objects, SerializeUtils.deserializeObjects(buffer));
  }

  /** This method tests SerializeUtils.serializeLongs() and SerializeUtils.deserializeLongs() */
  @Test
  public void serdesLongsTest() {
    long[] array = {1, 10, 100, 1000, 10000};
    ByteBuffer buffer = SerializeUtils.serializeLongs(array);
    Assert.assertArrayEquals(array, SerializeUtils.deserializeLongs(buffer));
  }
}
