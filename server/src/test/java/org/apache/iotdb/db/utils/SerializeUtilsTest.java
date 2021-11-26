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
import org.apache.iotdb.tsfile.read.common.DescReadBatchData;
import org.apache.iotdb.tsfile.read.common.DescReadWriteBatchData;
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

  @Test
  public void descReadWriteBatchDataTest() {
    descReadWriteBatchDataSerializableTest(0);
    descReadWriteBatchDataSerializableTest(1);
    descReadWriteBatchDataSerializableTest(10);
    descReadWriteBatchDataSerializableTest(16);
    descReadWriteBatchDataSerializableTest(100);
    descReadWriteBatchDataSerializableTest(1000);
    descReadWriteBatchDataSerializableTest(1500);
  }

  @Test
  public void descReadBatchDataTest() {
    descReadBatchDataSerializableTest(0);
    descReadBatchDataSerializableTest(1);
    descReadBatchDataSerializableTest(10);
    descReadBatchDataSerializableTest(16);
    descReadBatchDataSerializableTest(100);
    descReadBatchDataSerializableTest(1000);
    descReadBatchDataSerializableTest(1500);
  }

  @Test
  public void batchDataTest() {
    batchDataSerializableTest(0);
    batchDataSerializableTest(1);
    batchDataSerializableTest(10);
    batchDataSerializableTest(16);
    batchDataSerializableTest(100);
    batchDataSerializableTest(1000);
    batchDataSerializableTest(1500);
  }
  // In DescReadWriteBatchData, read has the same order with descending write
  private void descReadWriteBatchDataSerializableTest(int dataSize) {
    double E = 0.00001;
    String debugMsg = "Data size: " + dataSize + ", Data type: ";
    // test INT64
    TSDataType dataType = TSDataType.INT64;
    DescReadWriteBatchData data = new DescReadWriteBatchData(dataType);
    String fullMsg = debugMsg + dataType;
    for (int i = dataSize; i > 0; i--) {
      data.putLong(i, i);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    BatchData data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertTrue(fullMsg, data2 instanceof DescReadWriteBatchData);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, i + 1, data2.getLongByIndex(i));
    }
    for (int i = dataSize; i > 0; i--) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i, data2.getLong());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test INT32
    dataType = TSDataType.INT32;
    data = new DescReadWriteBatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = dataSize; i > 0; i--) {
      data.putInt(i, i);
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertTrue(fullMsg, data2 instanceof DescReadWriteBatchData);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, i + 1, data2.getIntByIndex(i));
    }
    for (int i = dataSize; i > 0; i--) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i, data2.getInt());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test DOUBLE
    dataType = TSDataType.DOUBLE;
    data = new DescReadWriteBatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = dataSize; i > 0; i--) {
      data.putDouble(i, i);
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertTrue(fullMsg, data2 instanceof DescReadWriteBatchData);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, i + 1, data2.getDoubleByIndex(i), E);
    }
    for (int i = dataSize; i > 0; i--) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i, data2.getDouble(), E);
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test FLOAT
    dataType = TSDataType.FLOAT;
    data = new DescReadWriteBatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = dataSize; i > 0; i--) {
      data.putFloat(i, i);
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertTrue(fullMsg, data2 instanceof DescReadWriteBatchData);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, i + 1, data2.getFloatByIndex(i), E);
    }
    for (int i = dataSize; i > 0; i--) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i, data2.getFloat(), E);
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test BOOLEAN
    dataType = TSDataType.BOOLEAN;
    data = new DescReadWriteBatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = dataSize; i > 0; i--) {
      data.putBoolean(i, i % 3 == 0);
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertTrue(fullMsg, data2 instanceof DescReadWriteBatchData);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, (i + 1) % 3 == 0, data2.getBooleanByIndex(i));
    }
    for (int i = dataSize; i > 0; i--) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i % 3 == 0, data2.getBoolean());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test BINARY
    dataType = TSDataType.TEXT;
    data = new DescReadWriteBatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = dataSize; i > 0; i--) {
      data.putBinary(i, Binary.valueOf(String.valueOf(i)));
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertTrue(fullMsg, data2 instanceof DescReadWriteBatchData);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(
          fullMsg, String.valueOf(i + 1), data2.getBinaryByIndex(i).getStringValue());
    }
    for (int i = dataSize; i > 0; i--) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, String.valueOf(i), data2.getBinary().getStringValue());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());

    // test VECTOR
    dataType = TSDataType.VECTOR;
    data = new DescReadWriteBatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = dataSize; i > 0; i--) {
      data.putVector(
          i,
          new TsPrimitiveType[] {
            new TsPrimitiveType.TsLong(i),
            new TsPrimitiveType.TsInt(i),
            new TsPrimitiveType.TsDouble(i),
            new TsPrimitiveType.TsFloat(i),
            new TsPrimitiveType.TsBoolean(i % 3 == 0),
            new TsPrimitiveType.TsBinary(new Binary(String.valueOf(i))),
          });
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertTrue(fullMsg, data2 instanceof DescReadWriteBatchData);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertArrayEquals(
          fullMsg,
          new TsPrimitiveType[] {
            new TsPrimitiveType.TsLong(i + 1),
            new TsPrimitiveType.TsInt(i + 1),
            new TsPrimitiveType.TsDouble(i + 1),
            new TsPrimitiveType.TsFloat(i + 1),
            new TsPrimitiveType.TsBoolean((i + 1) % 3 == 0),
            new TsPrimitiveType.TsBinary(new Binary(String.valueOf(i + 1))),
          },
          data2.getVectorByIndex(i));
    }
    for (int i = dataSize; i > 0; i--) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertArrayEquals(
          fullMsg,
          new TsPrimitiveType[] {
            new TsPrimitiveType.TsLong(i),
            new TsPrimitiveType.TsInt(i),
            new TsPrimitiveType.TsDouble(i),
            new TsPrimitiveType.TsFloat(i),
            new TsPrimitiveType.TsBoolean(i % 3 == 0),
            new TsPrimitiveType.TsBinary(new Binary(String.valueOf(i))),
          },
          data2.getVector());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
  }
  // In DescReadBatchData, read has a reverse order with ascending write
  private void descReadBatchDataSerializableTest(int dataSize) {
    double E = 0.00001;
    String debugMsg = "Data size: " + dataSize + ", Data type: ";
    // test INT64
    TSDataType dataType = TSDataType.INT64;
    DescReadBatchData data = new DescReadBatchData(dataType);
    String fullMsg = debugMsg + dataType;
    for (int i = 1; i <= dataSize; i++) {
      data.putLong(i, i);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    BatchData data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertTrue(fullMsg, data2 instanceof DescReadBatchData);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, i + 1, data2.getLongByIndex(i));
    }
    for (int i = dataSize; i > 0; i--) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i, data2.getLong());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test INT32
    dataType = TSDataType.INT32;
    data = new DescReadBatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = 1; i <= dataSize; i++) {
      data.putInt(i, i);
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertTrue(fullMsg, data2 instanceof DescReadBatchData);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, i + 1, data2.getIntByIndex(i));
    }
    for (int i = dataSize; i > 0; i--) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i, data2.getInt());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test DOUBLE
    dataType = TSDataType.DOUBLE;
    data = new DescReadBatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = 1; i <= dataSize; i++) {
      data.putDouble(i, i);
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertTrue(fullMsg, data2 instanceof DescReadBatchData);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, i + 1, data2.getDoubleByIndex(i), E);
    }
    for (int i = dataSize; i > 0; i--) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i, data2.getDouble(), E);
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test FLOAT
    dataType = TSDataType.FLOAT;
    data = new DescReadBatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = 1; i <= dataSize; i++) {
      data.putFloat(i, i);
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertTrue(fullMsg, data2 instanceof DescReadBatchData);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, i + 1, data2.getFloatByIndex(i), E);
    }
    for (int i = dataSize; i > 0; i--) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i, data2.getFloat(), E);
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test BOOLEAN
    dataType = TSDataType.BOOLEAN;
    data = new DescReadBatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = 1; i <= dataSize; i++) {
      data.putBoolean(i, i % 3 == 0);
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertTrue(fullMsg, data2 instanceof DescReadBatchData);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, (i + 1) % 3 == 0, data2.getBooleanByIndex(i));
    }
    for (int i = dataSize; i > 0; i--) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i % 3 == 0, data2.getBoolean());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test BINARY
    dataType = TSDataType.TEXT;
    data = new DescReadBatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = 1; i <= dataSize; i++) {
      data.putBinary(i, Binary.valueOf(String.valueOf(i)));
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertTrue(fullMsg, data2 instanceof DescReadBatchData);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(
          fullMsg, String.valueOf(i + 1), data2.getBinaryByIndex(i).getStringValue());
    }
    for (int i = dataSize; i > 0; i--) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, String.valueOf(i), data2.getBinary().getStringValue());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test VECTOR
    dataType = TSDataType.VECTOR;
    data = new DescReadBatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = 1; i <= dataSize; i++) {
      data.putVector(
          i,
          new TsPrimitiveType[] {
            new TsPrimitiveType.TsLong(i),
            new TsPrimitiveType.TsInt(i),
            new TsPrimitiveType.TsDouble(i),
            new TsPrimitiveType.TsFloat(i),
            new TsPrimitiveType.TsBoolean(i % 3 == 0),
            new TsPrimitiveType.TsBinary(new Binary(String.valueOf(i))),
          });
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertTrue(fullMsg, data2 instanceof DescReadBatchData);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertArrayEquals(
          fullMsg,
          new TsPrimitiveType[] {
            new TsPrimitiveType.TsLong(i + 1),
            new TsPrimitiveType.TsInt(i + 1),
            new TsPrimitiveType.TsDouble(i + 1),
            new TsPrimitiveType.TsFloat(i + 1),
            new TsPrimitiveType.TsBoolean((i + 1) % 3 == 0),
            new TsPrimitiveType.TsBinary(new Binary(String.valueOf(i + 1))),
          },
          data2.getVectorByIndex(i));
    }
    for (int i = dataSize; i > 0; i--) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertArrayEquals(
          fullMsg,
          new TsPrimitiveType[] {
            new TsPrimitiveType.TsLong(i),
            new TsPrimitiveType.TsInt(i),
            new TsPrimitiveType.TsDouble(i),
            new TsPrimitiveType.TsFloat(i),
            new TsPrimitiveType.TsBoolean(i % 3 == 0),
            new TsPrimitiveType.TsBinary(new Binary(String.valueOf(i))),
          },
          data2.getVector());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
  }
  // In BatchData, read has a reverse order with ascending write
  private void batchDataSerializableTest(int dataSize) {
    double E = 0.00001;
    String debugMsg = "Data size: " + dataSize + ", Data type: ";
    // test INT64
    TSDataType dataType = TSDataType.INT64;
    BatchData data = new BatchData(dataType);
    String fullMsg = debugMsg + dataType;
    for (int i = 1; i <= dataSize; i++) {
      data.putLong(i, i);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    BatchData data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, i + 1, data2.getLongByIndex(i));
    }
    for (int i = 1; i <= dataSize; i++) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i, data2.getLong());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test INT32
    dataType = TSDataType.INT32;
    data = new BatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = 1; i <= dataSize; i++) {
      data.putInt(i, i);
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, i + 1, data2.getIntByIndex(i));
    }
    for (int i = 1; i <= dataSize; i++) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i, data2.getInt());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test DOUBLE
    dataType = TSDataType.DOUBLE;
    data = new BatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = 1; i <= dataSize; i++) {
      data.putDouble(i, i);
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, i + 1, data2.getDoubleByIndex(i), E);
    }
    for (int i = 1; i <= dataSize; i++) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i, data2.getDouble(), E);
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test FLOAT
    dataType = TSDataType.FLOAT;
    data = new BatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = 1; i <= dataSize; i++) {
      data.putFloat(i, i);
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, i + 1, data2.getFloatByIndex(i), E);
    }
    for (int i = 1; i <= dataSize; i++) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i, data2.getFloat(), E);
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test BOOLEAN
    dataType = TSDataType.BOOLEAN;
    data = new BatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = 1; i <= dataSize; i++) {
      data.putBoolean(i, i % 3 == 0);
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(fullMsg, (i + 1) % 3 == 0, data2.getBooleanByIndex(i));
    }
    for (int i = 1; i <= dataSize; i++) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, i % 3 == 0, data2.getBoolean());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test BINARY
    dataType = TSDataType.TEXT;
    data = new BatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = 1; i <= dataSize; i++) {
      data.putBinary(i, Binary.valueOf(String.valueOf(i)));
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertEquals(
          fullMsg, String.valueOf(i + 1), data2.getBinaryByIndex(i).getStringValue());
    }
    for (int i = 1; i <= dataSize; i++) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertEquals(fullMsg, String.valueOf(i), data2.getBinary().getStringValue());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
    // test VECTOR
    dataType = TSDataType.VECTOR;
    data = new BatchData(dataType);
    fullMsg = debugMsg + dataType;
    for (int i = 1; i <= dataSize; i++) {
      data.putVector(
          i,
          new TsPrimitiveType[] {
            new TsPrimitiveType.TsLong(i),
            new TsPrimitiveType.TsInt(i),
            new TsPrimitiveType.TsDouble(i),
            new TsPrimitiveType.TsFloat(i),
            new TsPrimitiveType.TsBoolean(i % 3 == 0),
            new TsPrimitiveType.TsBinary(new Binary(String.valueOf(i))),
          });
    }
    baos = new ByteArrayOutputStream();
    outputStream = new DataOutputStream(baos);
    SerializeUtils.serializeBatchData(data, outputStream);
    buffer = ByteBuffer.wrap(baos.toByteArray());
    data2 = SerializeUtils.deserializeBatchData(buffer);
    Assert.assertEquals(fullMsg, dataSize, data2.length());
    if (dataSize > 0) {
      Assert.assertEquals(fullMsg, 1L, data2.getMinTimestamp());
      Assert.assertEquals(fullMsg, dataSize, data2.getMaxTimestamp());
    }
    for (int i = 0; i < dataSize; i++) {
      Assert.assertEquals(fullMsg, i + 1, data2.getTimeByIndex(i));
      Assert.assertArrayEquals(
          fullMsg,
          new TsPrimitiveType[] {
            new TsPrimitiveType.TsLong(i + 1),
            new TsPrimitiveType.TsInt(i + 1),
            new TsPrimitiveType.TsDouble(i + 1),
            new TsPrimitiveType.TsFloat(i + 1),
            new TsPrimitiveType.TsBoolean((i + 1) % 3 == 0),
            new TsPrimitiveType.TsBinary(new Binary(String.valueOf(i + 1))),
          },
          data2.getVectorByIndex(i));
    }
    for (int i = 1; i <= dataSize; i++) {
      Assert.assertTrue(fullMsg, data2.hasCurrent());
      Assert.assertEquals(fullMsg, i, data2.currentTime());
      Assert.assertArrayEquals(
          fullMsg,
          new TsPrimitiveType[] {
            new TsPrimitiveType.TsLong(i),
            new TsPrimitiveType.TsInt(i),
            new TsPrimitiveType.TsDouble(i),
            new TsPrimitiveType.TsFloat(i),
            new TsPrimitiveType.TsBoolean(i % 3 == 0),
            new TsPrimitiveType.TsBinary(new Binary(String.valueOf(i))),
          },
          data2.getVector());
      data2.next();
    }
    Assert.assertFalse(fullMsg, data2.hasCurrent());
  }
}
