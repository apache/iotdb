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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.utils.MathUtils;
import org.apache.iotdb.db.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

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
        new WritableMemChunk(new MeasurementSchema("s1", dataType, TSEncoding.PLAIN));
    int count = 1000;
    for (int i = 0; i < count; i++) {
      series.writeWithFlushCheck(i, i);
    }
    IPointReader it =
        series.getSortedTvListForQuery().buildTsBlock().getTsBlockSingleColumnIterator();
    int i = 0;
    while (it.hasNextTimeValuePair()) {
      Assert.assertEquals(i, it.nextTimeValuePair().getTimestamp());
      i++;
    }
    Assert.assertEquals(count, i);
  }

  @Test
  public void memSeriesToStringTest() throws IOException {
    TSDataType dataType = TSDataType.INT32;
    WritableMemChunk series =
        new WritableMemChunk(new MeasurementSchema("s1", dataType, TSEncoding.PLAIN));
    int count = 100;
    for (int i = 0; i < count; i++) {
      series.writeWithFlushCheck(i, i);
    }
    series.writeWithFlushCheck(0, 21);
    series.writeWithFlushCheck(99, 20);
    series.writeWithFlushCheck(20, 21);
    String str = series.toString();
    Assert.assertFalse(series.getTVList().isSorted());
    Assert.assertEquals(
        "MemChunk Size: 103"
            + System.lineSeparator()
            + "Data type:INT32"
            + System.lineSeparator()
            + "First point:0 : 0"
            + System.lineSeparator()
            + "Last point:99 : 20"
            + System.lineSeparator(),
        str);
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
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          Collections.singletonList(
              new MeasurementSchema(measurementId[0], TSDataType.INT32, TSEncoding.PLAIN)),
          dataSize - i - 1,
          new Object[] {i + 10});
    }
    for (int i = 0; i < dataSize; i++) {
      memTable.write(
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          Collections.singletonList(
              new MeasurementSchema(measurementId[0], TSDataType.INT32, TSEncoding.PLAIN)),
          i,
          new Object[] {i});
    }
    MeasurementPath fullPath =
        new MeasurementPath(
            deviceId,
            measurementId[0],
            new MeasurementSchema(
                measurementId[0],
                TSDataType.INT32,
                TSEncoding.RLE,
                CompressionType.UNCOMPRESSED,
                Collections.emptyMap()));
    ReadOnlyMemChunk memChunk = memTable.query(fullPath, Long.MIN_VALUE, null);
    IPointReader iterator = memChunk.getPointReader();
    for (int i = 0; i < dataSize; i++) {
      iterator.hasNextTimeValuePair();
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      Assert.assertEquals(i, timeValuePair.getTimestamp());
      Assert.assertEquals(i, timeValuePair.getValue().getValue());
    }
  }

  @Test
  public void totalSeriesNumberTest() throws IOException, QueryProcessException, MetadataException {
    IMemTable memTable = new PrimitiveMemTable();
    int count = 10;
    String deviceId = "d1";
    String[] measurementId = new String[count];
    for (int i = 0; i < measurementId.length; i++) {
      measurementId[i] = "s" + i;
    }
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema(measurementId[0], TSDataType.INT32, TSEncoding.PLAIN));
    schemaList.add(new MeasurementSchema(measurementId[1], TSDataType.INT32, TSEncoding.PLAIN));
    int dataSize = 10000;
    for (int i = 0; i < dataSize; i++) {
      memTable.write(
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          Collections.singletonList(
              new MeasurementSchema(measurementId[0], TSDataType.INT32, TSEncoding.PLAIN)),
          i,
          new Object[] {i});
    }
    deviceId = "d2";
    for (int i = 0; i < dataSize; i++) {
      memTable.write(
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          schemaList,
          i,
          new Object[] {i, i});
    }
    Assert.assertEquals(3, memTable.getSeriesNumber());
    // aligned
    deviceId = "d3";

    for (int i = 0; i < dataSize; i++) {
      memTable.writeAlignedRow(
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          schemaList,
          i,
          new Object[] {i, i});
    }
    Assert.assertEquals(5, memTable.getSeriesNumber());
    memTable.writeAlignedRow(
        DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
        Collections.singletonList(
            new MeasurementSchema(measurementId[2], TSDataType.INT32, TSEncoding.PLAIN)),
        0,
        new Object[] {0});
    Assert.assertEquals(6, memTable.getSeriesNumber());
  }

  @Test
  public void queryWithDeletionTest() throws IOException, QueryProcessException, MetadataException {
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
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          Collections.singletonList(
              new MeasurementSchema(measurementId[0], TSDataType.INT32, TSEncoding.PLAIN)),
          dataSize - i - 1,
          new Object[] {i + 10});
    }
    for (int i = 0; i < dataSize; i++) {
      memTable.write(
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          Collections.singletonList(
              new MeasurementSchema(measurementId[0], TSDataType.INT32, TSEncoding.PLAIN)),
          i,
          new Object[] {i});
    }
    MeasurementPath fullPath =
        new MeasurementPath(
            deviceId,
            measurementId[0],
            new MeasurementSchema(
                measurementId[0],
                TSDataType.INT32,
                TSEncoding.RLE,
                CompressionType.UNCOMPRESSED,
                Collections.emptyMap()));
    List<Pair<Modification, IMemTable>> modsToMemtable = new ArrayList<>();
    Modification deletion =
        new Deletion(new PartialPath(deviceId, measurementId[0]), Long.MAX_VALUE, 10, dataSize);
    modsToMemtable.add(new Pair<>(deletion, memTable));
    ReadOnlyMemChunk memChunk = memTable.query(fullPath, Long.MIN_VALUE, modsToMemtable);
    IPointReader iterator = memChunk.getPointReader();
    int cnt = 0;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      Assert.assertEquals(cnt, timeValuePair.getTimestamp());
      Assert.assertEquals(cnt, timeValuePair.getValue().getValue());
      cnt++;
    }
    Assert.assertEquals(10, cnt);
  }

  @Test
  public void queryAlignChuckWithDeletionTest()
      throws IOException, QueryProcessException, MetadataException {
    IMemTable memTable = new PrimitiveMemTable();
    int count = 10;
    String deviceId = "d1";
    String[] measurementId = new String[count];
    for (int i = 0; i < measurementId.length; i++) {
      measurementId[i] = "s" + i;
    }

    int dataSize = 10000;
    for (int i = 0; i < dataSize; i++) {
      memTable.writeAlignedRow(
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          Collections.singletonList(
              new MeasurementSchema(measurementId[0], TSDataType.INT32, TSEncoding.PLAIN)),
          dataSize - i - 1,
          new Object[] {i + 10});
    }
    for (int i = 0; i < dataSize; i++) {
      memTable.writeAlignedRow(
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          Collections.singletonList(
              new MeasurementSchema(measurementId[0], TSDataType.INT32, TSEncoding.PLAIN)),
          i,
          new Object[] {i});
    }
    AlignedPath fullPath =
        new AlignedPath(
            deviceId,
            Collections.singletonList(measurementId[0]),
            Collections.singletonList(
                new MeasurementSchema(
                    measurementId[0],
                    TSDataType.INT32,
                    TSEncoding.RLE,
                    CompressionType.UNCOMPRESSED,
                    Collections.emptyMap())));
    List<Pair<Modification, IMemTable>> modsToMemtable = new ArrayList<>();
    Modification deletion =
        new Deletion(new PartialPath(deviceId, measurementId[0]), Long.MAX_VALUE, 10, dataSize);
    modsToMemtable.add(new Pair<>(deletion, memTable));
    ReadOnlyMemChunk memChunk = memTable.query(fullPath, Long.MIN_VALUE, modsToMemtable);
    IPointReader iterator = memChunk.getPointReader();
    int cnt = 0;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      Assert.assertEquals(cnt, timeValuePair.getTimestamp());
      Assert.assertEquals(cnt, timeValuePair.getValue().getVector()[0].getInt());
      cnt++;
    }
    Assert.assertEquals(10, cnt);
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
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          Collections.singletonList(new MeasurementSchema(sensorId, dataType, encoding)),
          aRet.getTimestamp(),
          new Object[] {aRet.getValue().getValue()});
    }
    MeasurementPath fullPath =
        new MeasurementPath(
            deviceId,
            sensorId,
            new MeasurementSchema(
                sensorId,
                dataType,
                encoding,
                CompressionType.UNCOMPRESSED,
                Collections.emptyMap()));
    IPointReader tvPair = memTable.query(fullPath, Long.MIN_VALUE, null).getPointReader();
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

  private void writeVector(IMemTable memTable)
      throws IOException, QueryProcessException, MetadataException, WriteProcessException {
    memTable.insertAlignedTablet(genInsertTableNode(), 0, 100);

    AlignedPath fullPath =
        new AlignedPath(
            "root.sg.device5",
            Collections.singletonList("sensor1"),
            Collections.singletonList(
                new MeasurementSchema(
                    "sensor1",
                    TSDataType.INT64,
                    TSEncoding.GORILLA,
                    CompressionType.UNCOMPRESSED,
                    Collections.emptyMap())));
    IPointReader tvPair = memTable.query(fullPath, Long.MIN_VALUE, null).getPointReader();
    for (int i = 0; i < 100; i++) {
      tvPair.hasNextTimeValuePair();
      TimeValuePair next = tvPair.nextTimeValuePair();
      Assert.assertEquals(i, next.getTimestamp());
      Assert.assertEquals(i, next.getValue().getVector()[0].getLong());
    }

    fullPath =
        new AlignedPath(
            "root.sg.device5",
            Arrays.asList("sensor0", "sensor1"),
            Arrays.asList(
                new MeasurementSchema(
                    "sensor0",
                    TSDataType.BOOLEAN,
                    TSEncoding.PLAIN,
                    CompressionType.UNCOMPRESSED,
                    Collections.emptyMap()),
                new MeasurementSchema(
                    "sensor1",
                    TSDataType.INT64,
                    TSEncoding.GORILLA,
                    CompressionType.UNCOMPRESSED,
                    Collections.emptyMap())));

    tvPair = memTable.query(fullPath, Long.MIN_VALUE, null).getPointReader();
    for (int i = 0; i < 100; i++) {
      tvPair.hasNextTimeValuePair();
      TimeValuePair next = tvPair.nextTimeValuePair();
      Assert.assertEquals(i, next.getTimestamp());
      Assert.assertEquals(i, next.getValue().getVector()[1].getLong());
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
  public void testAllType()
      throws IOException, QueryProcessException, MetadataException, WriteProcessException {
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
    writeVector(memTable);
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

  private static InsertTabletNode genInsertTableNode() throws IllegalPathException {
    String[] measurements = new String[2];
    measurements[0] = "sensor0";
    measurements[1] = "sensor1";
    String deviceId = "root.sg.device5";
    TSDataType[] dataTypes = new TSDataType[2];
    dataTypes[0] = TSDataType.BOOLEAN;
    dataTypes[1] = TSDataType.INT64;
    long[] times = new long[100];
    Object[] columns = new Object[2];
    columns[0] = new boolean[100];
    columns[1] = new long[100];

    for (long r = 0; r < 100; r++) {
      times[(int) r] = r;
      ((boolean[]) columns[0])[(int) r] = false;
      ((long[]) columns[1])[(int) r] = r;
    }
    TSEncoding[] encodings = new TSEncoding[2];
    encodings[0] = TSEncoding.PLAIN;
    encodings[1] = TSEncoding.GORILLA;

    MeasurementSchema[] schemas = new MeasurementSchema[2];
    schemas[0] = new MeasurementSchema(measurements[0], dataTypes[0], encodings[0]);
    schemas[1] = new MeasurementSchema(measurements[1], dataTypes[1], encodings[1]);

    InsertTabletNode node =
        new InsertTabletNode(
            new PlanNodeId("0"),
            new PartialPath(deviceId),
            true,
            measurements,
            dataTypes,
            times,
            null,
            columns,
            times.length);
    node.setMeasurementSchemas(schemas);
    return node;
  }

  @Test
  public void testSerializeSize()
      throws IOException, QueryProcessException, MetadataException, WriteProcessException {
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
    writeVector(memTable);

    int serializedSize = memTable.serializedSize();
    WALByteBufferForTest walBuffer = new WALByteBufferForTest(ByteBuffer.allocate(serializedSize));
    memTable.serializeToWAL(walBuffer);
    assertEquals(0, walBuffer.getBuffer().remaining());
  }
}
