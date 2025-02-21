/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemChunkDeserializeTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private String storageGroup = "sg1";
  private String dataRegionId = "1";

  private IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("d1");
  double delta;

  @Before
  public void setUp() throws Exception {
    delta = Math.pow(0.1, TSFileDescriptor.getInstance().getConfig().getFloatPrecision());
    config.setTVListSortThreshold(100);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testNonAlignedBoolean() throws IOException, QueryProcessException, MetadataException {
    TSDataType dataType = TSDataType.BOOLEAN;
    WritableMemChunk series =
        new WritableMemChunk(new MeasurementSchema("s1", dataType, TSEncoding.PLAIN));
    int count = 1000;
    for (int i = 0; i < count; i++) {
      series.writeNonAlignedPoint(i, i % 2 == 0);
    }
    series.delete(100, 200);

    WritableMemChunk memChunk = createWritableMemChunkFromBytes(series);
    ReadOnlyMemChunk readableChunk = getReadOnlyChunk(memChunk, dataType);
    IPointReader it = readableChunk.getPointReader();
    for (int i = 0; i < count; i++) {
      if (i >= 100 && i <= 200) {
        continue;
      }
      it.hasNextTimeValuePair();
      TimeValuePair timeValuePair = it.nextTimeValuePair();
      Assert.assertEquals(i, timeValuePair.getTimestamp());
      Assert.assertEquals(i % 2 == 0, timeValuePair.getValue().getBoolean());
    }
  }

  @Test
  public void testNonAlignedInt32() throws IOException, QueryProcessException, MetadataException {
    TSDataType dataType = TSDataType.INT32;
    WritableMemChunk series =
        new WritableMemChunk(new MeasurementSchema("s1", dataType, TSEncoding.PLAIN));
    int count = 1000;
    for (int i = 0; i < count; i++) {
      series.writeNonAlignedPoint(i, i);
    }
    series.delete(100, 200);

    WritableMemChunk memChunk = createWritableMemChunkFromBytes(series);
    ReadOnlyMemChunk readableChunk = getReadOnlyChunk(memChunk, dataType);
    IPointReader it = readableChunk.getPointReader();
    for (int i = 0; i < count; i++) {
      if (i >= 100 && i <= 200) {
        continue;
      }
      it.hasNextTimeValuePair();
      TimeValuePair timeValuePair = it.nextTimeValuePair();
      Assert.assertEquals(i, timeValuePair.getTimestamp());
      Assert.assertEquals(i, timeValuePair.getValue().getInt());
    }
  }

  @Test
  public void testNonAlignedInt64() throws IOException, QueryProcessException, MetadataException {
    TSDataType dataType = TSDataType.INT64;
    WritableMemChunk series =
        new WritableMemChunk(new MeasurementSchema("s1", dataType, TSEncoding.PLAIN));
    int count = 1000;
    for (int i = 0; i < count; i++) {
      series.writeNonAlignedPoint(i, (long) i);
    }
    series.delete(100, 200);

    WritableMemChunk memChunk = createWritableMemChunkFromBytes(series);
    ReadOnlyMemChunk readableChunk = getReadOnlyChunk(memChunk, dataType);
    IPointReader it = readableChunk.getPointReader();
    for (int i = 0; i < count; i++) {
      if (i >= 100 && i <= 200) {
        continue;
      }
      it.hasNextTimeValuePair();
      TimeValuePair timeValuePair = it.nextTimeValuePair();
      Assert.assertEquals(i, timeValuePair.getTimestamp());
      Assert.assertEquals(i, timeValuePair.getValue().getLong());
    }
  }

  @Test
  public void testNonAlignedFloat() throws IOException, QueryProcessException, MetadataException {
    TSDataType dataType = TSDataType.FLOAT;
    WritableMemChunk series =
        new WritableMemChunk(new MeasurementSchema("s1", dataType, TSEncoding.PLAIN));
    int count = 1000;
    for (int i = 0; i < count; i++) {
      series.writeNonAlignedPoint(i, (float) i);
    }
    series.delete(100, 200);

    WritableMemChunk memChunk = createWritableMemChunkFromBytes(series);
    ReadOnlyMemChunk readableChunk = getReadOnlyChunk(memChunk, dataType);
    IPointReader it = readableChunk.getPointReader();
    for (int i = 0; i < count; i++) {
      if (i >= 100 && i <= 200) {
        continue;
      }
      it.hasNextTimeValuePair();
      TimeValuePair timeValuePair = it.nextTimeValuePair();
      Assert.assertEquals(i, timeValuePair.getTimestamp());
      Assert.assertEquals(timeValuePair.getValue().getFloat(), i, delta);
    }
  }

  @Test
  public void testNonAlignedDouble() throws IOException, QueryProcessException, MetadataException {
    TSDataType dataType = TSDataType.DOUBLE;
    WritableMemChunk series =
        new WritableMemChunk(new MeasurementSchema("s1", dataType, TSEncoding.PLAIN));
    int count = 1000;
    for (int i = 0; i < count; i++) {
      series.writeNonAlignedPoint(i, (double) i);
    }
    series.delete(100, 200);

    WritableMemChunk memChunk = createWritableMemChunkFromBytes(series);
    ReadOnlyMemChunk readableChunk = getReadOnlyChunk(memChunk, dataType);
    IPointReader it = readableChunk.getPointReader();
    for (int i = 0; i < count; i++) {
      if (i >= 100 && i <= 200) {
        continue;
      }
      it.hasNextTimeValuePair();
      TimeValuePair timeValuePair = it.nextTimeValuePair();
      Assert.assertEquals(i, timeValuePair.getTimestamp());
      Assert.assertEquals(timeValuePair.getValue().getDouble(), i, delta);
    }
  }

  @Test
  public void testNonAlignedBinary() throws IOException, QueryProcessException, MetadataException {
    TSDataType dataType = TSDataType.TEXT;
    WritableMemChunk series =
        new WritableMemChunk(new MeasurementSchema("s1", dataType, TSEncoding.PLAIN));
    int count = 1000;
    for (int i = 0; i < count; i++) {
      series.writeNonAlignedPoint(i, new Binary("text" + i, StandardCharsets.UTF_8));
    }
    series.delete(100, 200);

    WritableMemChunk memChunk = createWritableMemChunkFromBytes(series);
    ReadOnlyMemChunk readableChunk = getReadOnlyChunk(memChunk, dataType);
    IPointReader it = readableChunk.getPointReader();
    for (int i = 0; i < count; i++) {
      if (i >= 100 && i <= 200) {
        continue;
      }
      it.hasNextTimeValuePair();
      TimeValuePair timeValuePair = it.nextTimeValuePair();
      Assert.assertEquals(i, timeValuePair.getTimestamp());
      Assert.assertEquals(timeValuePair.getValue().getBinary().toString(), "text" + i);
    }
  }

  @Test
  public void testAlignedSeries() throws IOException, QueryProcessException, MetadataException {
    //    IMemTable memTable = new PrimitiveMemTable(storageGroup, dataRegionId);
    List<String> measurementList = Arrays.asList("s1", "s2", "s3", "s4", "s5", "s6");
    List<IMeasurementSchema> schemaList =
        Arrays.asList(
            new MeasurementSchema("s1", TSDataType.BOOLEAN),
            new MeasurementSchema("s2", TSDataType.INT32),
            new MeasurementSchema("s3", TSDataType.INT64),
            new MeasurementSchema("s4", TSDataType.FLOAT),
            new MeasurementSchema("s5", TSDataType.DOUBLE),
            new MeasurementSchema("s6", TSDataType.TEXT));
    AlignedWritableMemChunk series = new AlignedWritableMemChunk(schemaList, false);

    int count = 1000;
    for (int i = 0; i < count; i++) {
      Object[] data =
          new Object[] {
            i % 2 == 0,
            i,
            (long) i,
            (float) i,
            (double) i,
            new Binary("text" + i, TSFileConfig.STRING_CHARSET)
          };
      series.writeAlignedPoints(i, data, schemaList);
    }
    series.delete(100, 200);

    int serializedSize = series.serializedSize();
    WALByteBufferForTest walBuffer = new WALByteBufferForTest(ByteBuffer.allocate(serializedSize));
    series.serializeToWAL(walBuffer);
    DataInputStream inputStream =
        new DataInputStream(new ByteArrayInputStream(walBuffer.getBuffer().array()));
    AlignedWritableMemChunk memChunk = AlignedWritableMemChunk.deserialize(inputStream, false);

    AlignedReadOnlyMemChunk readableChunk =
        (AlignedReadOnlyMemChunk) getAlignedReadOnlyChunk(memChunk, schemaList, measurementList);
    IPointReader it = readableChunk.getPointReader();
    for (int i = 0; i < count; i++) {
      if (i >= 100 && i <= 200) {
        continue;
      }
      it.hasNextTimeValuePair();
      TimeValuePair timeValuePair = it.nextTimeValuePair();
      Assert.assertEquals(i, timeValuePair.getTimestamp());
      TsPrimitiveType[] values = timeValuePair.getValue().getVector();
      Assert.assertEquals(values[0].getBoolean(), i % 2 == 0);
      Assert.assertEquals(values[1].getInt(), i);
      Assert.assertEquals(values[2].getLong(), i);
      Assert.assertEquals(values[3].getFloat(), i, delta);
      Assert.assertEquals(values[4].getDouble(), i, delta);
      Assert.assertEquals(values[5].getBinary().toString(), "text" + i);
    }
  }

  private WritableMemChunk createWritableMemChunkFromBytes(WritableMemChunk series)
      throws IOException {
    int serializedSize = series.serializedSize();
    WALByteBufferForTest walBuffer = new WALByteBufferForTest(ByteBuffer.allocate(serializedSize));
    series.serializeToWAL(walBuffer);
    DataInputStream inputStream =
        new DataInputStream(new ByteArrayInputStream(walBuffer.getBuffer().array()));
    return WritableMemChunk.deserialize(inputStream);
  }

  private ReadOnlyMemChunk getReadOnlyChunk(WritableMemChunk memChunk, TSDataType dataType)
      throws QueryProcessException, IOException, MetadataException {
    WritableMemChunkGroup memChunkGroup = new WritableMemChunkGroup();
    memChunkGroup.getMemChunkMap().put("s1", memChunk);
    Map<IDeviceID, IWritableMemChunkGroup> memTableMap = new HashMap<>();
    memTableMap.put(deviceID, memChunkGroup);
    IMemTable memTable = new PrimitiveMemTable(storageGroup, dataRegionId, memTableMap);

    QueryContext context = new QueryContext();
    NonAlignedFullPath nonAlignedFullPath =
        new NonAlignedFullPath(
            deviceID,
            new MeasurementSchema(
                "s1",
                dataType,
                TSEncoding.RLE,
                CompressionType.UNCOMPRESSED,
                Collections.emptyMap()));
    return memTable.query(context, nonAlignedFullPath, Long.MIN_VALUE, null, null);
  }

  private ReadOnlyMemChunk getAlignedReadOnlyChunk(
      AlignedWritableMemChunk memChunk,
      List<IMeasurementSchema> schemaList,
      List<String> measurementList)
      throws QueryProcessException, IOException, MetadataException {
    AlignedWritableMemChunkGroup memChunkGroup =
        new AlignedWritableMemChunkGroup(memChunk, schemaList);
    Map<IDeviceID, IWritableMemChunkGroup> memTableMap = new HashMap<>();
    memTableMap.put(deviceID, memChunkGroup);
    IMemTable memTable = new PrimitiveMemTable(storageGroup, dataRegionId, memTableMap);

    QueryContext context = new QueryContext();
    AlignedFullPath alignedFullPath = new AlignedFullPath(deviceID, measurementList, schemaList);
    return memTable.query(context, alignedFullPath, Long.MIN_VALUE, null, null);
  }
}
